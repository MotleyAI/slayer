"""Query models for SLayer.

SlayerQuery is the user-facing query object — minimal, just enough to express intent.
It is later planned into a ``PlannedQuery`` (see slayer/engine/planned.py), which
carries typed value keys interned into slots with their resolved expressions, join
paths and phases, and is ready for SQL generation.
"""
from __future__ import annotations

import datetime
import hashlib
import logging
import math
import re
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from slayer.core.enums import TimeGranularity
from slayer.core.errors import DistinctDimensionValuesError
from slayer.core.formula import _rewrite_funcstyle_aggregations
from slayer.core.models import ModelMeasure, SlayerModel, _validate_model_name
from slayer.engine.syntax import parse_expr
from slayer.sql.window_detect import WINDOW_IN_FILTER_ERROR, has_window_function
from slayer.storage.migrations import migrate as _migrate_schema

logger = logging.getLogger(__name__)

_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_VAR_PATTERN = re.compile(r"\{\{|\}\}|\{([a-zA-Z_][a-zA-Z0-9_]*)\}|\{([^}]*)\}")


def _validate_query_filter_string(formula: str) -> None:
    """Apply DEV-1369 DSL-mode construction-time rules to a single
    ``SlayerQuery.filters`` entry: reject raw ``OVER (...)`` window-function
    syntax.

    Raw SQL function calls (``json_extract``, ``coalesce``, …) and
    unknown bare names are rejected at binding time by
    :func:`slayer.core.formula.parse_filter` and the strict-resolution
    pass in :func:`slayer.engine.binding.bind_expr`.
    """
    if has_window_function(formula):
        raise ValueError(f"Filter '{formula}' {WINDOW_IN_FILTER_ERROR}")


# C0 control characters (U+0000–U+001F) → Python string-literal escapes, used
# by the ``"python"`` regime (DEV-1727). A raw newline / carriage return / NUL
# inside a single-quoted literal makes ``ast.parse`` raise, so every C0 char is
# encoded: ``\t``/``\n``/``\r`` as their named escape, the rest as ``\xNN``.
# Encoding the whole C0 range (not just the three ast-breakers) keeps the
# substituted filter single-line and printable, at zero behavioural cost —
# ``ast.parse`` recovers the identical value either way.
_C0_NAMED_ESCAPES = {"\t": "\\t", "\n": "\\n", "\r": "\\r"}
_C0_ESCAPE_MAP = {
    chr(codepoint): _C0_NAMED_ESCAPES.get(chr(codepoint), f"\\x{codepoint:02x}")
    for codepoint in range(0x20)
}
_C0_RE = re.compile(r"[\x00-\x1f]")


def _escape_string_value(
    value: str, escape: Literal["sql", "python"], *, backslash_escapes: bool
) -> str:
    """Escape a string variable value for the target expression layer.

    The value is inserted, unquoted, into a quoted literal the author already
    wrote (``status = '{v}'``); escaping keeps it from breaking out of that
    literal. Two layers, two escaping regimes (DEV-1625, hardened in DEV-1727):

    - ``"sql"`` — Mode-A raw-SQL surfaces are parsed by sqlglot. The regime is
      **dialect-aware** (``backslash_escapes``):

      * ``False`` (standard dialects — SQLite/Postgres/DuckDB/T-SQL/Trino/
        Presto/Oracle): a backslash is an ordinary literal char, so only the
        single quote is doubled (``'`` → ``''``).
      * ``True`` (backslash dialects — MySQL/ClickHouse/Snowflake/Redshift/
        BigQuery/Databricks/Spark): a backslash escapes the next char, so it is
        doubled FIRST (``\\`` → ``\\\\``) and the single quote is
        backslash-escaped (``'`` → ``\\'``). The double quote is left untouched
        — inside a single-quoted literal it is an ordinary char on every
        dialect, and ``\\"`` is not a recognised escape on 6 of the 7 backslash
        dialects (only MySQL), so escaping it would corrupt the value.

    - ``"python"`` — Mode-B query filters are parsed by SLayer's Python-AST
      formula parser, where SQL quote-doubling would be read as adjacent-literal
      concatenation (``'O''Brien'`` → ``'OBrien'``). So backslash is doubled
      FIRST, then both quote styles are backslash-escaped, then every C0
      control char (U+0000–U+001F) is encoded, matching Python string-literal
      rules so ``ast.parse`` recovers the original value. This matters because a
      raw newline/CR/NUL in a single-quoted Python literal is a ``SyntaxError``
      (or "null bytes" error), so leaving control chars unescaped would make
      ``ast.parse`` reject an otherwise valid value. SQL literals permit raw
      newlines, so the ``"sql"`` branch leaves them alone.
      ``backslash_escapes`` is ignored here.
    """
    if escape == "sql":
        if backslash_escapes:
            # Double the backslash before escaping the quote (order matters).
            return value.replace("\\", "\\\\").replace("'", "\\'")
        return value.replace("'", "''")
    # python: order matters — double the backslash before escaping quotes, then
    # encode C0 control chars so ast.parse recovers a single-line literal.
    escaped = value.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')
    return _C0_RE.sub(lambda m: _C0_ESCAPE_MAP[m.group(0)], escaped)


def _render_list_value(
    name: str,
    value: "list | tuple",
    escape: Literal["sql", "python"],
    *,
    backslash_escapes: bool,
) -> str:
    """Render a ``list``/``tuple`` variable value into an ``IN``-list body
    (DEV-1730 multi-value pushdown).

    The intended template shape is ``col IN ({var})`` — the author writes the
    parentheses; this renders the comma-separated body only. Unlike a scalar
    string (where the author writes the surrounding quotes), each string element
    is **auto-quoted** here: a single placeholder can't carry per-element quotes,
    so quoting has to happen at render time. Elements are escaped per the target
    layer via :func:`_escape_string_value`, so DEV-1727's escaping composes.

    - ``str`` element → auto-quoted + escaped (``O'Brien`` → ``'O''Brien'`` in
      sql mode; ``'O\\'Brien'`` in python mode).
    - ``int``/``float``/``bool`` element → bare via ``str()``; a non-finite float
      element raises (it can never render a valid literal).
    - ``escape="python"`` appends a **trailing comma** so the Mode-B Python-AST
      parser always reads a tuple — ``x in ('A',)`` (1-tuple membership), never
      ``x in ('A')`` (which parses as ``str`` containment).
    - An **empty** list/tuple raises: ``IN ()`` is invalid SQL, and "no filter"
      semantics belong to a sentinel default (see DEV-1730).
    - ``None``, nested list/tuple, dict, or any other element type raises,
      naming the variable.
    """
    if len(value) == 0:
        raise ValueError(
            f"Variable '{name}' cannot be an empty list; 'IN ()' is invalid SQL. "
            f"For 'no filter' semantics, use a sentinel default (see DEV-1730)."
        )
    rendered: list[str] = []
    for element in value:
        if isinstance(element, str):
            rendered.append(
                "'"
                + _escape_string_value(
                    value=element, escape=escape, backslash_escapes=backslash_escapes
                )
                + "'"
            )
        # bool is an int subclass and is accepted (renders True/False).
        elif isinstance(element, (int, float)):
            if isinstance(element, float) and not math.isfinite(element):
                raise ValueError(
                    f"Variable '{name}' list element must be finite, got {element!r}"
                )
            rendered.append(str(element))
        else:
            raise ValueError(
                f"Variable '{name}' list element must be a string, number, or bool, "
                f"got {type(element).__name__}"
            )
    joined = ", ".join(rendered)
    # Mode-B (python) needs the trailing comma to force tuple parsing; Mode-A
    # (sql) must NOT have it (``IN (1, 2,)`` is a syntax error in most dialects).
    return f"{joined}," if escape == "python" else joined


def _render_variable_value(
    name: str,
    value: Any,
    escape: Literal["sql", "python"],
    *,
    backslash_escapes: bool,
) -> str:
    """Render a single resolved variable value into substitution text.

    Strings are escaped for the target layer (see :func:`_escape_string_value`);
    numbers (including ``bool``) pass through via ``str()`` but non-finite floats
    raise (they can never render a valid literal). A ``list``/``tuple`` renders
    an ``IN``-list body (see :func:`_render_list_value`); anything else raises.
    """
    # list/tuple first: an IN-list body (DEV-1730). Checked before str so the
    # scalar path only ever sees a single value.
    if isinstance(value, (list, tuple)):
        return _render_list_value(
            name=name, value=value, escape=escape, backslash_escapes=backslash_escapes
        )
    if isinstance(value, str):
        return _escape_string_value(
            value=value, escape=escape, backslash_escapes=backslash_escapes
        )
    # bool is an int subclass and is accepted (renders True/False).
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"Variable '{name}' must be finite, got {value!r}")
        return str(value)
    raise ValueError(
        f"Variable '{name}' must be a string, number, or list/tuple, "
        f"got {type(value).__name__}"
    )


_BLOCK_OPEN = "{?"
_BLOCK_CLOSE = "?}"


def _find_block_end(text: str, start: int) -> int:
    """Scan from ``start`` (just past a ``{?``) to the matching ``?}``.

    Returns the index of the closing ``?}``. Raises on a nested ``{?`` (blocks
    do not nest) or if no close is found before end-of-string. ``{{``/``}}``
    escapes are skipped so they can never masquerade as block delimiters.
    """
    i, n = start, len(text)
    while i < n:
        two = text[i:i + 2]
        if two in ("{{", "}}"):
            i += 2
            continue
        if two == _BLOCK_OPEN:
            raise ValueError(
                f"Nested optional block '{{?' is not allowed in: {text!r}"
            )
        if two == _BLOCK_CLOSE:
            return i
        i += 1
    raise ValueError(
        f"Unterminated optional block (missing '?}}') in: {text!r}"
    )


def _split_blocks(text: str) -> list[tuple[str, str]]:
    """Split ``text`` into ``("text", s)`` / ``("block", inner)`` parts.

    Top-level ``{? ... ?}`` spans become ``block`` parts (inner text only);
    everything else is ``text``. ``{{``/``}}`` escapes are preserved verbatim in
    ``text`` parts (the downstream var pass renders them). Raises on a stray
    ``?}`` (a close with no open). Blocks never nest (enforced here).
    """
    parts: list[tuple[str, str]] = []
    buf: list[str] = []
    i, n = 0, len(text)
    while i < n:
        two = text[i:i + 2]
        if two in ("{{", "}}"):
            buf.append(two)
            i += 2
            continue
        if two == _BLOCK_OPEN:
            parts.append(("text", "".join(buf)))
            buf = []
            end = _find_block_end(text, i + 2)
            parts.append(("block", text[i + 2:end]))
            i = end + 2
            continue
        if two == _BLOCK_CLOSE:
            raise ValueError(
                f"Unexpected '?}}' (optional-block close without open) in: {text!r}"
            )
        buf.append(text[i])
        i += 1
    parts.append(("text", "".join(buf)))
    return parts


def _block_var_names(inner: str, whole: str) -> list[str]:
    """Return the valid ``{var}`` names inside a block's ``inner`` text.

    Raises if the block carries an invalid ``{...}`` name, or if it contains no
    ``{var}`` at all (an optional block with nothing to key on is a mistake —
    it would render identically whether or not any variable is supplied).
    """
    names: list[str] = []
    for match in _VAR_PATTERN.finditer(inner):
        if match.group(0) in ("{{", "}}"):
            continue
        if match.group(1) is not None:
            names.append(match.group(1))
        else:
            raise ValueError(
                f"Invalid variable name '{match.group(2)}' in optional block "
                f"of: {whole!r}."
            )
    if not names:
        raise ValueError(
            f"Optional block '{{? ... ?}}' must contain at least one "
            f"{{variable}} in: {whole!r}."
        )
    return names


def _contains_block_delimiter(text: str) -> bool:
    return _BLOCK_OPEN in text or _BLOCK_CLOSE in text


def _make_var_replacer(
    filter_str: str, variables: dict, escape: str, backslash_escapes: bool
):
    """Build the ``re.sub`` replacement callable for ``{var}`` / ``{{`` / ``}}``
    tokens, closed over the resolved escaping regime. Extracted from
    :func:`substitute_variables` to keep its cognitive complexity in check."""

    def _replace(match: re.Match) -> str:
        full = match.group(0)
        if full == "{{":
            return "{"
        if full == "}}":
            return "}"
        # Group 1: valid variable name
        valid_name = match.group(1)
        if valid_name is not None:
            if valid_name not in variables:
                raise ValueError(
                    f"Undefined variable '{valid_name}' in filter: {filter_str!r}. "
                    f"Available variables: {sorted(variables.keys())}"
                )
            return _render_variable_value(
                name=valid_name,
                value=variables[valid_name],
                escape=escape,
                backslash_escapes=backslash_escapes,
            )
        # Group 2: invalid variable name (matched {something} but name was invalid)
        bad_name = match.group(2)
        raise ValueError(
            f"Invalid variable name '{bad_name}' in filter: {filter_str!r}. "
            f"Variable names must contain only letters, digits, and underscores."
        )

    return _replace


def substitute_variables(
    filter_str: str,
    variables: dict[str, Any],
    *,
    escape: Literal["sql", "python"],
    backslash_escapes: bool | None = None,
) -> str:
    """Substitute {variable} placeholders in a filter or raw-SQL string.

    - {var_name} is replaced with the variable's value (str, number, or list).
    - {{ and }} are escaped to literal { and }.
    - Variable names must be alphanumeric + underscore.
    - Raises ValueError for undefined variables or invalid variable names.

    ``escape`` (required, keyword-only) selects the escaping regime for string
    values by expression layer — ``"sql"`` for Mode-A raw-SQL surfaces,
    ``"python"`` for Mode-B query filters. See :func:`_escape_string_value`.
    Numbers (including ``bool``) pass through via ``str()``; non-finite floats
    (``nan``/``inf``) raise, since they can never render a valid literal.

    ``backslash_escapes`` (keyword-only) is the **dialect-aware** signal for the
    ``"sql"`` regime (DEV-1727) and is **fail-closed**: with ``escape="sql"`` it
    is REQUIRED (``None`` raises), so a caller that renders raw SQL can never
    silently under-escape on a backslash dialect. Derive it from
    ``SqlDialect.backslash_escapes_strings``. It is ignored for
    ``escape="python"`` (Mode-B escaping is dialect-independent).

    A ``list``/``tuple`` value renders an ``IN``-list body (DEV-1730). The
    template writes the parentheses (``col IN ({var})``) and each string element
    is **auto-quoted** — the opposite of the scalar-string convention where the
    author writes the quotes (``status = '{v}'``). See :func:`_render_list_value`.

    Example:
        substitute_variables("status = '{status_val}'", {"status_val": "active"}, escape="sql", backslash_escapes=False)
        → "status = 'active'"

        substitute_variables("amount > {min_amount}", {"min_amount": 100}, escape="sql", backslash_escapes=False)
        → "amount > 100"

        substitute_variables("region IN ({regions})", {"regions": ["US", "CA"]}, escape="sql", backslash_escapes=False)
        → "region IN ('US', 'CA')"
    """
    if escape not in ("sql", "python"):
        raise ValueError(
            f"Invalid escape mode {escape!r}; expected 'sql' or 'python'."
        )
    if escape == "sql" and backslash_escapes is None:
        raise ValueError(
            "escape='sql' requires backslash_escapes to be specified: True on "
            "backslash-escaping dialects (MySQL, ClickHouse, Snowflake, "
            "Redshift, BigQuery, Databricks, Spark), False on standard dialects "
            "(SQLite, Postgres, DuckDB, ...). Derive it from "
            "SqlDialect.backslash_escapes_strings."
        )
    # Normalise to a concrete bool for the value renderers; python mode ignores
    # the signal (Mode-B escaping is dialect-independent).
    effective_backslash_escapes = bool(backslash_escapes) if escape == "sql" else False
    _replace = _make_var_replacer(
        filter_str, variables, escape, effective_backslash_escapes
    )

    # Optional blocks {? ... ?} are a Mode-A-only construct (DEV-1730). The
    # Mode-B Python-AST filter layer rejects them outright.
    if escape == "python":
        if _contains_block_delimiter(filter_str):
            raise ValueError(
                f"Optional blocks '{{? ... ?}}' are not supported in Mode-B "
                f"(python) filters: {filter_str!r}."
            )
        return _VAR_PATTERN.sub(_replace, filter_str)

    # Fast path: no block delimiters -> the original single-pass regex sub.
    if not _contains_block_delimiter(filter_str):
        return _VAR_PATTERN.sub(_replace, filter_str)

    return _render_block_segments(filter_str, variables, _replace)


def _render_block_segments(filter_str: str, variables: dict, replace_fn) -> str:
    """Render a Mode-A string that contains at least one ``{? ... ?}`` block.

    Plain text segments substitute normally; a block renders parenthesised when
    every inner ``{var}`` is supplied, else collapses to the neutral ``(1=1)``.
    """
    out: list[str] = []
    for kind, segment in _split_blocks(filter_str):
        if kind == "text":
            out.append(_VAR_PATTERN.sub(replace_fn, segment))
            continue
        names = _block_var_names(segment, filter_str)
        if all(name in variables for name in names):
            out.append("(" + _VAR_PATTERN.sub(replace_fn, segment).strip() + ")")
        else:
            out.append("(1=1)")
    return "".join(out)


def extract_placeholder_names(query: "SlayerQuery") -> set:
    """Return the set of valid {var} placeholder names referenced in
    ``query.filters``. Used to compute required-variable lists and to
    inject placeholder defaults during save-time dry-run validation.
    """
    found: set = set()
    for f in (query.filters or []):
        for match in _VAR_PATTERN.finditer(f):
            if match.group(0) in ("{{", "}}"):
                continue
            valid_name = match.group(1)
            if valid_name:
                found.add(valid_name)
    return found


def _probe_replace(match: re.Match) -> str:
    full = match.group(0)
    if full == "{{":
        return "{"
    if full == "}}":
        return "}"
    return "0"  # any {var} (valid or not) -> a syntactically safe literal


def render_probe_text(text: str) -> str:
    """Render a Mode-A surface for a syntax-only sqlglot parse (DEV-1730).

    Optional blocks collapse to ``(1=1)`` (their absent-value form) and every
    remaining ``{var}`` becomes the literal ``0`` — enough for sqlglot to parse
    structure without any caller variables. Shared by every converter validation
    path so import-time validation matches execution-time rendering.
    """
    out: list[str] = []
    for kind, segment in _split_blocks(text):
        if kind == "block":
            out.append("(1=1)")
        else:
            out.append(_VAR_PATTERN.sub(_probe_replace, segment))
    return "".join(out)


class ModelVariables(BaseModel):
    """Structural classification of a model's Mode-A ``{var}`` placeholders.

    ``required`` vars have no default and are not inside an optional block, so a
    query that omits them raises. ``optional`` vars either sit inside a ``{? ?}``
    block (collapse to ``(1=1)`` when absent) or carry a ``query_variables``
    default. Derived on demand from the four Mode-A surfaces — nothing is
    persisted, so there is no schema-version impact (DEV-1730).
    """

    required: list[str] = Field(default_factory=list)
    optional: list[str] = Field(default_factory=list)


def extract_variable_refs(text: str) -> tuple[set[str], set[str]]:
    """Return ``(bare_names, blocked_names)`` referenced in a Mode-A ``text``.

    ``bare_names`` appear outside any ``{? ?}`` block; ``blocked_names`` appear
    inside one. A name may land in both sets (used bare in one place and blocked
    in another) — the caller resolves the precedence.
    """
    bare: set[str] = set()
    blocked: set[str] = set()
    try:
        parts = _split_blocks(text)
    except ValueError:
        # Malformed block delimiters (stray '?}' / unterminated '{?'): classify
        # structurally as block-free rather than breaking read-only inspection
        # (extract_model_variables runs unguarded from the inspect skeleton).
        # Execution still raises through substitute_variables.
        parts = [("text", text)]
    for kind, segment in parts:
        target = bare if kind == "text" else blocked
        for match in _VAR_PATTERN.finditer(segment):
            if match.group(0) in ("{{", "}}"):
                continue
            if match.group(1):
                target.add(match.group(1))
    return bare, blocked


def extract_model_variables(model: SlayerModel) -> ModelVariables:
    """Classify a model's Mode-A ``{var}`` placeholders as required / optional.

    Walks the four Mode-A surfaces — ``SlayerModel.sql``, ``SlayerModel.filters``,
    ``Column.sql``, ``Column.filter`` — the same surfaces DEV-1625 substitutes.
    A bare occurrence with no ``query_variables`` default is required; everything
    else (inside a block, or defaulted) is optional. A bare-without-default
    occurrence anywhere wins, so a var used both bare and blocked is required.
    """
    surfaces: list[str] = []
    if model.sql:
        surfaces.append(model.sql)
    surfaces.extend(f for f in (model.filters or []) if f)
    for col in model.columns:
        if col.sql:
            surfaces.append(col.sql)
        if col.filter:
            surfaces.append(col.filter)

    bare: set[str] = set()
    blocked: set[str] = set()
    for surface in surfaces:
        s_bare, s_blocked = extract_variable_refs(surface)
        bare |= s_bare
        blocked |= s_blocked

    defaults = set(model.query_variables or {})
    required = {name for name in bare if name not in defaults}
    optional = (bare | blocked) - required
    return ModelVariables(
        required=sorted(required), optional=sorted(optional)
    )


def declared_variable_specs(model: SlayerModel) -> dict[str, dict]:
    """The model's DECLARED Mode-A variable bag (``name -> spec``), or ``{}``.

    An importer that generates parameterized model SQL records what it emitted
    under ``meta.cube_variables``, so the engine can tell a generated template
    apart from hand-written SQL that merely contains braces. ``meta`` is
    user-extensible, so every layer is shape-checked and a malformed bag
    degrades to "nothing declared" rather than raising during a query.

    An entry counts as a declaration only if it carries a NON-EMPTY string
    ``member`` — the shape every importer writes (a member name is always a
    parsed identifier). That makes the bag SELF-IDENTIFYING, so a hand-written
    ``meta`` that happens to reuse the ``cube_variables`` key for something else
    (``{"cube_variables": {"note": {}}}``) is not mistaken for generated SQL.
    The distinction matters: :func:`declares_variables` disables the
    zero-variable brace-literal fast path, so a false positive would make a
    previously-working model with raw braces start raising.
    """
    declared = (model.meta or {}).get("cube_variables")
    if not isinstance(declared, dict):
        return {}
    return {
        name: spec
        for name, spec in declared.items()
        if isinstance(name, str) and isinstance(spec, dict) and _is_member_name(spec)
    }


def _is_member_name(spec: dict) -> bool:
    """True if ``spec`` carries the non-empty string ``member`` that marks it as
    a real importer-written variable declaration."""
    member = spec.get("member")
    return isinstance(member, str) and bool(member)


def declares_variables(model: SlayerModel) -> bool:
    """True if the model declares its Mode-A variables (importer-generated SQL).

    Such a model is unambiguously parameterized, so the DEV-1625 brace-literal
    protection — which leaves surfaces untouched on a zero-variable call so raw
    braces like a Postgres array ``'{1,2,3}'`` survive — must NOT apply: leaving
    a declared ``{var}`` unrendered emits it into SQL instead of raising the
    documented missing-variable error. Hand-written models declare nothing and
    keep the protection.
    """
    return bool(declared_variable_specs(model))


def list_valued_variable_names(model: SlayerModel) -> set[str]:
    """Names of the model's Mode-A variables DECLARED to fill an ``IN``-list.

    The generic ``{var}`` contract puts quoting on the template author — a
    scalar string renders unquoted so ``{var}`` also works in numeric and
    fragment positions (``order_total >= {floor}``, ``{d}::TIMESTAMP``). That
    reasoning needs an author who can see the position, and it breaks down for
    a MACHINE-generated surface: the Cube importer emits the fixed template
    ``col IN ({var})``, so the caller can never write the quotes, and a scalar
    string would render ``IN (US)`` — a column reference that sqlglot parses
    happily and the database rejects (or, worse, silently resolves).

    An importer therefore declares such a variable with ``list_valued: true`` in
    ``meta.cube_variables``; :func:`coerce_declared_list_variables` acts on it.
    Only that neutral flag is read here — not Cube's ``kind`` taxonomy — so a
    future front-end emitting a different list-shaped template opts in the same
    way. Returns an empty set for a hand-written model (nothing declared), which
    keeps the generic scalar convention untouched.

    The flag must be exactly ``True``: ``meta`` is user-extensible, and matching
    on truthiness would let a stray ``1`` or the string ``"false"`` silently
    switch a variable's substitution semantics.
    """
    return {
        name
        for name, spec in declared_variable_specs(model).items()
        if spec.get("list_valued") is True
    }


def coerce_declared_list_variables(
    variables: dict[str, Any], *, list_valued: set[str]
) -> dict[str, Any]:
    """Wrap a scalar supplied for a declared list-valued variable in a
    one-element list, so it renders ``IN ('US')`` rather than ``IN (US)``.

    In ``IN (...)`` position a scalar and a one-element list are semantically
    identical, so this is a normalisation, not a guess — there is no competing
    reading of ``{"regions": "US"}`` against ``region IN ({regions})``.

    Only ``str``/``int``/``float``/``bool`` are wrapped. A ``list``/``tuple``
    passes through unchanged (including the empty list, which keeps raising —
    ``IN ()`` is invalid SQL and "no filter" belongs to an optional block or a
    sentinel default). Any other type is left alone so
    :func:`_render_variable_value` still raises its own naming error. Returns
    the input dict unchanged when nothing needs wrapping; never mutates it.
    """
    if not list_valued:
        return variables
    coerced: dict[str, Any] | None = None
    for name in list_valued:
        if name not in variables:
            continue
        value = variables[name]
        # bool is an int subclass and is accepted, matching the list renderer.
        if isinstance(value, (str, int, float)):
            if coerced is None:
                coerced = dict(variables)
            coerced[name] = [value]
    return variables if coerced is None else coerced


class ColumnRef(BaseModel):
    """Reference to a dimension by name.

    Supports dotted paths for joined models: "status", "customers.name",
    "customers.regions.name" (multi-hop). Dots are parsed at validation time:
    everything before the last dot goes into ``model``, the leaf stays in ``name``.

    Computed dimensions (SQL expressions) should be defined via ModelExtension
    on the query's model.
    """
    name: str
    model: str | None = None
    label: str | None = None

    @model_validator(mode="after")
    def _parse_dotted_name(self) -> "ColumnRef":
        """Parse dotted paths into model + leaf name.

        "customers.regions.name" → model="customers.regions", name="name"
        "customers.name"         → model="customers",         name="name"
        "status"                 → model=None,                 name="status"
        """
        if self.model is None and "." in self.name:
            prefix, leaf = self.name.rsplit(".", 1)
            self.model = prefix
            self.name = leaf
        # Validate leaf name (must be a simple identifier, no dots)
        if not _NAME_PATTERN.match(self.name):
            raise ValueError(
                f"Invalid name '{self.name}': must contain only letters, "
                f"digits, and underscores, and start with a letter or underscore"
            )
        # Validate each part of the model path
        if self.model:
            for part in self.model.split("."):
                if not _NAME_PATTERN.match(part):
                    raise ValueError(
                        f"Invalid model path '{self.model}': each part must contain "
                        f"only letters, digits, and underscores"
                    )
        return self

    @property
    def full_name(self) -> str:
        if self.model:
            return f"{self.model}.{self.name}"
        return self.name

    @classmethod
    def from_string(cls, s: str) -> ColumnRef:
        """Create a ColumnRef from a string. Dots are parsed by the validator."""
        return cls(name=s)


def _auto_name_from_expression(expression: str) -> str:
    """A deterministic identifier for an unnamed computed dimension.

    Sanitises the expression text to an identifier; when that is over-long it is
    shortened to ``<head>_<hash8>_<tail>`` so distinct expressions never collide
    on a truncated prefix. Name the dimension to control the result key.
    """
    base = re.sub(r"\W+", "_", expression.strip()).strip("_").lower() or "expr"
    if base[0].isdigit():
        base = f"e_{base}"
    if len(base) > 48:
        digest = hashlib.sha256(expression.encode("utf-8")).hexdigest()[:8]
        base = f"{base[:28]}_{digest}_{base[-8:]}"
    # No ``__`` — reserved for join-path aliases in generated SQL.
    return re.sub(r"_+", "_", base).strip("_")


class ComputedDimension(BaseModel):
    """A dimension defined by a Mode-B expression rather than a bare reference.

    ``expression`` is grouped by (and projected). ``name`` is the result key's
    leaf (``model.<name>``); when omitted it is derived from the expression.
    An aggregate inside the expression must carry ``partition_by=`` and triggers
    the aggregate-then-regroup path (DEV-1740).
    """

    model_config = ConfigDict(extra="forbid")

    expression: str
    name: str | None = None

    @model_validator(mode="after")
    def _fill_name(self) -> "ComputedDimension":
        if self.name is None:
            self.name = _auto_name_from_expression(self.expression)
        _validate_model_name(self.name, "Computed dimension")
        return self


def _coerce_column_ref(v: Any) -> Any:
    """Allow plain string where a ColumnRef is expected: "x" → {"name": "x"}."""
    if isinstance(v, str):
        return {"name": v}
    return v


_FUNCSTYLE_CALL_PATTERN = re.compile(r"^\w+\([^()]*\)$")


# DEV-1733: sentinel ``ColumnRef.name`` values meaning "this ORDER BY item is
# an EXPRESSION — resolve it from ``raw_formula``, not as a column reference".
# ``_funcstyle_pending`` marks an unrewritten function-style call (a custom
# aggregation or a transform); ``_expr_pending`` marks any other formula shape
# that is not expressible as a ``ColumnRef`` (composite arithmetic, a scalar
# call over an aggregation, arithmetic over a transform). Consumers MUST also
# require a non-empty ``raw_formula`` before treating a name as a sentinel, so
# a model that genuinely has a column of that name still resolves normally.
_FUNCSTYLE_PENDING = "_funcstyle_pending"
_EXPR_PENDING = "_expr_pending"
ORDER_PLACEHOLDER_NAMES = frozenset({_FUNCSTYLE_PENDING, _EXPR_PENDING})


def _order_formula_candidate(v: str) -> str | None:
    """The func-style-rewritten form of ``v`` when it carries a measure
    expression (a colon aggregation, or a function-style call), else ``None``.

    Single source of truth for "this ORDER BY string is a formula, not a column
    reference". Shared by :meth:`OrderItem._capture_raw_formula` (which
    preserves the original text) and :func:`_coerce_order_column` (which emits
    the placeholder ``ColumnRef``) so the two cannot drift — if only one of
    them recognised a shape, the item would either lose its formula or bind a
    meaningless placeholder name.
    """
    rewritten = _rewrite_funcstyle_aggregations(v)
    if ":" in rewritten or _FUNCSTYLE_CALL_PATTERN.match(rewritten):
        return rewritten
    return None


def _is_valid_column_ref_name(name: str) -> bool:
    """Whether ``name`` parses as a ``ColumnRef`` (bare leaf or dotted path)."""
    try:
        ColumnRef.model_validate({"name": name})
    except Exception:
        return False
    return True


def _coerce_order_column(v: Any) -> Any:
    """Coerce ORDER BY column, normalizing aggregation syntax.

    Handles both colon syntax and function-style syntax for built-in
    aggregations. Converts to the underscore form that matches enriched
    measure names.

    Examples:
    - "revenue:sum" → "revenue_sum"
    - "*:count" → "_count"
    - "sum(revenue)" → "revenue_sum"
    - "revenue:last(ordered_at)" → "revenue_last"
    - "rolling_avg(revenue)" → placeholder, raw_formula carries the call so
      binding can resolve it via ``extra_agg_names``.
    - "revenue:sum / cnt:sum" → placeholder (DEV-1733), raw_formula carries the
      composite so the planner binds it as an expression.

    DEV-1733: a composite that is NOT a formula candidate — ``"rev / cnt"``,
    arithmetic over declared measure ALIASES — falls through to normal
    ``ColumnRef`` validation and keeps its original error. Alias references
    inside expressions are unsupported everywhere in SLayer, so failing fast at
    construction is better than a deep binder error.
    """
    if isinstance(v, str):
        candidate = _order_formula_candidate(v)
        rewritten = (
            candidate if candidate is not None
            else _rewrite_funcstyle_aggregations(v)
        )
        if _FUNCSTYLE_CALL_PATTERN.match(rewritten):
            # Unrewritten function-style call (custom aggregation). Enrichment
            # parses raw_formula with custom_agg_names and overwrites
            # column.name with the canonical alias, so a placeholder is fine.
            return {"name": _FUNCSTYLE_PENDING}
        if ":" in rewritten:
            base, agg = rewritten.rsplit(":", 1)
            agg_name = agg.split("(", 1)[0]  # strip arglist
            if base == "*":
                rewritten = f"_{agg_name}"
            else:
                rewritten = f"{base}_{agg_name}"
        if candidate is not None and not _is_valid_column_ref_name(rewritten):
            # A formula that does not canonicalise to a column reference —
            # composite arithmetic, a scalar call over an aggregation, or
            # arithmetic over a transform. ``raw_formula`` carries the original.
            return {"name": _EXPR_PENDING}
        return {"name": rewritten}
    return v


# DEV-1575: the accepted ORDER BY direction vocabulary (case-insensitive),
# mapping every synonym to the canonical lowercase form the SQL generator
# compares against (``direction == "asc"``). Single source of truth shared by
# the shorthand-healing detector (``_process_order_item``) and the
# ``OrderItem.direction`` normalizing validator.
_DIRECTION_NORMALIZE = {
    "asc": "asc",
    "ascending": "asc",
    "desc": "desc",
    "descending": "desc",
}


def _is_direction(value: Any) -> bool:
    """True if ``value`` is a recognized direction word (case/whitespace-insensitive)."""
    return isinstance(value, str) and value.strip().lower() in _DIRECTION_NORMALIZE


class TimeDimension(BaseModel):
    dimension: Annotated[ColumnRef, BeforeValidator(_coerce_column_ref)]
    granularity: TimeGranularity
    date_range: list[str] | None = None
    label: str | None = None


class OrderItem(BaseModel):
    # DEV-1575: reject stray keys so a mixed canonical+shorthand item
    # (e.g. ``{"column": "x", "b": "asc"}``) raises loudly instead of silently
    # dropping the extra key.
    model_config = ConfigDict(extra="forbid")

    column: Annotated[ColumnRef, BeforeValidator(_coerce_order_column)]
    direction: str = "asc"
    raw_formula: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _capture_raw_formula(cls, data: Any) -> Any:
        """Capture the raw column formula before coercion normalizes it.

        Shares :func:`_order_formula_candidate` with ``_coerce_order_column``
        so a shape can never be recognised by one and not the other.
        """
        if isinstance(data, dict):
            col = data.get("column")
            if isinstance(col, str):
                candidate = _order_formula_candidate(col)
                if candidate is not None:
                    data = {**data, "raw_formula": candidate}
        return data

    @field_validator("direction")
    @classmethod
    def _normalize_direction(cls, v: str) -> str:
        """DEV-1575: normalize direction to canonical ``asc``/``desc`` (case- and
        whitespace-insensitive, accepting ``ascending``/``descending`` synonyms),
        rejecting anything else.

        The SQL generator compares ``direction == "asc"`` strictly, so a
        non-normalized value (``"ASC"``, ``"ascending"``) would silently emit
        DESC. Normalizing here fixes that for both healed and canonical items.
        """
        if _is_direction(v):
            return _DIRECTION_NORMALIZE[v.strip().lower()]
        raise ValueError(
            "order direction must be one of asc/desc/ascending/descending "
            f"(case-insensitive), got {v!r}"
        )


def _coerce_measures(v: Any) -> Any:
    """Allow plain strings in the measures list: "count" → {"formula": "count"}."""
    if v is None:
        return v
    if not isinstance(v, (list, tuple)):
        raise TypeError(f"'measures' must be a list, got {type(v).__name__}")
    return [{"formula": item} if isinstance(item, str) else item for item in v]


def _coerce_dimension_item(item: Any) -> Any:
    """Coerce one ``dimensions`` entry (DEV-1740).

    A bare identifier / dotted path stays a ``ColumnRef``; any other string is
    parsed as a Mode-B expression → ``ComputedDimension`` (a string that is
    neither raises, naming both readings). A dict with ``expression`` is a
    ``ComputedDimension``; other dicts / instances pass through unchanged.
    """
    if isinstance(item, (ColumnRef, ComputedDimension)):
        return item
    if isinstance(item, dict):
        if "expression" in item:
            return ComputedDimension(**item)
        return item
    if isinstance(item, str):
        if _is_valid_column_ref_name(item):
            return {"name": item}
        try:
            parse_expr(item)
        except Exception as exc:
            raise ValueError(
                f"Dimension {item!r} is neither a column reference (a bare "
                f"identifier or dotted join path) nor a parseable Mode-B "
                f"expression: {exc}"
            ) from exc
        return ComputedDimension(expression=item)
    return item


def _coerce_dimensions(v: Any) -> Any:
    """Allow plain strings / expression dicts in the dimensions list."""
    if v is None:
        return v
    if not isinstance(v, (list, tuple)):
        raise TypeError(f"'dimensions' must be a list, got {type(v).__name__}")
    return [_coerce_dimension_item(item) for item in v]


def _process_order_item(item: Any) -> list:
    """DEV-1575: heal a single ``order`` entry, returning the list of canonical
    items it expands to.

    LLM agents frequently write a shorthand dict mapping column → direction
    instead of the canonical ``{"column", "direction"}`` shape. A dict with no
    ``column``/``direction`` key whose values are *all* direction words is
    treated as shorthand and expanded — one canonical item per key, preserving
    insertion order (so a single-key dict yields one item and a multi-key dict
    yields several). Everything else passes through unchanged: canonical dicts
    (their stray keys are policed by ``OrderItem``'s ``extra="forbid"``) and
    malformed input (rejected by ``OrderItem`` validation).
    """
    if not isinstance(item, dict):
        return [item]
    # A dict carrying a reserved key is canonical-intended; never reinterpret it
    # as shorthand. extra="forbid" on OrderItem rejects any stray keys.
    if "column" in item or "direction" in item:
        return [item]
    if (
        item
        and all(isinstance(k, str) for k in item)
        and all(_is_direction(val) for val in item.values())
    ):
        return [{"column": k, "direction": val} for k, val in item.items()]
    return [item]


def _coerce_order(v: Any) -> Any:
    """DEV-1575: heal shorthand ``order`` items before ``OrderItem`` validation.

    Accepts the canonical ``list``/``tuple`` of items, healing each shorthand
    dict (see ``_process_order_item``). A bare single item passed without the
    enclosing list (a ``dict`` or an ``OrderItem``) is wrapped into a
    one-element list; any other non-list input raises (mirrors the
    ``_coerce_measures``/``_coerce_dimensions`` convention).
    """
    if v is None:
        return v
    if not isinstance(v, (list, tuple)):
        if isinstance(v, (dict, OrderItem)):
            v = [v]
        else:
            raise TypeError(f"'order' must be a list, got {type(v).__name__}")
    result: list = []
    for item in v:
        result.extend(_process_order_item(item))
    return result


class ModelExtension(BaseModel):
    """Extend an existing model with extra columns, measures, or joins.

    Used inline on a query to add computed columns (SQL expressions),
    extra joins, or additional measure formulas without modifying the
    stored model.
    """
    source_name: str                                # Model/query to extend
    columns: list | None = None                  # Extra Column objects
    measures: list[ModelMeasure] | None = None   # Extra ModelMeasure formulas
    joins: list | None = None                    # Extra ModelJoin objects


def _get_source_model_name(source_model: object) -> str | None:
    """Extract the model name from any source_model type.

    Works before model resolution — handles str, dict, ModelExtension,
    and SlayerModel (or any object with a .name attribute).
    """
    if isinstance(source_model, str):
        return source_model
    if isinstance(source_model, dict):
        return source_model.get("source_name") or source_model.get("name")
    # ModelExtension has .source_name; SlayerModel has .name
    source_name = getattr(source_model, "source_name", None)
    if isinstance(source_name, str):
        return source_name
    name = getattr(source_model, "name", None)
    if isinstance(name, str):
        return name
    return None


def _strip_column_ref(ref, model_name: str):
    """Strip source model prefix from a ColumnRef.

    "orders.status"          on model "orders" → model=None,  name="status"
    "orders.customers.name"  on model "orders" → model="customers", name="name"
    "customers.name"         on model "orders" → unchanged
    "status"                 on model "orders" → unchanged

    A ``ComputedDimension`` carries a free-form expression, not a model-qualified
    reference, so it passes through untouched.
    """
    if isinstance(ref, ComputedDimension):
        return ref
    if ref.model is None:
        return ref
    if ref.model == model_name:
        return ref.model_copy(update={"model": None})
    prefix = model_name + "."
    if ref.model.startswith(prefix):
        return ref.model_copy(update={"model": ref.model[len(prefix):]})
    return ref


class SlayerQuery(BaseModel):
    """User-facing query object. Specifies what data to retrieve from a model.

    This is intentionally minimal — just names and references, no SQL.
    The query engine plans it into a ``PlannedQuery`` for execution.

    Use ``measures`` for computed/aggregated values and ``filters`` for
    conditions::

        measures=[{"formula": "*:count"}, {"formula": "revenue:sum / *:count", "name": "aov"}]
        filters=["status == 'completed'", "amount > 100"]
    """

    model_config = ConfigDict(extra="forbid")

    version: int = 3
    name: str | None = None  # For referencing this query from other queries in a list
    source_model: object  # str (model name), SlayerModel (inline), or ModelExtension
    measures: Annotated[list[ModelMeasure] | None, BeforeValidator(_coerce_measures)] = None

    @model_validator(mode="before")
    @classmethod
    def _apply_schema_migrations(cls, data: Any) -> Any:
        return _migrate_schema(entity="SlayerQuery", data=data)

    @field_validator("name")
    @classmethod
    def _validate_query_name(cls, v: str | None) -> str | None:
        # Share the same rejection rules as SlayerModel.name —
        # SlayerQuery names occupy the same naming space when persisted
        # as query-backed models. Rejects ``__`` (join-path alias
        # separator), ``.`` (dotted reference syntax), and ``:`` (DSL
        # aggregation separator).
        if v is None:
            return v
        return _validate_model_name(v, "Query")
    dimensions: Annotated[list[ColumnRef | ComputedDimension] | None, BeforeValidator(_coerce_dimensions)] = None
    time_dimensions: list[TimeDimension] | None = None
    main_time_dimension: str | None = None  # Explicit time dimension for transforms (overrides auto-detection)
    filters: list[str] | None = None
    variables: dict[str, Any] | None = None  # Variable values for filter substitution
    order: Annotated[list[OrderItem] | None, BeforeValidator(_coerce_order)] = None
    limit: int | None = None
    offset: int | None = None
    whole_periods_only: bool = False
    # DEV-1543: opt out of the auto "distinct dimension tuples" GROUP BY
    # for dim-only queries. Default ``True`` preserves the Cube.js-style
    # dedup that fires when ``measures`` is empty. Setting ``False`` emits
    # a flat ``SELECT <dims/td-exprs> FROM ... WHERE ... ORDER BY ...
    # LIMIT ...`` projection. Any measure reference (in ``measures``, in
    # ``filters``, or in ``order``) is rejected with
    # ``DistinctDimensionValuesError``; both ``dimensions`` and
    # ``time_dimensions`` empty is also rejected (nothing to project).
    distinct_dimension_values: bool = True

    @model_validator(mode="after")
    def _validate_dsl_user_input(self) -> "SlayerQuery":
        """DEV-1369: enforce DSL-mode rules on every user-input string field.

        Filter strings are pre-parsed in DSL mode so raw ``OVER (...)``
        is caught at construction time with an actionable error message.
        Bare-name strict resolution and raw-SQL-function rejection happen
        at binding, where the parser has full custom-aggregation and
        named-measure context.

        Note: ``__`` is **not** rejected here. Virtual-model columns
        produced by ``_query_as_model`` flatten join paths into single
        identifiers like ``kpis__total_amount_sum``, which downstream
        stages reference directly. Strict resolution at binding
        catches typos that don't resolve to any column / measure.
        """
        if self.filters:
            for f in self.filters:
                _validate_query_filter_string(f)
        self._validate_distinct_dimension_values()
        return self

    def _validate_distinct_dimension_values(self) -> None:
        """DEV-1543: structural rejection rules for ``distinct_dimension_values=False``.

        Only the cheap, model-free checks fire here:

        * ``measures`` non-empty — flag asks for raw rows, but the query
          asks for aggregations.
        * Both ``dimensions`` and ``time_dimensions`` empty — there are
          no projected columns to ``SELECT``.

        Deep filter / order measure-reference checks happen at binding,
        where named measures, custom aggregations, and post-substitution
        text are all available. Detecting them here would either reject
        valid ``{var}`` filters before substitution or miss model-defined
        custom aggregations.
        """
        if self.distinct_dimension_values:
            return
        if self.measures:
            n = len(self.measures)
            raise DistinctDimensionValuesError(
                f"distinct_dimension_values=False requires an empty `measures` "
                f"field, but {n} measure(s) were supplied. Either remove the "
                f"measures (and any other measure references) or set "
                f"distinct_dimension_values=True (the default) to keep the "
                f"auto-aggregating behaviour."
            )
        if not self.dimensions and not self.time_dimensions:
            raise DistinctDimensionValuesError(
                "distinct_dimension_values=False requires at least one of "
                "`dimensions` or `time_dimensions` to be non-empty — there "
                "are no columns to SELECT. Add the columns you want to "
                "project."
            )

    def snap_to_whole_periods(self) -> "SlayerQuery":
        """Adjust date filters to align with period boundaries when whole_periods_only=True.

        For each time dimension with a granularity, adds a date range filter
        to exclude the current incomplete period if no date filter exists.
        """
        if not self.whole_periods_only or not self.time_dimensions:
            return self

        filters = list(self.filters or [])
        for td in self.time_dimensions:
            gran = td.granularity
            dim_name = td.dimension.name

            # Check if any filter already references this time dimension
            has_filter = any(dim_name in f for f in filters)
            if not has_filter:
                # Add filter to exclude current incomplete period
                today = datetime.date.today()
                prev_end = gran.period_end(gran.period_start(today) - datetime.timedelta(days=1))
                filters.append(f"{dim_name} <= '{prev_end.isoformat()}'")

        return self.model_copy(update={"filters": filters, "whole_periods_only": False})

    def strip_source_model_prefix(self) -> "SlayerQuery":
        """Strip redundant source model name prefix from all dotted references.

        LLMs frequently include the source model name as a prefix
        (e.g., "orders.revenue:sum" instead of "revenue:sum" when
        querying source_model="orders"). This normalizes all references
        by removing the redundant prefix before any other processing.
        """
        model_name = _get_source_model_name(self.source_model)
        if model_name is None:
            return self

        updates: dict[str, Any] = {}
        pattern = re.compile(r"\b" + re.escape(model_name) + r"\.")

        # Dimensions
        if self.dimensions:
            new_dims = [_strip_column_ref(d, model_name) for d in self.dimensions]
            if any(n is not o for n, o in zip(new_dims, self.dimensions)):
                updates["dimensions"] = new_dims

        # Time dimensions
        if self.time_dimensions:
            new_tds = []
            td_changed = False
            for td in self.time_dimensions:
                stripped = _strip_column_ref(td.dimension, model_name)
                if stripped is not td.dimension:
                    new_tds.append(TimeDimension(
                        dimension=stripped,
                        granularity=td.granularity,
                        date_range=td.date_range,
                        label=td.label,
                    ))
                    td_changed = True
                else:
                    new_tds.append(td)
            if td_changed:
                updates["time_dimensions"] = new_tds

        # Order
        if self.order:
            new_order = []
            order_changed = False
            for item in self.order:
                stripped = _strip_column_ref(item.column, model_name)
                stripped_raw_formula = (
                    pattern.sub("", item.raw_formula) if item.raw_formula else None
                )
                if stripped is not item.column or stripped_raw_formula != item.raw_formula:
                    new_order.append(OrderItem(
                        column=stripped,
                        direction=item.direction,
                        raw_formula=stripped_raw_formula,
                    ))
                    order_changed = True
                else:
                    new_order.append(item)
            if order_changed:
                updates["order"] = new_order

        # Measures (formula strings)
        if self.measures:
            new_measures = []
            measures_changed = False
            for f in self.measures:
                new_formula = pattern.sub("", f.formula)
                if new_formula != f.formula:
                    new_measures.append(f.model_copy(update={"formula": new_formula}))
                    measures_changed = True
                else:
                    new_measures.append(f)
            if measures_changed:
                updates["measures"] = new_measures

        # Filters
        if self.filters:
            new_filters = [pattern.sub("", f) for f in self.filters]
            if new_filters != self.filters:
                updates["filters"] = new_filters

        # main_time_dimension
        prefix = model_name + "."
        if self.main_time_dimension and self.main_time_dimension.startswith(prefix):
            updates["main_time_dimension"] = self.main_time_dimension[len(prefix):]

        if not updates:
            return self

        # Sanitize for log injection (S5145): model names are usually trusted
        # internal identifiers, but they originate from user input via the
        # public API, so strip CR/LF before logging.
        safe_name = model_name.replace("\r", "\\r").replace("\n", "\\n")
        logger.info(
            "Stripped source model prefix '%s.' from query references",
            safe_name,
        )
        return self.model_copy(update=updates)
