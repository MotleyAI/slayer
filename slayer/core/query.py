"""Query models for SLayer — the user-facing ``SlayerQuery`` and its helpers."""
from __future__ import annotations

import json
import logging
import math
import re
from collections.abc import Callable
from typing import Annotated, Any, Literal, Union

from pydantic import (
    AfterValidator,
    AliasChoices,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Discriminator,
    Field,
    PrivateAttr,
    Tag,
    field_validator,
    model_validator,
)

from slayer.core.direction import normalize_direction
from slayer.core.enums import BUILTIN_AGGREGATIONS, GRANULARITY_NAMES, TimeGranularity, normalize_aggregation_name
from slayer.core.formula import ALL_TRANSFORMS
from slayer.core.keys import SCALAR_FUNCTIONS
from slayer.core.errors import DistinctDimensionValuesError, GranularityCallError, RefinementConflictError
from slayer.core.granularity import GranularitySpec, granularity_key
from slayer.core.models import (
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
    _validate_model_name,
)
from slayer.core.refs import auto_name_from_expression
from slayer.core.time_points import TIME_POINT_FORMS, is_time_point_shape
from slayer.engine.syntax import AggCall, DottedRef, Ref, parse_expr, walk_parsed_refs
from slayer.sql.window_detect import WINDOW_IN_FILTER_ERROR, has_window_function
from slayer.storage.migrations import CURRENT_VERSIONS, migrate as _migrate_schema

logger = logging.getLogger(__name__)

#: The SlayerQuery version whose migration retired ``strict``.
_STRICT_RETIRED_AT = 4

_NAME_PATTERN = re.compile(r"^[a-zA-Z_]\w*$", re.ASCII)
_VAR_PATTERN = re.compile(r"\{\{|\}\}|\{([a-zA-Z_]\w*)\}|\{([^}]*)\}", re.ASCII)

# Leading callee of a whole-string single call ``name( ... )``; used only when
# ``parse_expr`` rejects the shape (e.g. ``month()``) but the callee still names a
# granularity, so the wrong-shape error can fire.
_WHOLE_CALL_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s*\(.*\)\s*$", re.S)


def _reject_call_placeholders(entry: str) -> None:
    """A functional time-dimension string whose granularity or column is a ``{var}`` is refused."""
    # Any callee text, so a ``{g}(col)`` placeholder callee is caught.
    head, paren, tail = entry.partition("(")
    body = tail.rstrip()
    if paren and ")" not in head and body.endswith(")"):
        _reject_time_dimension_placeholders(entry, granularity=head.strip(), column=body[:-1])


def _granularity_names() -> str:
    return ", ".join(g.value for g in TimeGranularity)


def _single_col_of_source(node: AggCall) -> str | None:
    """The dotted column name if ``node``'s source is a lone ref, else ``None``."""
    src = node.source
    if isinstance(src, Ref):
        return src.name
    if isinstance(src, DottedRef):
        return ".".join(src.parts)
    return None


def call_callee(entry: str) -> tuple[Any, str | None]:
    """``entry`` parsed (``None`` when unparseable) and its whole-call callee, if any."""
    try:
        node: Any = parse_expr(entry)
    except Exception:
        node = None
    if isinstance(node, AggCall):
        return node, node.agg
    m = _WHOLE_CALL_RE.match(entry)
    return node, (m.group(1) if m else None)


def functional_call_column(node: Any) -> str | None:
    """The column of a single-column call ``name(col)``, else ``None``."""
    if isinstance(node, AggCall) and not node.args and not node.kwargs:
        return _single_col_of_source(node)
    return None


def time_dimension_from_functional(entry: str, *, names: "frozenset[str] | set[str]") -> dict | None:
    """A ``gran(col)`` string whose casefolded callee is in ``names`` → its ``TimeDimension``
    dict; another callee or a non-call → ``None``; a granularity callee of any other shape → raise."""
    node, callee = call_callee(entry)
    if callee is None or callee.casefold() not in names:
        return None
    col = functional_call_column(node)
    if col is None:
        _reject_call_placeholders(entry)
        raise GranularityCallError.wrong_shape(entry)
    return {"dimension": col, "granularity": callee.lower() if callee.lower() in GRANULARITY_NAMES else callee}


def granularity_call_parts(entry: str) -> tuple[str, str] | None:
    """``(col, callee)`` for a well-formed single-column call string, else ``None`` (never raises);
    used to resolve a functional order key against projected time dimensions."""
    node, callee = call_callee(entry)
    col = functional_call_column(node)
    if callee is None or col is None:
        return None
    return col, (callee.lower() if callee.lower() in GRANULARITY_NAMES else callee)


def _split_functional_dimensions(dims: "list | tuple") -> tuple[list, list]:
    """Partition ``dimensions`` into (kept, extracted-TD-dicts): a built-in ``gran(col)``
    string becomes a ``TimeDimension`` dict; any other call resolves at binding."""
    kept: list = []
    rewritten: list = []
    for item in dims:
        if isinstance(item, str):
            td = time_dimension_from_functional(item, names=GRANULARITY_NAMES)
            if td is not None:
                rewritten.append(td)
                continue
        kept.append(item)
    return kept, rewritten


def _coerce_time_dimension_entry(entry: Any) -> Any:
    """A string ``time_dimensions`` entry must be the functional ``gran(col)`` form (a
    non-built-in callee resolves at binding); dicts/objects pass through untouched."""
    if not isinstance(entry, str):
        return entry
    _reject_call_placeholders(entry)
    td = time_dimension_from_functional(entry, names=GRANULARITY_NAMES)
    if td is not None:
        return td
    node, callee = call_callee(entry)
    col = functional_call_column(node)
    if callee is None or col is None:
        raise GranularityCallError(
            f"Time dimension {entry!r} must be the functional form "
            f"``gran(col)`` (e.g. ``month(created_at)``) for one of: "
            f"{_granularity_names()} or a datasource granularity; no default granularity is invented."
        )
    return {"dimension": col, "granularity": callee}


def _rewrite_functional_granularity(data: dict) -> dict:
    """Move ``gran(col)`` dimension strings into ``time_dimensions`` and coerce
    string ``time_dimensions`` entries; both accept only the functional form."""
    rewritten: list = []
    dims = data.get("dimensions")
    if isinstance(dims, (list, tuple)):
        kept, rewritten = _split_functional_dimensions(dims)
        if len(kept) != len(dims):
            # Consuming every entry leaves ``None`` (not ``[]``) so the dump
            # matches an explicitly time-dimension-only query.
            data = {**data, "dimensions": kept or None}
    tds = data.get("time_dimensions")
    # A malformed non-list ``time_dimensions`` is left untouched so field
    # validation rejects it cleanly, rather than a silent replace / TypeError.
    if tds is not None and not isinstance(tds, (list, tuple)):
        return data
    if tds or rewritten:
        coerced = [_coerce_time_dimension_entry(entry) for entry in (tds or [])]
        coerced.extend(rewritten)
        data = {**data, "time_dimensions": coerced}
    return data


def _validate_query_filter_string(formula: str) -> None:
    """Reject raw ``OVER (...)`` window syntax in a ``SlayerQuery.filters`` entry."""
    if has_window_function(formula):
        raise ValueError(f"Filter '{formula}' {WINDOW_IN_FILTER_ERROR}")


# C0 controls (U+0000–U+001F) → Python string-literal escapes for the "python"
# regime: a raw newline/CR/NUL in a single-quoted literal makes ast.parse raise.
_C0_NAMED_ESCAPES = {"\t": "\\t", "\n": "\\n", "\r": "\\r"}
_C0_ESCAPE_MAP = {
    chr(codepoint): _C0_NAMED_ESCAPES.get(chr(codepoint), f"\\x{codepoint:02x}")
    for codepoint in range(0x20)
}
_C0_RE = re.compile(r"[\x00-\x1f]")


def _escape_string_value(
    value: str, escape: Literal["sql", "python"], *, backslash_escapes: bool
) -> str:
    """Escape a string value for the target layer: ``"sql"`` is dialect-aware via
    ``backslash_escapes`` (double quote deliberately left untouched); ``"python"`` backslash-escapes
    quotes and encodes C0 controls (SQL quote-doubling would concatenate in the Mode-B AST parser)."""
    if escape == "sql":
        if backslash_escapes:
            # order matters: double the backslash before escaping the quote.
            return value.replace("\\", "\\\\").replace("'", "\\'")
        return value.replace("'", "''")
    # order matters: backslash before quotes, then encode C0 controls.
    escaped = value.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')
    return _C0_RE.sub(lambda m: _C0_ESCAPE_MAP[m.group(0)], escaped)


def _render_list_value(
    name: str,
    value: "list | tuple",
    escape: Literal["sql", "python"],
    *,
    backslash_escapes: bool,
) -> str:
    """Render a ``list``/``tuple`` into an ``IN``-list body: template writes
    the parens (``col IN ({var})``), string elements auto-quoted. ``escape="python"``
    appends a trailing comma to force 1-tuple parsing; empty list raises (``IN ()`` invalid)."""
    if len(value) == 0:
        raise ValueError(
            f"Variable '{name}' cannot be an empty list; 'IN ()' is invalid SQL. "
            f"For 'no filter' semantics, use a sentinel default."
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
    # sql must NOT have a trailing comma (``IN (1, 2,)`` is a syntax error).
    return f"{joined}," if escape == "python" else joined


def _render_variable_value(
    name: str,
    value: Any,
    escape: Literal["sql", "python"],
    *,
    backslash_escapes: bool,
) -> str:
    """Render one resolved variable value into substitution text (see the two helpers)."""
    # list/tuple first, so the scalar path only ever sees a single value.
    if isinstance(value, (list, tuple)):
        return _render_list_value(
            name=name, value=value, escape=escape, backslash_escapes=backslash_escapes
        )
    if isinstance(value, str):
        return _escape_string_value(
            value=value, escape=escape, backslash_escapes=backslash_escapes
        )
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
    """Index of the ``?}`` closing the block at ``start`` (blocks don't nest; ``{{``/``}}`` skipped)."""
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
    """Split ``text`` into ``("text", s)`` / ``("block", inner)`` parts on top-level ``{? ?}`` spans."""
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
    """Valid ``{var}`` names inside a block; raises on an invalid name or an empty block."""
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
    filter_str: str, variables: dict, escape: Literal["sql", "python"], backslash_escapes: bool
):
    """Build the ``re.sub`` replacement callable for ``{var}`` / ``{{`` / ``}}`` tokens."""

    def _replace(match: re.Match) -> str:
        full = match.group(0)
        if full == "{{":
            return "{"
        if full == "}}":
            return "}"
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
    """Substitute ``{var}`` placeholders in a filter or raw-SQL string (``{{``/``}}`` → literal
    braces). ``escape`` (keyword-only) picks the regime; ``backslash_escapes`` is the fail-closed
    dialect signal for ``"sql"``. A ``list``/``tuple`` renders an auto-quoted ``IN``-list body."""
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
    # python mode ignores the signal (Mode-B escaping is dialect-independent).
    effective_backslash_escapes = bool(backslash_escapes) if escape == "sql" else False
    _replace = _make_var_replacer(
        filter_str, variables, escape, effective_backslash_escapes
    )

    # Optional blocks are Mode-A only; Mode-B rejects them outright.
    if escape == "python":
        if _contains_block_delimiter(filter_str):
            raise ValueError(
                f"Optional blocks '{{? ... ?}}' are supported only on raw-SQL model "
                f"surfaces, not in Mode-B expressions: {filter_str!r}."
            )
        return _VAR_PATTERN.sub(_replace, filter_str)

    # Fast path: no blocks -> single-pass regex sub.
    if not _contains_block_delimiter(filter_str):
        return _VAR_PATTERN.sub(_replace, filter_str)

    return _render_block_segments(filter_str, variables, _replace)


def _render_block_segments(filter_str: str, variables: dict, replace_fn) -> str:
    """Render a Mode-A string with ``{? ?}`` blocks: parenthesised when every inner ``{var}`` is supplied, else ``(1=1)``."""
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


def query_variable_surfaces(query: "SlayerQuery") -> list[str]:
    """Every text of ``query`` that substitutes ``{var}``: filters, measure formulas,
    computed-dimension expressions, order expressions and ``date_range`` bounds."""
    return [
        *(query.filters or []),
        *(m.formula for m in query.measures or []),
        *(d.expression for d in query.dimensions or [] if isinstance(d, ComputedDimension)),
        *(o.raw_formula for o in query.order or [] if o.raw_formula),
        *(b for td in query.time_dimensions or [] for b in td.date_range or [] if b is not None),
    ]


def _bare_variable_names(text: str) -> set[str]:
    return {m.group(1) for m in _VAR_PATTERN.finditer(text) if m.group(1)}


def has_variable_syntax(text: str) -> bool:
    """True if ``text`` holds any ``{...}`` / ``{{`` / ``}}`` token substitution rewrites or rejects."""
    return _VAR_PATTERN.search(text) is not None


def extract_placeholder_names(query: "SlayerQuery") -> set:
    """Valid ``{var}`` names referenced on any of ``query``'s substituted surfaces."""
    return set().union(*(_bare_variable_names(text) for text in query_variable_surfaces(query)))


def _probe_replace(match: re.Match) -> str:
    full = match.group(0)
    if full == "{{":
        return "{"
    if full == "}}":
        return "}"
    return "0"  # any {var} (valid or not) -> a syntactically safe literal


def render_probe_text(text: str) -> str:
    """Render a Mode-A surface for a syntax-only sqlglot parse (blocks → ``(1=1)``, ``{var}`` → ``0``); shared so import-time parsing matches execution."""
    out: list[str] = []
    for kind, segment in _split_blocks(text):
        if kind == "block":
            out.append("(1=1)")
        else:
            out.append(_VAR_PATTERN.sub(_probe_replace, segment))
    return "".join(out)


class ModelVariables(BaseModel):
    """A model's ``{var}`` placeholders split into ``required`` (no default, not in a block) and ``optional`` (blocked or defaulted)."""

    required: list[str] = Field(default_factory=list)
    optional: list[str] = Field(default_factory=list)


def extract_variable_refs(text: str) -> tuple[set[str], set[str]]:
    """Return ``(bare_names, blocked_names)`` in a Mode-A ``text`` — outside vs inside a ``{? ?}`` block; a name may land in both."""
    bare: set[str] = set()
    blocked: set[str] = set()
    try:
        parts = _split_blocks(text)
    except ValueError:
        # Malformed delimiters: classify as block-free so read-only inspection
        # doesn't break; execution still raises through substitute_variables.
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
    """Classify a model's ``{var}`` placeholders as required / optional across its four
    Mode-A surfaces and its saved measure formulas. A bare, undefaulted occurrence
    anywhere makes the var required (a measure has no optional blocks); everything else is optional."""
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
    for measure in model.measures:
        bare |= set().union(*extract_variable_refs(measure.formula))

    defaults = set(model.query_variables or {})
    required = {name for name in bare if name not in defaults}
    optional = (bare | blocked) - required
    return ModelVariables(
        required=sorted(required), optional=sorted(optional)
    )


def declared_variable_specs(model: SlayerModel) -> dict[str, dict]:
    """The model's declared Mode-A variable bag (``name -> spec``) from ``meta.cube_variables``,
    or ``{}`` (shape-checked; ``meta`` is user-extensible). An entry counts only with a non-empty
    string ``member``, so a reused ``cube_variables`` key isn't mistaken for generated SQL."""
    declared = (model.meta or {}).get("cube_variables")
    if not isinstance(declared, dict):
        return {}
    return {
        name: spec
        for name, spec in declared.items()
        if isinstance(name, str) and isinstance(spec, dict) and _is_member_name(spec)
    }


def _is_member_name(spec: dict) -> bool:
    """True if ``spec`` carries the non-empty string ``member`` marking a real declaration."""
    member = spec.get("member")
    return isinstance(member, str) and bool(member)


def declares_variables(model: SlayerModel) -> bool:
    """True if the model declares its Mode-A variables (importer-generated SQL), disabling the
    brace-literal protection: a declared but unrendered ``{var}`` must raise, not survive as a raw brace."""
    return bool(declared_variable_specs(model))


def list_valued_variable_names(model: SlayerModel) -> set[str]:
    """Names of Mode-A variables declared ``list_valued: true`` in ``meta.cube_variables``. A
    machine-generated ``col IN ({var})`` can't quote the author, so a scalar renders ``IN (US)``
    (rejected); the flag opts into list-wrapping. Must be exactly ``True`` (truthiness would flip semantics)."""
    return {
        name
        for name, spec in declared_variable_specs(model).items()
        if spec.get("list_valued") is True
    }


def coerce_declared_list_variables(
    variables: dict[str, Any], *, list_valued: set[str]
) -> dict[str, Any]:
    """Wrap a scalar supplied for a declared list-valued variable in a one-element list, so it
    renders ``IN ('US')`` not ``IN (US)`` — a normalisation, not a guess. Only ``str``/``int``/``float``/
    ``bool`` are wrapped; other types (incl. the empty list, which keeps raising) pass through. Never mutates."""
    if not list_valued:
        return variables
    coerced: dict[str, Any] | None = None
    for name in list_valued:
        if name not in variables:
            continue
        value = variables[name]
        if isinstance(value, (str, int, float)):
            if coerced is None:
                coerced = dict(variables)
            coerced[name] = [value]
    return variables if coerced is None else coerced


class ColumnRef(BaseModel):
    """A column reference: bare name or dotted join path (``customers.regions.name``); a short form (``regions.name``) auto-routes when exactly one route exists."""
    name: str
    model: str | None = None
    label: str | None = None

    @model_validator(mode="after")
    def _parse_dotted_name(self) -> "ColumnRef":
        """Split a dotted ``name`` into ``model`` prefix + leaf, then validate both."""
        if self.model is None and "." in self.name:
            prefix, leaf = self.name.rsplit(".", 1)
            self.model = prefix
            self.name = leaf
        if not _NAME_PATTERN.match(self.name):
            raise ValueError(
                f"Invalid name '{self.name}': must contain only letters, "
                f"digits, and underscores, and start with a letter or underscore"
            )
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


class ComputedDimension(BaseModel):
    """A dimension computed by an ``expression``; an aggregation inside must carry ``partition_by=`` to fix its grain. Best given an explicit ``name``."""

    model_config = ConfigDict(extra="forbid")

    expression: str
    name: str | None = None
    # Pre-substitution expression; explicitness is judged against it.
    _template: str | None = PrivateAttr(default=None)

    @property
    def template(self) -> str:
        """The expression as written, before ``{var}`` substitution."""
        return self._template or self.expression

    def substituted(self, expression: str) -> "ComputedDimension":
        """A copy carrying the substituted ``expression`` and remembering its template."""
        out = self.model_copy(update={"expression": expression})
        out._template = self.template
        return out

    @model_validator(mode="after")
    def _fill_name(self) -> "ComputedDimension":
        if self.name is None:
            self.name = auto_name_from_expression(self.expression)
        _validate_model_name(self.name, "Computed dimension")
        return self


def _coerce_column_ref(v: Any) -> Any:
    """Allow plain string where a ColumnRef is expected: "x" → {"name": "x"}."""
    if isinstance(v, str):
        return {"name": v}
    return v


_FUNCSTYLE_CALL_PATTERN = re.compile(r"^\w+\([^()]*\)$")


# Sentinel ``ColumnRef.name`` values: this ORDER BY item is an expression to
# resolve from ``raw_formula``, not a column reference. Consumers must also
# require a non-empty ``raw_formula`` before treating a name as a sentinel, so a
# real column of that name still resolves normally.
_FUNCSTYLE_PENDING = "_funcstyle_pending"
_EXPR_PENDING = "_expr_pending"
ORDER_PLACEHOLDER_NAMES = frozenset({_FUNCSTYLE_PENDING, _EXPR_PENDING})


def _order_formula_candidate(v: str) -> str | None:
    """``v`` verbatim if it carries a measure expression (colon aggregation,
    call-style text, or an expression containing an aggregation), else
    ``None``; shared by ``_capture_raw_formula`` and ``_coerce_order_column``
    so they can't drift. The author's spelling is preserved — resolution
    happens at binding via the native parser. Bare-alias
    arithmetic (``rev / cnt``) is deliberately NOT a candidate — it falls
    through to ColumnRef validation and fails fast."""
    if ":" in v or _FUNCSTYLE_CALL_PATTERN.match(v):
        return v
    if _is_valid_column_ref_name(v):
        return None
    try:
        parsed = parse_expr(v)
    except Exception:
        return None
    if any(isinstance(node, AggCall) for node in walk_parsed_refs(parsed)):
        return v
    return None


def _is_valid_column_ref_name(name: str) -> bool:
    """Whether ``name`` parses as a ``ColumnRef`` (bare leaf or dotted path)."""
    try:
        ColumnRef.model_validate({"name": name})
    except Exception:
        return False
    return True


def _coerce_order_column(v: Any) -> Any:
    """Coerce an ORDER BY column. Colon aggregations normalize to the underscore
    form (``revenue:sum`` → ``revenue_sum``); call-style entries (``sum(revenue)``,
    ``my_agg(price)``) emit a placeholder whose ``raw_formula`` the planner binds;
    other non-column expressions carry ``raw_formula`` under ``_EXPR_PENDING``."""
    if isinstance(v, str):
        candidate = _order_formula_candidate(v)
        if candidate is None:
            return {"name": v}
        if _FUNCSTYLE_CALL_PATTERN.match(v):
            return {"name": _FUNCSTYLE_PENDING}
        if ":" in v:
            base, agg = v.rsplit(":", 1)
            agg_name = agg.split("(", 1)[0]
            rewritten = f"_{agg_name}" if base == "*" else f"{base}_{agg_name}"
            if _is_valid_column_ref_name(rewritten):
                return {"name": rewritten}
        return {"name": _EXPR_PENDING}
    return v


def _is_direction(value: Any) -> bool:
    """True if ``value`` is a recognized direction word (case/whitespace-insensitive)."""
    return normalize_direction(value) is not None


def _coerce_date_range(value: Any) -> Any:
    """A single time-point string is a one-element range."""
    return [value] if isinstance(value, str) else value


def _advertise_string_date_range(schema: dict[str, Any]) -> None:
    """Advertise the single-string ``date_range`` form the before-validator accepts."""
    schema["anyOf"] = [{"type": "string"}, *schema.get("anyOf", [])]


def _date_range_problem(date_range: list[str | None]) -> str | None:
    if not 1 <= len(date_range) <= 2:
        return "must be one time point or a [lower, upper] pair"
    if all(bound is None for bound in date_range):
        return "needs at least one non-null bound"
    # A placeholder bound is checked after substitution (the query is rebuilt then).
    bad = next(
        (b for b in date_range if b is not None and not _bare_variable_names(b) and not is_time_point_shape(b)),
        None,
    )
    return None if bad is None else f"has {bad!r}, which is not {TIME_POINT_FORMS}"


def _reject_time_dimension_placeholders(entry: Any, *, granularity: Any = None, column: Any = None) -> None:
    """Variables supply literals: a ``{var}`` naming a time dimension's granularity or column is refused."""
    for what, value in (("granularity", granularity), ("column", column)):
        if isinstance(value, str) and _bare_variable_names(value):
            raise ValueError(
                f"Time dimension {entry!r}: variables supply literal values; they can't name a {what} ({value!r})."
            )


class TimeDimension(BaseModel):
    """Group-by on ``dimension`` truncated to ``granularity``; optional ``date_range``: one time point or a ``[lower, upper]`` pair (either may be null)."""
    dimension: Annotated[ColumnRef, BeforeValidator(_coerce_column_ref)] = Field(
        validation_alias=AliasChoices("dimension", "column"),
    )
    granularity: GranularitySpec = Field(
        description="A built-in granularity, or a custom granularity defined on the query's datasource.",
    )
    date_range: Annotated[list[str | None] | None, BeforeValidator(_coerce_date_range)] = Field(
        default=None, json_schema_extra=_advertise_string_date_range,
    )
    label: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _reject_placeholder_names(cls, data: Any) -> Any:
        if isinstance(data, dict):
            _reject_time_dimension_placeholders(
                data, granularity=data.get("granularity"), column=data.get("dimension", data.get("column")),
            )
        return data

    @model_validator(mode="after")
    def _check_date_range(self) -> "TimeDimension":
        if self.date_range is not None and (problem := _date_range_problem(self.date_range)):
            raise ValueError(
                f"time dimension {self.dimension.full_name!r}: date_range {self.date_range!r} {problem}."
            )
        return self


def _advertise_string_time_dimensions(schema: dict[str, Any]) -> None:
    """Add the functional ``gran(col)`` string alternative to each ``time_dimensions``
    item in the derived JSON/MCP input schema, without widening the field's Python
    type (strings are coerced by the model before-validator). A callable
    ``json_schema_extra`` works on the pydantic >=2.0 floor."""
    for option in schema.get("anyOf", [schema]):
        if option.get("type") == "array" and "items" in option:
            option["items"] = {"anyOf": [option["items"], {"type": "string"}]}


class OrderItem(BaseModel):
    """A sort key: ``column`` is a result column name or an expression string; ``direction`` asc|desc."""
    # extra="forbid": reject stray keys so a mixed canonical+shorthand item
    # raises instead of silently dropping the extra key.
    model_config = ConfigDict(extra="forbid")

    column: Annotated[ColumnRef, BeforeValidator(_coerce_order_column)]
    direction: str = "asc"
    raw_formula: str | None = Field(
        default=None,
        description="Internal — captured automatically from expression strings; do not set.",
    )

    @model_validator(mode="before")
    @classmethod
    def _capture_raw_formula(cls, data: Any) -> Any:
        """Capture the raw column formula before coercion normalizes it."""
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
        """Normalize direction to canonical ``asc``/``desc`` (else raise); the generator
        compares ``== "asc"`` strictly, so a non-normalized ``"ASC"`` would silently emit DESC."""
        normalized = normalize_direction(v)
        if normalized is not None:
            return normalized
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


def _may_name_datasource_granularity(entry: str) -> bool:
    """A whole call whose callee is no built-in name — possibly a datasource granularity."""
    _, callee = call_callee(entry)
    if callee is None:
        return False
    name = callee.lower()
    return not (
        name in GRANULARITY_NAMES or name in SCALAR_FUNCTIONS or name in ALL_TRANSFORMS
        or normalize_aggregation_name(callee) in BUILTIN_AGGREGATIONS
    )


def _coerce_dimension_item(item: Any) -> Any:
    """Coerce one ``dimensions`` entry: a bare identifier / dotted path stays a ``ColumnRef``, any other string parses as a Mode-B expression → ``ComputedDimension`` (neither raises)."""
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
            if _may_name_datasource_granularity(item):
                return ComputedDimension(expression=item)  # its datasource resolves it at binding
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
    """Heal a single ``order`` entry into the canonical items it expands to; a shorthand dict
    column → direction (no ``column``/``direction`` key, all-direction values) expands one item per key."""
    if not isinstance(item, dict):
        return [item]
    # A dict with a reserved key is canonical-intended, never shorthand.
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
    """Heal shorthand ``order`` items before ``OrderItem`` validation; a bare ``dict``/``OrderItem`` is wrapped into a one-element list, other non-list input raises."""
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


# Field types shared by ``SlayerQuery`` and ``QueryRefinement`` (one coercion each).
MeasuresField = Annotated[list[ModelMeasure] | None, BeforeValidator(_coerce_measures)]
DimensionsField = Annotated[list[ColumnRef | ComputedDimension] | None, BeforeValidator(_coerce_dimensions)]
OrderField = Annotated[list[OrderItem] | None, BeforeValidator(_coerce_order)]


class ModelExtension(BaseModel):
    """Extend a model inline on a query with extra columns, measures, or joins, without modifying the stored model."""

    model_config = ConfigDict(extra="forbid")

    source_name: str                                # Model/query to extend
    columns: list[Column] | None = None
    measures: list[ModelMeasure] | None = None
    joins: list[ModelJoin] | None = None


def _source_spec_tag(value: Any) -> str:
    """Classify a raw ``source_model``: an object carrying ``source_name`` is always an extension."""
    if isinstance(value, ModelExtension):
        return "extension"
    if isinstance(value, SlayerModel):
        return "model"
    if isinstance(value, dict):
        return "extension" if "source_name" in value else "model"
    return "name"


def _reject_inline_query_backed(value: Any) -> Any:
    if isinstance(value, SlayerModel) and value.source_queries:
        raise ValueError(
            f"Inline model {value.name!r} carries source_queries; an inline query-backed "
            f"source is not supported — write those queries as named stages of the "
            f"query list instead."
        )
    return value


# Anything accepted as ``SlayerQuery.source_model``; validated at construction.
SourceSpec = Annotated[
    Union[
        Annotated[str, Tag("name")],
        Annotated[ModelExtension, Tag("extension")],
        Annotated[SlayerModel, Tag("model")],
    ],
    Discriminator(_source_spec_tag),
    AfterValidator(_reject_inline_query_backed),
]


def _strip_column_ref(ref, model_name: str):
    """Strip the source-model prefix from a ColumnRef (a ``ComputedDimension`` passes through)."""
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


def _strip_time_dimensions(tds: list[TimeDimension] | None, model_name: str) -> list[TimeDimension] | None:
    """The prefix-stripped time dimensions, or ``None`` when nothing changed."""
    if not tds:
        return None
    new_tds = []
    for td in tds:
        stripped = _strip_column_ref(td.dimension, model_name)
        if stripped is td.dimension:
            new_tds.append(td)
        else:
            new_tds.append(TimeDimension(
                dimension=stripped, granularity=td.granularity, date_range=td.date_range, label=td.label,
            ))
    return None if all(n is o for n, o in zip(new_tds, tds)) else new_tds


def _strip_order(order: list[OrderItem] | None, model_name: str, pattern: re.Pattern) -> list[OrderItem] | None:
    """The prefix-stripped order items, or ``None`` when nothing changed."""
    if not order:
        return None
    new_order = []
    for item in order:
        stripped = _strip_column_ref(item.column, model_name)
        raw_formula = pattern.sub("", item.raw_formula) if item.raw_formula else None
        if stripped is item.column and raw_formula == item.raw_formula:
            new_order.append(item)
        else:
            new_order.append(OrderItem(column=stripped, direction=item.direction, raw_formula=raw_formula))
    return None if all(n is o for n, o in zip(new_order, order)) else new_order


def _strip_measures(measures: list[ModelMeasure] | None, pattern: re.Pattern) -> list[ModelMeasure] | None:
    """The prefix-stripped measures, or ``None`` when nothing changed."""
    if not measures:
        return None
    new_measures = [
        f if (formula := pattern.sub("", f.formula)) == f.formula else f.model_copy(update={"formula": formula})
        for f in measures
    ]
    return None if all(n is o for n, o in zip(new_measures, measures)) else new_measures


class SlayerQuery(BaseModel):
    """User-facing query object — what to retrieve from a model, as names/references, no SQL."""

    model_config = ConfigDict(extra="forbid")

    version: int = CURRENT_VERSIONS["SlayerQuery"]
    name: str | None = Field(
        default=None,
        description=(
            "Stage name in a multi-stage list; other stages reference it as "
            "their source_model."
        ),
    )
    source_model: SourceSpec | None = Field(
        default=None,
        description=(
            "The query's population: a saved model name, an inline ModelExtension "
            '({"source_name": ..., plus optional "columns"/"measures"/"joins"}), '
            "or a full inline model. Omit to infer the smallest model determining "
            "every queried dimension, time dimension, and row-level filter column (the "
            "choice is reported in response metadata)."
        ),
    )
    measures: MeasuresField = Field(
        default=None,
        description=(
            "Values to return: aggregation-expression formulas (see the query tool "
            "description). A bare name references a saved model measure."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _migrate_and_rewrite(cls, data: Any) -> Any:
        # Single before-validator: migrate, THEN rewrite the functional granularity
        # form. Pydantic runs before-validators in reverse declaration order, so
        # sequencing them explicitly here keeps the rewrite on migrated input.
        # `strict` is retired. Reject it for fresh (no version), v4-or-later,
        # or malformed payloads; only a pre-v4 *integer* stored version
        # migrates it (v3→v4 maps strict:true→to_many_handling='error'). ``version``
        # is raw here (pre-coercion), so accept only int / integer-string forms —
        # never truncate a float or other malformed value into a stale version.
        if isinstance(data, dict) and "strict" in data:
            raw_version = data.get("version")
            version: int | None = None
            if isinstance(raw_version, int) and not isinstance(raw_version, bool):
                version = raw_version
            elif isinstance(raw_version, str):
                try:
                    version = int(raw_version)
                except ValueError:
                    version = None
            if version is None or version >= _STRICT_RETIRED_AT:
                raise ValueError(
                    "`strict` is retired; set to_many_handling='error' instead "
                    "(one of broadcast|associate|error)."
                )
        data = _migrate_schema(entity="SlayerQuery", data=data)
        if isinstance(data, dict):
            data = _rewrite_functional_granularity(data)
        return data

    @field_validator("name")
    @classmethod
    def _validate_query_name(cls, v: str | None) -> str | None:
        # Same rules as SlayerModel.name: query names share the naming space when
        # persisted as query-backed models (rejects __, ., :).
        if v is None:
            return v
        return _validate_model_name(v, "Query")
    dimensions: DimensionsField = Field(
        default=None,
        description=(
            "Group-by columns — names / dotted paths, or computed expressions "
            '({"expression": ..., "name": ...}); one result row per distinct '
            "value combination."
        ),
    )
    time_dimensions: list[TimeDimension] | None = Field(
        default=None,
        json_schema_extra=_advertise_string_time_dimensions,
        description=(
            "Time-bucketed group-bys — one result row per bucket. Each entry is a "
            "TimeDimension dict or the functional string gran(col), e.g. "
            "\"month(created_at)\"."
        ),
    )
    main_time_dimension: str | None = Field(
        default=None,
        description=(
            "Name of the time dimension that time-ordered transforms (change, lag, ...) "
            "key off; overrides auto-detection when the query has multiple time dimensions."
        ),
    )
    filters: list[str] | None = Field(
        default=None,
        description=(
            "Condition strings, AND-ed; each routes automatically to WHERE / "
            "HAVING / post-aggregation. May contain aggregations, transforms, "
            "and {variable} placeholders."
        ),
    )
    variables: dict[str, Any] | None = Field(
        default=None,
        description=(
            "{placeholder} values scoped to this query object / stage; the "
            "tool-level variables argument overrides."
        ),
    )
    order: OrderField = Field(
        default=None,
        description=(
            "Sort keys; column is a result column name or an "
            "aggregation-bearing expression string."
        ),
    )
    limit: int | None = Field(
        default=None,
        description=(
            "Max rows to return. Use only for top-N / 'the single most X' "
            "requests — never to trim a plain list (an uncapped MCP response "
            "is truncated at 20 rows with an explicit notice)."
        ),
    )
    offset: int | None = Field(default=None, description="Rows to skip.")
    whole_periods_only: bool = Field(
        default=False,
        description=(
            "Snap date filters to whole time buckets and drop the current "
            "incomplete bucket."
        ),
    )
    distinct_dimension_values: bool = Field(
        default=True,
        description=(
            "Default true: dimension-only queries return distinct dimension "
            "combinations (GROUP BY the projected dimensions). Set false for "
            "raw per-record rows — requires empty `measures` and no measure "
            "reference in `filters`/`order`. For rows plus a count, keep the "
            "default and add `count(*)`."
        ),
    )

    # Filters keep EXISTS pushdown in every mode.
    to_many_handling: Literal["broadcast", "associate", "error"] = Field(
        default="broadcast",
        description=(
            "What happens when an aggregation is sliced by a dimension not "
            "attributable to it: broadcast (default — repeat the value across "
            "the cells, with a warning) | associate (aggregate per cell over "
            "the distinct associated entities) | error (refuse)."
        ),
    )

    @model_validator(mode="after")
    def _validate_dsl_user_input(self) -> "SlayerQuery":
        """Enforce DSL-mode rules on user-input strings at construction — raw ``OVER (...)``
        in a filter is caught here; bare-name/raw-SQL-function rejection happen at binding.
        ``__`` is not rejected: virtual-model columns flatten join paths (``kpis__total_amount_sum``)."""
        if self.filters:
            for f in self.filters:
                _validate_query_filter_string(f)
        self._validate_distinct_dimension_values()
        return self

    @model_validator(mode="after")
    def _dedupe_time_dimensions(self) -> "SlayerQuery":
        """Collapse exact-duplicate time dimensions; reject same column+granularity
        differing in date range or label (their result keys would collide). Compares
        on the source-model-prefix-stripped column so ``created_at`` and
        ``orders.created_at`` (the same column) are treated as one."""
        if not self.time_dimensions:
            return self
        model_name = self.source_model_name

        def _canon(td: TimeDimension) -> tuple:
            col = _strip_column_ref(td.dimension, model_name) if model_name else td.dimension
            return (col.full_name, td.granularity, tuple(td.date_range or ()), td.label)

        seen: dict[tuple[str, str], tuple] = {}
        result: list[TimeDimension] = []
        for td in self.time_dimensions:
            canon = _canon(td)
            base = (canon[0], str(canon[1]))
            prior = seen.get(base)
            if prior is None:
                seen[base] = canon
                result.append(td)
            elif prior != canon:
                raise GranularityCallError(
                    f"Conflicting time dimensions on {td.dimension.full_name!r} at "
                    f"{td.granularity} granularity: same column and "
                    f"granularity must not differ in date range or label."
                )
        if len(result) != len(self.time_dimensions):
            self.time_dimensions = result
        return self

    def _validate_distinct_dimension_values(self) -> None:
        """Cheap, model-free rejection for ``distinct_dimension_values=False``: non-empty
        ``measures``, or both ``dimensions``/``time_dimensions`` empty. Deep filter/order
        measure-reference checks happen at binding, where post-substitution text is available."""
        if self.distinct_dimension_values:
            return
        if self.measures:
            n = len(self.measures)
            raise DistinctDimensionValuesError(
                summary=f"distinct_dimension_values=False requires an empty `measures` "
                f"field, but {n} measure(s) were supplied.",
                suggestion="Either remove the measures (and any other measure "
                "references) or set distinct_dimension_values=True (the default) "
                "to keep the auto-aggregating behaviour.",
            )
        if not self.dimensions and not self.time_dimensions:
            raise DistinctDimensionValuesError(
                summary="distinct_dimension_values=False requires at least one of "
                "`dimensions` or `time_dimensions` to be non-empty — there "
                "are no columns to SELECT.",
                suggestion="Add the columns you want to project.",
            )

    @property
    def source_model_name(self) -> str | None:
        """The population's model name before resolution (an extension names its base)."""
        match self.source_model:
            case str() as name:
                return name
            case ModelExtension(source_name=name):
                return name
            case SlayerModel(name=name):
                return name
            case _:
                return None

    def strip_source_model_prefix(self) -> "SlayerQuery":
        """Strip a redundant source-model-name prefix from all dotted references (agents write ``sum(orders.revenue)``)."""
        model_name = self.source_model_name
        if model_name is None:
            return self

        updates: dict[str, Any] = {}
        pattern = re.compile(r"\b" + re.escape(model_name) + r"\.")

        if self.dimensions:
            new_dims = [_strip_column_ref(d, model_name) for d in self.dimensions]
            if any(n is not o for n, o in zip(new_dims, self.dimensions)):
                updates["dimensions"] = new_dims

        stripped_lists = (
            ("time_dimensions", _strip_time_dimensions(self.time_dimensions, model_name)),
            ("order", _strip_order(self.order, model_name, pattern)),
            ("measures", _strip_measures(self.measures, pattern)),
        )
        updates.update((key, value) for key, value in stripped_lists if value is not None)

        if self.filters:
            new_filters = [pattern.sub("", f) for f in self.filters]
            if new_filters != self.filters:
                updates["filters"] = new_filters

        prefix = model_name + "."
        if self.main_time_dimension and self.main_time_dimension.startswith(prefix):
            updates["main_time_dimension"] = self.main_time_dimension[len(prefix):]

        if not updates:
            return self

        # Sanitize for log injection (S5145): strip CR/LF before logging.
        safe_name = model_name.replace("\r", "\\r").replace("\n", "\\n")
        logger.info(
            "Stripped source model prefix '%s.' from query references",
            safe_name,
        )
        return self.model_copy(update=updates)


def _refinement_field(name: str) -> Any:
    """``SlayerQuery``'s field ``name`` with a ``None`` default (supplied-ness is ``model_fields_set``)."""
    info = SlayerQuery.model_fields[name]
    return Field(default=None, description=info.description, json_schema_extra=info.json_schema_extra)


_NON_NULLABLE_SETTINGS = ("whole_periods_only", "distinct_dimension_values", "to_many_handling")


class QueryRefinement(BaseModel):
    """Clauses merged into a saved query's final stage when it runs by name."""

    model_config = ConfigDict(extra="forbid")

    dimensions: DimensionsField = _refinement_field("dimensions")
    time_dimensions: list[TimeDimension] | None = _refinement_field("time_dimensions")
    measures: MeasuresField = _refinement_field("measures")
    filters: list[str] | None = _refinement_field("filters")
    order: OrderField = _refinement_field("order")
    limit: int | None = _refinement_field("limit")
    offset: int | None = _refinement_field("offset")
    main_time_dimension: str | None = _refinement_field("main_time_dimension")
    whole_periods_only: bool | None = _refinement_field("whole_periods_only")
    distinct_dimension_values: bool | None = _refinement_field("distinct_dimension_values")
    to_many_handling: Literal["broadcast", "associate", "error"] | None = _refinement_field("to_many_handling")

    @model_validator(mode="before")
    @classmethod
    def _rewrite_functional(cls, data: Any) -> Any:
        return _rewrite_functional_granularity(data) if isinstance(data, dict) else data

    @model_validator(mode="after")
    def _validate_inputs(self) -> "QueryRefinement":
        for f in self.filters or []:
            _validate_query_filter_string(f)
        for name in _NON_NULLABLE_SETTINGS:
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"refine.{name} cannot be null; omit it to keep the saved value")
        return self


_ENTRY_CONFLICT_SUGGESTION = "Give the refinement entry another name, or repeat the saved entry exactly."
_WINDOW_CONFLICT_SUGGESTION = (
    "Add a filter on the time column instead (filters AND together), or save the window as a "
    "filter with variables, e.g. `ordered_at >= '{start}'`."
)


def _show(value: Any) -> str:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", exclude_none=True)
    return json.dumps(value)


def _canonical_ref(ref: ColumnRef, model_name: str | None) -> ColumnRef:
    return _strip_column_ref(ref, model_name) if model_name else ref


def _union(
    saved: list | None, refined: list | None, *, field: str, canon: Callable[[Any], Any],
    identity: Callable[[Any], tuple[str, str]],
) -> list | None:
    """``saved`` then the ``refined`` entries not present; same ``(kind, key)`` identity must mean equal ``canon``."""
    if not refined:
        return saved
    merged = list(saved or [])
    seen: dict[tuple[str, str], Any] = {}
    for entry in merged:
        seen.setdefault(identity(entry), entry)
    for entry in refined:
        key = identity(entry)
        prior = seen.get(key)
        if prior is None:
            seen[key] = entry
            merged.append(entry)
        elif canon(prior) != canon(entry):
            raise RefinementConflictError(
                field=field, key=key[1], saved=_show(prior), refined=_show(entry),
                suggestion=_ENTRY_CONFLICT_SUGGESTION,
            )
    return merged


def _merge_attribute(*, saved: Any, refined: Any, key: str, attribute: str) -> Any:
    if refined is None or refined == saved:
        return saved
    if saved is None:
        return refined
    raise RefinementConflictError(
        field="time_dimensions", key=key, saved=f"{attribute}={_show(saved)}",
        refined=f"{attribute}={_show(refined)}", suggestion=_WINDOW_CONFLICT_SUGGESTION,
    )


def _merge_time_dimensions(
    saved: list[TimeDimension] | None, refined: list[TimeDimension] | None, model_name: str | None,
) -> list[TimeDimension] | None:
    """Per (column, granularity): append new keys, merge ``date_range`` / ``label`` of existing ones."""
    if not refined:
        return saved
    merged = list(saved or [])
    positions: dict[tuple[str, str], int] = {}
    for i, td in enumerate(merged):
        positions.setdefault((_canonical_ref(td.dimension, model_name).full_name, granularity_key(td.granularity)), i)
    for td in refined:
        slot = (_canonical_ref(td.dimension, model_name).full_name, granularity_key(td.granularity))
        i = positions.get(slot)
        if i is None:
            positions[slot] = len(merged)
            merged.append(td)
            continue
        prior, key = merged[i], f"{slot[0]}@{td.granularity}"
        update = {
            attribute: _merge_attribute(
                saved=getattr(prior, attribute), refined=getattr(td, attribute), key=key, attribute=attribute,
            )
            for attribute in ("date_range", "label")
        }
        merged[i] = prior.model_copy(update=update)
    return merged


def refine_query(*, saved: SlayerQuery, refinement: QueryRefinement) -> SlayerQuery:
    """The saved final stage with ``refinement`` merged in, revalidated as a whole."""
    model_name = saved.source_model_name
    supplied = refinement.model_fields_set
    data: dict[str, Any] = {name: getattr(saved, name) for name in saved.model_fields_set | {"version"}}

    def canonical_dimension(dim: ColumnRef | ComputedDimension) -> Any:
        return dim if isinstance(dim, ComputedDimension) else _canonical_ref(dim, model_name)

    def dimension_identity(dim: ColumnRef | ComputedDimension) -> tuple[str, str]:
        if isinstance(dim, ComputedDimension):
            return "computed", dim.name or dim.expression
        return "column", _canonical_ref(dim, model_name).full_name

    merged_lists = {
        "dimensions": _union(
            saved.dimensions, refinement.dimensions, field="dimensions", canon=canonical_dimension,
            identity=dimension_identity,
        ),
        "measures": _union(
            saved.measures, refinement.measures, field="measures", canon=lambda m: m,
            identity=lambda m: ("name", m.name) if m.name else ("formula", m.formula),
        ),
        "time_dimensions": _merge_time_dimensions(saved.time_dimensions, refinement.time_dimensions, model_name),
        "filters": _union(saved.filters, refinement.filters, field="filters", canon=str, identity=lambda f: ("filter", f)),
    }
    data.update((name, value) for name, value in merged_lists.items() if value is not getattr(saved, name))
    for name in ("limit", "offset", "main_time_dimension", *_NON_NULLABLE_SETTINGS):
        if name in supplied:
            data[name] = getattr(refinement, name)
    if "order" in supplied:
        data["order"] = refinement.order or None
    return SlayerQuery.model_validate(data)
