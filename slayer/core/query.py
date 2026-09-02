"""Query models for SLayer — the user-facing ``SlayerQuery`` and its helpers."""
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
    """Escape a string value for the target layer (DEV-1727): ``"sql"`` is dialect-aware via
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
    """Render a ``list``/``tuple`` into an ``IN``-list body (DEV-1730): template writes
    the parens (``col IN ({var})``), string elements auto-quoted. ``escape="python"``
    appends a trailing comma to force 1-tuple parsing; empty list raises (``IN ()`` invalid)."""
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
    filter_str: str, variables: dict, escape: str, backslash_escapes: bool
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
                f"Optional blocks '{{? ... ?}}' are not supported in Mode-B "
                f"(python) filters: {filter_str!r}."
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


def extract_placeholder_names(query: "SlayerQuery") -> set:
    """Valid ``{var}`` names referenced in ``query.filters``."""
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
    """Render a Mode-A surface for a syntax-only sqlglot parse (blocks → ``(1=1)``, ``{var}`` → ``0``); shared so import-time parsing matches execution."""
    out: list[str] = []
    for kind, segment in _split_blocks(text):
        if kind == "block":
            out.append("(1=1)")
        else:
            out.append(_VAR_PATTERN.sub(_probe_replace, segment))
    return "".join(out)


class ModelVariables(BaseModel):
    """A model's Mode-A ``{var}`` placeholders split into ``required`` (no default, not in a block) and ``optional`` (blocked or defaulted)."""

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
    """Classify a model's Mode-A ``{var}`` placeholders as required / optional across
    the four surfaces (model/column sql + filters). A bare, undefaulted occurrence
    anywhere makes the var required; everything else is optional."""
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
    """A dimension reference; dotted paths (``customers.regions.name``) split at validation into ``model`` + leaf ``name``."""
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


def _auto_name_from_expression(expression: str) -> str:
    """Deterministic identifier for an unnamed computed dimension; long names fold to ``<head>_<hash8>_<tail>`` to avoid prefix collisions."""
    base = re.sub(r"\W+", "_", expression.strip()).strip("_").lower() or "expr"
    if base[0].isdigit():
        base = f"e_{base}"
    if len(base) > 48:
        digest = hashlib.sha256(expression.encode("utf-8")).hexdigest()[:8]
        base = f"{base[:28]}_{digest}_{base[-8:]}"
    # No ``__`` — reserved for join-path aliases in generated SQL.
    return re.sub(r"_+", "_", base).strip("_")


class ComputedDimension(BaseModel):
    """A dimension defined by a Mode-B ``expression`` (grouped by and projected); an aggregate inside must carry ``partition_by=`` (aggregate-then-regroup path)."""

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


# Sentinel ``ColumnRef.name`` values: this ORDER BY item is an expression to
# resolve from ``raw_formula``, not a column reference. Consumers must also
# require a non-empty ``raw_formula`` before treating a name as a sentinel, so a
# real column of that name still resolves normally.
_FUNCSTYLE_PENDING = "_funcstyle_pending"
_EXPR_PENDING = "_expr_pending"
ORDER_PLACEHOLDER_NAMES = frozenset({_FUNCSTYLE_PENDING, _EXPR_PENDING})


def _order_formula_candidate(v: str) -> str | None:
    """Func-style-rewritten form of ``v`` if it carries a measure expression, else ``None``; shared by ``_capture_raw_formula`` and ``_coerce_order_column`` so they can't drift."""
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
    """Coerce an ORDER BY column, normalizing aggregation to the underscore form (``revenue:sum``
    → ``revenue_sum``). A formula that doesn't canonicalise to a column ref emits a placeholder whose
    ``raw_formula`` the planner binds; arithmetic over aliases (``rev / cnt``) falls through, failing fast."""
    if isinstance(v, str):
        candidate = _order_formula_candidate(v)
        rewritten = (
            candidate if candidate is not None
            else _rewrite_funcstyle_aggregations(v)
        )
        if _FUNCSTYLE_CALL_PATTERN.match(rewritten):
            # Unrewritten function-style call (custom aggregation): enrichment
            # re-parses raw_formula and overwrites column.name, so a placeholder is fine.
            return {"name": _FUNCSTYLE_PENDING}
        if ":" in rewritten:
            base, agg = rewritten.rsplit(":", 1)
            agg_name = agg.split("(", 1)[0]
            if base == "*":
                rewritten = f"_{agg_name}"
            else:
                rewritten = f"{base}_{agg_name}"
        if candidate is not None and not _is_valid_column_ref_name(rewritten):
            # Formula that doesn't canonicalise to a column ref; raw_formula carries it.
            return {"name": _EXPR_PENDING}
        return {"name": rewritten}
    return v


# ORDER BY direction synonyms → canonical lowercase (the generator compares
# ``direction == "asc"``). Shared by the shorthand healer and the validator.
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
    # extra="forbid": reject stray keys so a mixed canonical+shorthand item
    # raises instead of silently dropping the extra key.
    model_config = ConfigDict(extra="forbid")

    column: Annotated[ColumnRef, BeforeValidator(_coerce_order_column)]
    direction: str = "asc"
    raw_formula: str | None = None

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


class ModelExtension(BaseModel):
    """Extend a model inline on a query with extra columns, measures, or joins, without modifying the stored model."""
    source_name: str                                # Model/query to extend
    columns: list | None = None                  # Extra Column objects
    measures: list[ModelMeasure] | None = None   # Extra ModelMeasure formulas
    joins: list | None = None                    # Extra ModelJoin objects


def _get_source_model_name(source_model: object) -> str | None:
    """Model name from any ``source_model`` type, before model resolution."""
    if isinstance(source_model, str):
        return source_model
    if isinstance(source_model, dict):
        return source_model.get("source_name") or source_model.get("name")
    source_name = getattr(source_model, "source_name", None)
    if isinstance(source_name, str):
        return source_name
    name = getattr(source_model, "name", None)
    if isinstance(name, str):
        return name
    return None


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


class SlayerQuery(BaseModel):
    """User-facing query object — what to retrieve from a model, as names/references, no SQL."""

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
        # Same rules as SlayerModel.name: query names share the naming space when
        # persisted as query-backed models (rejects __, ., :).
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
    # Default True: auto-dedup dim-only queries (Cube.js-style) when measures is
    # empty. False emits a flat projection and rejects any measure reference.
    distinct_dimension_values: bool = True

    # Default False (broadcast + warn). True turns silent-semantics events — an
    # implicit-grain broadcast, a dropped-as-unreachable filter — into hard errors.
    # Explicit partition_by= broadcasting is by design and never errors.
    strict: bool = False

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

    def _validate_distinct_dimension_values(self) -> None:
        """Cheap, model-free rejection for ``distinct_dimension_values=False``: non-empty
        ``measures``, or both ``dimensions``/``time_dimensions`` empty. Deep filter/order
        measure-reference checks happen at binding, where post-substitution text is available."""
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
        """When ``whole_periods_only``, add a filter per time dimension excluding the current incomplete period."""
        if not self.whole_periods_only or not self.time_dimensions:
            return self

        filters = list(self.filters or [])
        for td in self.time_dimensions:
            gran = td.granularity
            dim_name = td.dimension.name

            has_filter = any(dim_name in f for f in filters)
            if not has_filter:
                today = datetime.date.today()
                prev_end = gran.period_end(gran.period_start(today) - datetime.timedelta(days=1))
                filters.append(f"{dim_name} <= '{prev_end.isoformat()}'")

        return self.model_copy(update={"filters": filters, "whole_periods_only": False})

    def strip_source_model_prefix(self) -> "SlayerQuery":
        """Strip a redundant source-model-name prefix from all dotted references (agents write ``orders.revenue:sum``)."""
        model_name = _get_source_model_name(self.source_model)
        if model_name is None:
            return self

        updates: dict[str, Any] = {}
        pattern = re.compile(r"\b" + re.escape(model_name) + r"\.")

        if self.dimensions:
            new_dims = [_strip_column_ref(d, model_name) for d in self.dimensions]
            if any(n is not o for n, o in zip(new_dims, self.dimensions)):
                updates["dimensions"] = new_dims

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
