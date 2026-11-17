"""Query models for SLayer — the user-facing ``SlayerQuery`` and its helpers."""
from __future__ import annotations

import json
import re
from collections.abc import Callable, Container, Iterable
from typing import Annotated, Any, Literal, TypeVar

from pydantic import (
    AliasChoices,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ModelWrapValidatorHandler,
    PrivateAttr,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
)

from slayer.core.deprecation import deprecated_input
from slayer.core.direction import normalize_direction
from slayer.core.enums import DataType, TimeGranularity, _coerce_legacy_datatype
from slayer.core.errors import (
    ColonSpellingError,
    DistinctDimensionValuesError,
    GranularityCallError,
    RefinementConflictError,
)
from slayer.core.granularity import GranularitySpec, granularity_key
from slayer.core.models import (
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
    _validate_dataset_name,
)
from slayer.core.names import quoted_name
from slayer.core.refs import auto_name_from_expression
from slayer.core.strict_flag import strict_flag, without_strict
from slayer.core.time_points import TIME_POINT_FORMS, is_time_point_shape
from slayer.core.warnings import DeprecatedInputWarning
from slayer.core.syntax import (
    AggCall,
    DateTrunc,
    DottedRef,
    Placeholder,
    Ref,
    TransformCall,
    call_source,
    canonical_metric_text,
    dotted_quoting,
    drop_root_qualifier,
    legacy_result_name_seed,
    metric_text_at_path,
    parse_expr,
    parse_template,
    root_relative,
    variable_names,
    walk_parsed,
)
from slayer.sql.window_detect import WINDOW_IN_FILTER_ERROR, has_window_function
from slayer.storage.migrations import (
    CURRENT_VERSIONS,
    hoist_root_extension,
    in_stored_document,
    is_legacy_root_extension,
    migrate as _migrate_schema,
    with_root_extension,
)

_T = TypeVar("_T")

#: The SlayerQuery version whose migration retired ``strict``.
_STRICT_RETIRED_AT = 4


def _stored_before_strict_retired(raw_version: Any) -> bool:
    """A raw ``version`` naming a stored pre-v4 query: an int or integer string below 4; anything else is fresh."""
    if isinstance(raw_version, bool):
        return False
    if isinstance(raw_version, str):
        try:
            raw_version = int(raw_version)
        except ValueError:
            return False
    return isinstance(raw_version, int) and raw_version < _STRICT_RETIRED_AT


def _has_fresh_strict(data: dict[str, Any]) -> bool:
    """``data`` carries a ``strict`` that converts with a deprecation rather than migrating silently."""
    return "strict" in data and not _stored_before_strict_retired(data.get("version"))


def _convert_strict(data: dict[str, Any]) -> tuple[dict[str, Any], DeprecatedInputWarning]:
    """Fresh input's deprecated ``strict`` converted to ``to_many_handling``, with its deprecation."""
    if not strict_flag(data["strict"]):
        new = "the query without `strict`"
    elif "to_many_handling" in data:
        new = "to_many_handling"
    else:
        new = 'to_many_handling: "error"'
    return without_strict(data), deprecated_input("strict", new, removal="3.0")

_NAME_PATTERN = re.compile(r"^[a-zA-Z_]\w*$", re.ASCII)
_VAR_PATTERN = re.compile(r"\{\{|\}\}|\{([a-zA-Z_]\w*)\}|\{([^}]*)\}", re.ASCII)

# Leading callee of a whole-string single call ``name( ... )``; used only when ``parse_expr`` rejects the shape
# (e.g. ``date_trunc('month')``) but the callee still names ``date_trunc``, so the wrong-shape error can fire.
_WHOLE_CALL_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s*\(.*\)\s*$", re.S)


def _reject_call_placeholders(entry: str) -> None:
    """A time-dimension call string whose unit (a ``date_trunc`` unit or the callee) or field is a ``{var}`` is
    refused."""
    call = call_source(entry)
    if call is None or len(call.callee) != 1 or not call.arguments:
        return
    if call.callee[0].lower() == "date_trunc":
        field = call.arguments[1] if len(call.arguments) == 2 else None
        _reject_time_dimension_placeholders(entry, granularity=call.arguments[0], field=field)
    else:
        field = call.arguments[0] if len(call.arguments) == 1 else None
        _reject_time_dimension_placeholders(entry, granularity=call.callee[0], field=field)


def _granularity_names() -> str:
    return ", ".join(g.value for g in TimeGranularity)


def _single_col_of_source(node: AggCall) -> str | None:
    """The dotted field name if ``node``'s source is a lone ref, else ``None``."""
    src = node.source
    if isinstance(src, Ref):
        return src.name
    if isinstance(src, DottedRef):
        return ".".join(src.parts)
    return None


def _parsed_or_none(text: str) -> Any:
    """``text`` parsed, ``None`` when unparseable; a removed colon spelling is refused."""
    try:
        return parse_expr(text)
    except ColonSpellingError:
        raise
    except Exception:  # noqa: BLE001 — the caller reports unparseable text
        return None


def call_callee(entry: str) -> tuple[Any, str | None]:
    """``entry`` parsed as a template (``None`` when unparseable) and its whole-call callee, if any."""
    try:
        node: Any = parse_template(entry)
    except ColonSpellingError:
        raise
    except Exception:  # noqa: BLE001 — the caller reports unparseable text
        node = None
    if isinstance(node, AggCall):
        return node, node.agg
    if node is None:
        m = _WHOLE_CALL_RE.match(entry)
        return None, (m.group(1) if m else None)
    call = call_source(entry)
    return node, (call.callee[0] if call is not None and len(call.callee) == 1 else None)


def functional_call_field(node: Any) -> str | None:
    """The field of a single-field call ``name(col)``, else ``None``."""
    if isinstance(node, AggCall) and not node.args and not node.kwargs:
        return _single_col_of_source(node)
    return None


def granularity_call_argument(node: Any, entry: str) -> str | None:
    """The bucketed expression's text of a ``date_trunc('<unit>', expr)`` string, else ``None``."""
    if not isinstance(node, DateTrunc):
        return None
    call = call_source(entry)
    return call.arguments[1] if call is not None and len(call.arguments) == 2 else None


def time_dimension_from_functional(entry: str) -> dict | None:
    """A ``date_trunc('<unit>', expr)`` string → its ``TimeDimension`` dict; another call or a non-call → ``None``;
    a ``date_trunc`` of any other shape → raise."""
    node, callee = call_callee(entry)
    if callee is None or callee.lower() != "date_trunc":
        return None
    arg = granularity_call_argument(node, entry)
    if arg is None:
        _reject_call_placeholders(entry)
        raise GranularityCallError.wrong_shape(entry)
    _reject_call_placeholders(entry)
    return {"field": arg, "granularity": node.unit}


def granularity_call_parts(entry: str) -> tuple[str, str] | None:
    """``(expr, unit)`` for a well-formed ``date_trunc('<unit>', expr)`` string, else ``None`` (never raises);
    used to resolve an order key against projected time dimensions."""
    node, _ = call_callee(entry)
    arg = granularity_call_argument(node, entry)
    return None if arg is None else (arg, node.unit)


def bucket_call_remedy(entry: str) -> str:
    """The ``date_trunc`` spelling a time-dimension string needs: ``f(expr)`` → ``date_trunc('f', expr)``."""
    call = call_source(entry)
    if call is not None and len(call.callee) == 1 and len(call.arguments) == 1:
        return f"date_trunc('{call.callee[0].lower()}', {call.arguments[0]})"
    return "date_trunc('<unit>', expr)"


def _split_functional_dimensions(dims: "list | tuple") -> tuple[list, list]:
    """Partition ``dimensions`` into (kept, extracted-TD-dicts): a ``date_trunc('<unit>', expr)`` string becomes a
    ``TimeDimension`` dict."""
    kept: list = []
    rewritten: list = []
    for item in dims:
        if isinstance(item, str):
            td = time_dimension_from_functional(item)
            if td is not None:
                rewritten.append(td)
                continue
        kept.append(item)
    return kept, rewritten


def _coerce_time_dimension_entry(entry: Any) -> Any:
    """A string ``time_dimensions`` entry must be ``date_trunc('<unit>', expr)``; dicts/objects pass through."""
    if not isinstance(entry, str):
        return entry
    _reject_call_placeholders(entry)
    td = time_dimension_from_functional(entry)
    if td is not None:
        return td
    raise GranularityCallError(
        f"Time dimension {entry!r} must be a time bucket written {bucket_call_remedy(entry)}; the unit is one of "
        f"{_granularity_names()} or a datasource granularity, and no default granularity is invented."
    )


def _rewrite_functional_granularity(data: dict) -> dict:
    """Move ``date_trunc`` dimension strings into ``time_dimensions`` and coerce string ``time_dimensions``
    entries; both accept only ``date_trunc``."""
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


def _validate_query_filter_string(expression: str) -> None:
    """Reject raw ``OVER (...)`` window syntax in a ``SlayerQuery.filter`` entry."""
    if has_window_function(expression):
        raise ValueError(f"Filter '{expression}' {WINDOW_IN_FILTER_ERROR}")


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


def resolve_optional_blocks(text: str, supplied: Container[str] | None) -> str:
    """``text`` with each ``{? ?}`` block parenthesised when ``supplied`` holds every variable it reads (always, given
    ``None``), else ``(1=1)``; placeholders and escaped braces are kept."""
    if not _contains_block_delimiter(text):
        return text
    out: list[str] = []
    for kind, segment in _split_blocks(text):
        if kind == "text":
            out.append(segment)
        elif supplied is None or all(name in supplied for name in _block_var_names(segment, text)):
            out.append("(" + segment.strip() + ")")
        else:
            out.append("(1=1)")
    return "".join(out)


def mask_placeholders(text: str, mask: Callable[[str], str]) -> str:
    """``text`` with each ``{name}`` replaced by ``mask(name)`` and ``{{`` / ``}}`` read as braces; an invalid name
    raises."""
    def _replace(match: re.Match) -> str:
        if match.group(0) in ("{{", "}}"):
            return match.group(0)[0]
        if match.group(1) is None:
            raise ValueError(
                f"Invalid variable name '{match.group(2)}' in {text!r}. "
                f"Variable names must contain only letters, digits, and underscores."
            )
        return mask(match.group(1))

    return _VAR_PATTERN.sub(_replace, text)


def _mode_b_surfaces(query: "SlayerQuery") -> list[str]:
    """Every Mode-B text of ``query`` that binds ``{var}``: filters, metric expressions,
    computed-dimension and time-dimension expressions and order expressions."""
    return [
        *(query.filter or []),
        *(m.expression for m in query.metrics or []),
        *(d.expression for d in query.dimensions or [] if isinstance(d, ComputedDimension)),
        *(o.raw_expression for o in query.order or [] if o.raw_expression),
        *(td.field for td in query.time_dimensions or []),
    ]


def _bare_variable_names(text: str) -> set[str]:
    """The ``{var}`` names of a non-Mode-B text (a granularity, a ``date_range`` bound)."""
    return {m.group(1) for m in _VAR_PATTERN.finditer(text) if m.group(1)}


def extract_placeholder_names(query: "SlayerQuery") -> set:
    """The variables ``query``'s binding looks up, ``date_range`` bounds included."""
    bounds = (b for td in query.time_dimensions or [] for b in td.date_range or [] if b is not None)
    return {
        *(name for text in _mode_b_surfaces(query) for name in variable_names(text)),
        *(name for bound in bounds for name in _bare_variable_names(bound)),
    }


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


class DatasetVariables(BaseModel):
    """A dataset's ``{var}`` placeholders split into ``required`` (no default, not in a block) and ``optional`` (blocked or defaulted)."""

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
        # doesn't break; execution still raises when it binds.
        parts = [("text", text)]
    for kind, segment in parts:
        target = bare if kind == "text" else blocked
        for match in _VAR_PATTERN.finditer(segment):
            if match.group(0) in ("{{", "}}"):
                continue
            if match.group(1):
                target.add(match.group(1))
    return bare, blocked


def extension_variable_refs(query: "SlayerQuery") -> set[str]:
    """The variables ``query``'s extension metrics read."""
    return {name for e in query.extensions or [] for m in e.metrics or [] for name in variable_names(m.expression)}


def mode_a_texts(dataset: SlayerDataset) -> list[tuple[str, str]]:
    """``(field, text)`` of each of ``dataset``'s Mode-A surfaces."""
    out = [(f"Dataset {dataset.name!r} sql", dataset.sql)] if dataset.sql else []
    out += [(f"Dataset {dataset.name!r} filters[{i}]", f) for i, f in enumerate(dataset.filters or []) if f]
    for dataset_field in dataset.fields:
        out += [(f"Field {dataset_field.name!r} {key}", text)
                for key, text in (("sql", dataset_field.sql), ("filter", dataset_field.filter)) if text]
    return out


def extract_dataset_variables(dataset: SlayerDataset) -> DatasetVariables:
    """Classify a dataset's ``{var}`` placeholders as required / optional across its four
    Mode-A surfaces and its saved metric expressions. A bare, undefaulted occurrence
    anywhere makes the var required (a metric has no optional blocks); everything else is optional."""
    surfaces: list[str] = []
    if dataset.sql:
        surfaces.append(dataset.sql)
    surfaces.extend(f for f in (dataset.filters or []) if f)
    for col in dataset.fields:
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
    for metric in dataset.metrics:
        bare |= set(variable_names(metric.expression))

    defaults = set(dataset.query_variables or {})
    required = {name for name in bare if name not in defaults}
    optional = (bare | blocked) - required
    return DatasetVariables(
        required=sorted(required), optional=sorted(optional)
    )


def declared_variable_specs(dataset: SlayerDataset) -> dict[str, dict]:
    """The dataset's declared Mode-A variable bag (``name -> spec``) from ``meta.cube_variables``,
    or ``{}`` (shape-checked; ``meta`` is user-extensible). An entry counts only with a non-empty
    string ``member``, so a reused ``cube_variables`` key isn't mistaken for generated SQL."""
    declared = (dataset.meta or {}).get("cube_variables")
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


def declares_variables(dataset: SlayerDataset) -> bool:
    """True if the dataset declares its Mode-A variables (importer-generated SQL), disabling the
    brace-literal protection: a declared but unrendered ``{var}`` must raise, not survive as a raw brace."""
    return bool(declared_variable_specs(dataset))


def list_valued_variable_names(dataset: SlayerDataset) -> set[str]:
    """Names of Mode-A variables declared ``list_valued: true`` in ``meta.cube_variables``. A
    machine-generated ``col IN ({var})`` can't quote the author, so a scalar renders ``IN (US)``
    (rejected); the flag opts into list-wrapping. Must be exactly ``True`` (truthiness would flip semantics)."""
    return {
        name
        for name, spec in declared_variable_specs(dataset).items()
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


class FieldRef(BaseModel):
    """A field reference: bare name or dotted relationship path."""
    name: str
    dataset: str | None = None
    label: str | None = None
    quoted: list[bool] | None = Field(
        default=None,
        description="Per path segment, whether it was double-quoted (matched exactly); omitted when none is.",
    )

    @model_validator(mode="after")
    def _parse_dotted_name(self) -> "FieldRef":
        """Split a dotted ``name`` into ``dataset`` prefix + leaf, then validate both."""
        if self.dataset is None and "." in self.name:
            prefix, leaf = self.name.rsplit(".", 1)
            self.dataset = prefix
            self.name = leaf
        if not _NAME_PATTERN.match(self.name):
            raise ValueError(
                f"Invalid name '{self.name}': must contain only letters, "
                f"digits, and underscores, and start with a letter or underscore"
            )
        if self.dataset:
            for part in self.dataset.split("."):
                if not _NAME_PATTERN.match(part):
                    raise ValueError(
                        f"Invalid dataset path '{self.dataset}': each part must contain "
                        f"only letters, digits, and underscores"
                    )
        return self

    @model_validator(mode="after")
    def _check_quoted(self) -> "FieldRef":
        if self.quoted is not None and len(self.quoted) != len(self.parts):
            raise ValueError(f"'quoted' needs one flag per segment of {self.full_name!r}, got {self.quoted!r}")
        if self.quoted is not None and not any(self.quoted):
            self.quoted = None
        return self

    @model_serializer(mode="wrap")
    def _omit_unquoted(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = handler(self)
        if self.quoted is None:
            data.pop("quoted", None)
        return data

    @property
    def full_name(self) -> str:
        if self.dataset:
            return f"{self.dataset}.{self.name}"
        return self.name

    @property
    def quoted_parts(self) -> tuple[bool, ...]:
        return tuple(self.quoted) if self.quoted is not None else (False,) * len(self.parts)

    @property
    def spelling(self) -> str:
        """The reference as Mode-B text, its quoted segments double-quoted."""
        return ".".join(quoted_name(p) if q else p for p, q in zip(self.parts, self.quoted_parts))

    @property
    def parts(self) -> tuple[str, ...]:
        return (*self.dataset.split("."), self.name) if self.dataset else (self.name,)

    @classmethod
    def from_string(cls, s: str) -> FieldRef:
        """Create a FieldRef from a string. Dots are parsed by the validator."""
        return cls(name=s)


def field_ref_of(text: str) -> FieldRef | None:
    """``text`` as a field reference when it is a bare, possibly quoted, identifier or dotted path, else ``None``."""
    if _is_valid_field_ref_name(text):
        return FieldRef.from_string(text)
    parsed = _parsed_or_none(text)
    if not isinstance(parsed, (Ref, DottedRef)):
        return None
    parts = (parsed.name,) if isinstance(parsed, Ref) else parsed.parts
    if not all(_NAME_PATTERN.match(p) for p in parts):
        return None
    return FieldRef(name=parts[-1], dataset=".".join(parts[:-1]) or None, quoted=list(dotted_quoting(parsed)))


class ComputedDimension(BaseModel):
    """A dimension computed from an ``expression``."""

    model_config = ConfigDict(extra="forbid")

    expression: str
    name: str | None = None
    # Pre-substitution expression; explicitness is judged against it.
    _template: str | None = PrivateAttr(default=None)

    @property
    def template(self) -> str:
        """The expression as written, before ``{var}`` substitution."""
        return self._template or self.expression

    @model_validator(mode="after")
    def _fill_name(self) -> "ComputedDimension":
        if self.name is None:
            self.name = computed_dimension_name(self.expression)
        _validate_dataset_name(self.name, "Computed dimension")
        return self


def computed_dimension_name(expression: str) -> str:
    """An unnamed computed dimension's name: its expression's legacy naming seed, sanitised, so every
    spelling of one expression names it alike."""
    try:
        seed = legacy_result_name_seed(parse_expr(expression))
    except Exception:  # noqa: BLE001 — an unparseable expression is reported at binding
        seed = expression
    return auto_name_from_expression(seed)


def _time_dimension_name_identity(td: "TimeDimension") -> tuple[str, tuple] | None:
    """A named or expression time dimension's name and what it may repeat for; ``None`` for an unnamed field."""
    if td.name is None and td.field_ref is not None:
        return None
    text = _canonical_expression_text(td.template)
    if td.name is not None:
        return td.name, ("named", text, str(td.granularity))
    return computed_dimension_name(td.template), ("auto", text)


def _canonical_expression_text(expression: str) -> str:
    try:
        return canonical_metric_text(parse_expr(expression))
    except Exception:  # noqa: BLE001 — an unparseable expression is reported at binding
        return expression


def _coerce_field_ref(v: Any) -> Any:
    """Allow plain string where a FieldRef is expected: "x" → {"name": "x"}."""
    if isinstance(v, str):
        return {"name": v}
    return v


# Sentinel ``FieldRef.name`` values: this ORDER BY item is an expression to
# resolve from ``raw_expression``, not a field reference. Consumers must also
# require a non-empty ``raw_expression`` before treating a name as a sentinel, so a
# real field of that name still resolves normally.
_FUNCSTYLE_PENDING = "_funcstyle_pending"
_EXPR_PENDING = "_expr_pending"
ORDER_PLACEHOLDER_NAMES = frozenset({_FUNCSTYLE_PENDING, _EXPR_PENDING})


def _order_expression_candidate(v: str) -> str | None:
    """``v`` verbatim if it is an expression rather than a field reference, else ``None``;
    shared by ``_capture_raw_expression`` and ``_coerce_order_field`` so they can't drift.
    Binding judges whether the expression is legal there."""
    if field_ref_of(v) is not None:
        return None
    return None if _parsed_or_none(v) is None else v


def _is_valid_field_ref_name(name: str) -> bool:
    """Whether ``name`` parses as a ``FieldRef`` (bare leaf or dotted path)."""
    try:
        FieldRef.model_validate({"name": name})
    except Exception:
        return False
    return True


def _coerce_order_field(v: Any) -> Any:
    """Coerce an ORDER BY field. Call-style entries (``sum(revenue)``, ``my_agg(price)``) emit a placeholder
    whose ``raw_expression`` the planner binds; other non-field expressions carry ``raw_expression`` under
    ``_EXPR_PENDING``."""
    if not isinstance(v, str):
        return v
    if _order_expression_candidate(v) is None:
        return field_ref_of(v) or {"name": v}
    call = call_source(v)
    if call is not None:
        return {"name": _FUNCSTYLE_PENDING if len(call.callee) == 1 else _EXPR_PENDING}
    return {"name": _EXPR_PENDING}


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


def _reject_time_dimension_placeholders(entry: Any, *, granularity: Any = None, field: Any = None) -> None:
    """Variables supply literals: a ``{var}`` naming a time dimension's granularity, or standing for its
    whole dimension, is refused; one inside an expression is a literal."""
    for what, value, names_it in (
        ("granularity", granularity, lambda text: bool(_bare_variable_names(text))),
        ("field", field, lambda text: isinstance(_parsed_or_none(text), Placeholder)),
    ):
        if isinstance(value, str) and names_it(value):
            raise ValueError(
                f"Time dimension {entry!r}: variables supply literal values; they can't name a {what} ({value!r})."
            )


def _legacy_field_ref_text(value: Any) -> Any:
    """The dotted text of a legacy field-reference object (``{"name", "dataset"}`` or a ``FieldRef``)."""
    if isinstance(value, FieldRef):
        return value.spelling
    if isinstance(value, dict) and isinstance(value.get("name"), str):
        return FieldRef.model_validate({k: v for k, v in value.items() if k in ("name", "dataset", "quoted")}).spelling
    return value


def _coerce_time_dimension_text(value: Any) -> Any:
    """A time dimension's field as Mode-B text; it must parse as an expression."""
    value = _legacy_field_ref_text(value)
    if isinstance(value, str):
        try:
            parse_template(value)
        except Exception as exc:
            raise ValueError(f"Time dimension {value!r} is not a row-level expression: {exc}") from exc
    return value


class TimeDimension(BaseModel):
    """A time-bucketed group-by: ``field`` truncated to ``granularity``."""
    field: Annotated[str, BeforeValidator(_coerce_time_dimension_text)] = Field(
        validation_alias=AliasChoices("field", "column"),
    )
    name: str | None = None
    granularity: GranularitySpec = Field(
        description="A built-in granularity, or a custom granularity defined on the query's datasource.",
    )
    date_range: Annotated[list[str | None] | None, BeforeValidator(_coerce_date_range)] = Field(
        default=None, json_schema_extra=_advertise_string_date_range,
        description=(
            'The buckets to return: one period ("2025", "2025-Q1", "last month") or [lower, upper] '
            'with either bound null (one-sided); a bound may be a {var} placeholder, written unquoted ("{start}"). '
            "change, change_pct, time_shift and window= "
            "aggregates read rows before the range (a forward time_shift, after it) by themselves, "
            "so each month of 2025 with its "
            'change vs the previous month is just "2025": never widen the range and drop rows '
            "afterwards. cumsum starts at the range start."
        ),
    )
    label: str | None = None
    # Pre-substitution dimension; an unnamed expression is named from it.
    _template: str | None = PrivateAttr(default=None)

    @property
    def template(self) -> str:
        """The dimension as written, before ``{var}`` substitution."""
        return self._template or self.field

    @model_validator(mode="before")
    @classmethod
    def _reject_placeholder_names(cls, data: Any) -> Any:
        if isinstance(data, dict):
            field = data.get("field", data.get("column"))
            _reject_time_dimension_placeholders(data, granularity=data.get("granularity"), field=field)
            if isinstance(field, dict) and field.get("label") is not None and data.get("label") is None:
                data = {**data, "label": field["label"]}
        return data

    @property
    def field_ref(self) -> FieldRef | None:
        """The dimension as a field reference when it is one, else ``None``."""
        return field_ref_of(self.field)

    @property
    def effective_name(self) -> str | None:
        """``name``, else an expression's sanitised template; ``None`` for an unnamed field."""
        if self.name is not None or self.field_ref is not None:
            return self.name
        return computed_dimension_name(self.template)

    @model_validator(mode="after")
    def _check_date_range(self) -> "TimeDimension":
        if self.name is not None:
            _validate_dataset_name(self.name, "Time dimension")
        if self.date_range is not None and (problem := _date_range_problem(self.date_range)):
            raise ValueError(
                f"time dimension {self.field!r}: date_range {self.date_range!r} {problem}."
            )
        return self


def _advertise_string_time_dimensions(schema: dict[str, Any]) -> None:
    """Add the ``date_trunc('<unit>', expr)`` string alternative to each ``time_dimensions``
    item in the derived JSON/MCP input schema, without widening the field's Python
    type (strings are coerced by the model before-validator). A callable
    ``json_schema_extra`` works on the pydantic >=2.0 floor."""
    for option in schema.get("anyOf", [schema]):
        if option.get("type") == "array" and "items" in option:
            option["items"] = {"anyOf": [option["items"], {"type": "string"}]}


def _advertise_order_item(schema: dict[str, Any]) -> None:
    """Publish ``field``'s string form; hide the internal ``raw_expression``."""
    props = schema.get("properties", {})
    props.pop("raw_expression", None)
    props.pop("implied", None)
    if "field" in props:
        props["field"] = {"anyOf": [{"type": "string"}, props["field"]]}


def _order_field_text(col: Any) -> str | None:
    """The string form of an order ``field``: the string itself, or the expression a ``{"name": …}``-only object carries."""
    if isinstance(col, str):
        return col
    if isinstance(col, dict) and set(col) == {"name"} and isinstance(name := col["name"], str):
        if not _is_valid_field_ref_name(name) and _order_expression_candidate(name) is not None:
            return name
    return None


class OrderItem(BaseModel):
    """A sort key."""
    # extra="forbid": reject stray keys so a mixed canonical+shorthand item
    # raises instead of silently dropping the extra key.
    model_config = ConfigDict(extra="forbid", json_schema_extra=_advertise_order_item)

    field: Annotated[FieldRef, BeforeValidator(_coerce_order_field)]
    direction: str = "asc"
    nulls: Literal["first", "last"] | None = None
    raw_expression: str | None = Field(
        default=None,
        description="Internal — captured automatically from expression strings; do not set.",
    )
    implied: bool = Field(
        default=False, exclude=True,
        description="Internal — set on the default order SLayer adds; do not set.",
    )

    @model_validator(mode="before")
    @classmethod
    def _capture_raw_expression(cls, data: Any) -> Any:
        """Capture the raw field expression before coercion normalizes it."""
        if isinstance(data, dict):
            col = _order_field_text(data.get("field"))
            if col is not None:
                candidate = _order_expression_candidate(col)
                if candidate is not None:
                    data = {**data, "field": col, "raw_expression": candidate}
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


def _coerce_metrics(v: Any) -> Any:
    """Allow plain strings in the metrics list: "count" → {"expression": "count"}."""
    if v is None:
        return v
    if not isinstance(v, (list, tuple)):
        raise TypeError(f"'metrics' must be a list, got {type(v).__name__}")
    return [{"expression": item} if isinstance(item, str) else item for item in v]


def _coerce_dimension_item(item: Any) -> Any:
    """Coerce one ``dimensions`` entry: a bare identifier / dotted path stays a ``FieldRef``, any other string parses as a Mode-B expression → ``ComputedDimension`` (neither raises)."""
    if isinstance(item, ComputedDimension):
        return _reference_or_computed(item)
    if isinstance(item, FieldRef):
        return item
    if isinstance(item, dict):
        if "expression" in item:
            return _reference_or_computed(ComputedDimension(**item))
        return item
    if isinstance(item, str):
        if (ref := field_ref_of(item)) is not None:
            return ref
        try:
            parse_expr(item)
        except Exception as exc:
            raise ValueError(
                f"Dimension {item!r} is neither a field reference (a bare "
                f"identifier or dotted relationship path) nor a parseable Mode-B "
                f"expression: {exc}"
            ) from exc
        return ComputedDimension(expression=item)
    return item


def _reference_or_computed(dimension: ComputedDimension) -> FieldRef | ComputedDimension:
    """An unnamed computed dimension that is just a reference is that field reference."""
    if dimension.name != computed_dimension_name(dimension.template):
        return dimension
    return field_ref_of(dimension.expression) or dimension


def _coerce_dimensions(v: Any) -> Any:
    """Allow plain strings / expression dicts in the dimensions list."""
    if v is None:
        return v
    if not isinstance(v, (list, tuple)):
        raise TypeError(f"'dimensions' must be a list, got {type(v).__name__}")
    return [_coerce_dimension_item(item) for item in v]


def _process_order_item(item: Any) -> list:
    """Heal a single ``order`` entry into the canonical items it expands to; a shorthand dict
    field → direction (no ``field``/``direction`` key, all-direction values) expands one item per key."""
    if not isinstance(item, dict):
        return [item]
    # A dict with a reserved key is canonical-intended, never shorthand.
    if "field" in item or "direction" in item:
        return [item]
    if (
        item
        and all(isinstance(k, str) for k in item)
        and all(_is_direction(val) for val in item.values())
    ):
        return [{"field": k, "direction": val} for k, val in item.items()]
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
MetricsField = Annotated[list[DatasetMetric] | None, BeforeValidator(_coerce_metrics)]
DimensionsField = Annotated[list[FieldRef | ComputedDimension] | None, BeforeValidator(_coerce_dimensions)]
OrderField = Annotated[list[OrderItem] | None, BeforeValidator(_coerce_order)]


_UNCAST = (DataType.TEXT, DataType.UNKNOWN)


def extension_field_text(field: dict, *, owner: str, what: str) -> str:
    """A removed extension field as one Mode-B row expression relative to ``owner``: its name when it has no ``sql``,
    else ``sql`` wrapped as ``iif(<filter>, <sql>, NULL)`` and cast to a declared type other than TEXT / UNKNOWN.
    Raises ``ValueError`` naming ``what`` when nothing preserves its meaning."""
    data_type = DataType(_coerce_legacy_datatype(field.get("type") or DataType.TEXT.value) or DataType.TEXT.value)
    if data_type is DataType.ARRAY:
        raise ValueError(f"{what}, a list field")
    sql, row_filter = field.get("sql"), field.get("filter")
    if sql is None:
        if row_filter is not None:
            raise ValueError(f"{what}, a filtered field without sql")
        return str(field["name"])
    text = canonical_metric_text(_row_expression(sql, what=what))
    if row_filter is not None:
        text = f"iif({canonical_metric_text(_row_expression(row_filter, what=f'{what} filter'))}, {text}, NULL)"
    if data_type not in _UNCAST:
        text = f"CAST({text} AS {data_type.value})"
    return canonical_metric_text(root_relative(parse_expr(text), owner))


def _row_expression(text: Any, *, what: str) -> Any:
    if not isinstance(text, str):
        raise ValueError(f"{what} without text")
    try:
        parsed = parse_expr(text)
    except ValueError as exc:
        raise ValueError(f"{what} {text!r}, which is not valid Mode B ({exc})") from exc
    if any(isinstance(node, (AggCall, TransformCall)) for node in walk_parsed(parsed)):
        raise ValueError(f"{what} {text!r}, which is not a Mode-B row expression")
    return parsed


def _field_expression(field: Any, *, dataset: Any) -> str | None:
    """A removed extension field as the expression written where it is used; ``None`` when nothing preserves it."""
    if not isinstance(field, dict) or not isinstance(dataset, str) or not isinstance(field.get("name"), str):
        return None
    try:
        text = extension_field_text(field, owner=dataset, what=f"extension field {field['name']}")
        return metric_text_at_path(text, path=(dataset,))
    except ValueError:
        return None


def _extension_fields_removed(data: dict) -> str:
    columns = data.get("columns")
    written = [
        f"{column['name']} → {expression}" for column in columns if isinstance(columns, list)
        for expression in [_field_expression(column, dataset=data.get("source_name"))]
        if expression is not None and isinstance(column.get("name"), str)
    ] if isinstance(columns, list) else []
    example = f" ({'; '.join(written)})" if written else ""
    return (
        "Extension columns were removed: write a per-query field as an expression in the position that uses "
        f"it — a computed dimension, a filter, an aggregation's argument{example}."
    )


class DatasetExtension(BaseModel):
    """Query-scoped metrics and relationships added to one dataset a query object reads."""

    model_config = ConfigDict(extra="forbid")

    source_name: str                                # Dataset/stage to extend
    metrics: list[DatasetMetric] | None = None
    relationships: list[DatasetRelationship] | None = None

    @model_validator(mode="before")
    @classmethod
    def _refuse_fields(cls, data: Any) -> Any:
        if isinstance(data, dict) and "columns" in data:
            raise ValueError(_extension_fields_removed(data))
        return data





def convert_legacy_root_extension(data: dict) -> tuple[dict, DeprecatedInputWarning | None]:
    """Fresh input's legacy root extension, moved into ``extensions``, with its deprecation (none in a stored document)."""
    source = data.get("population")
    if isinstance(source, DatasetExtension):
        data = with_root_extension(data, extension=source, base=source.source_name, key="population")
    elif "version" not in data and is_legacy_root_extension(source):
        data = hoist_root_extension(data, key="population")
    else:
        return data, None
    if in_stored_document():
        return data, None
    return data, deprecated_input("An extension object in `population`", "`extensions`", removal="3.0")


def prepare_query_input(data: dict) -> tuple[dict, list[DeprecatedInputWarning]]:
    """Fresh query input with its deprecated forms converted (and a versioned payload migrated), with every
    conversion's deprecation."""
    payloads: list[DeprecatedInputWarning] = []
    if _has_fresh_strict(data):
        data, payload = _convert_strict(data)
        payloads.append(payload)
    data, payload = convert_legacy_root_extension(data)
    if payload is not None:
        payloads.append(payload)
    if "version" in data:
        data = _migrate_schema(entity="SlayerQuery", data=data)
    return data, payloads


def _refuse_inline_dataset(value: Any) -> Any:
    if isinstance(value, (SlayerDataset, dict)):
        name = value.name if isinstance(value, SlayerDataset) else value.get("name")
        named = f" {name!r}" if isinstance(name, str) else ""
        raise ValueError(
            f"`population` cannot be an inline dataset{named}: it names a stored dataset or stage. Write a "
            f"query-backed dataset's queries as named stages of the query list, and put query-local metrics and relationships "
            f"in `extensions`."
        )
    return value


# ``SlayerQuery.population``: a stored dataset or stage name.
SourceSpec = Annotated[str, BeforeValidator(_refuse_inline_dataset)]


def extension_dataset_names(query: "SlayerQuery") -> list[str]:
    """The datasets ``query``'s extensions name: each extended dataset, then its relationships' targets."""
    names = [n for e in query.extensions or [] for n in (e.source_name, *(j.to for j in e.relationships or []))]
    return list(dict.fromkeys(names))


def check_unique_extensions(extensions: list[DatasetExtension] | None) -> None:
    """Reject a second extension of one dataset."""
    seen: set[str] = set()
    for extension in extensions or ():
        if extension.source_name in seen:
            raise ValueError(
                f"Dataset {extension.source_name!r} is extended twice; merge its metrics and relationships into one "
                f"`extensions` entry."
            )
        seen.add(extension.source_name)


def _root_relative_ref(ref: FieldRef, root: str | None) -> FieldRef:
    """``ref`` with a leading root qualifier dropped."""
    kept = drop_root_qualifier(ref.parts, root, quoted=ref.quoted_parts)
    if kept == ref.parts:
        return ref
    return FieldRef(
        name=kept[-1], dataset=".".join(kept[:-1]) or None, label=ref.label, quoted=list(ref.quoted_parts[-len(kept):]),
    )


def _parsed_identity(text: str, root: str | None) -> str:
    """``text``'s root-relative parsed meaning, as canonical text; the text itself when unparseable."""
    node = _parsed_or_none(text)
    return text if node is None else canonical_metric_text(root_relative(node, root))


class SlayerQuery(BaseModel):
    """User-facing query object — what to retrieve from a dataset, as names/references, no SQL."""

    model_config = ConfigDict(extra="forbid")

    version: int = CURRENT_VERSIONS["SlayerQuery"]
    name: str | None = Field(
        default=None,
        description=(
            "Stage name. In a list of query objects every entry but the last carries a name, and "
            "later stages use it as their population. A STAGE IS NOT NEEDED for top N per group "
            "(filter on `rank(...)` in the same stage), for averaging or bucketing per-entity "
            "totals (a nested aggregate, or a computed dimension with `partition_by`), or for "
            "shares and totals (`partition_by`). A filter on a transform's value "
            "(`rank(...) <= 3`, `cumsum(...) > 100`) applies after it, which is computed over "
            "every row the other filters keep. Stages ARE "
            "needed only to drop rows by a dimension or time condition that a cross-row value "
            "must still see, or to join or reuse a whole result: a row filter or date_range "
            "applies before a running total, lag / lead, rank, ntile, consecutive_periods or "
            "`partition_by` total (change, change_pct, time_shift and window= still read past "
            "date_range or a filter on their own time dimension, but not past other filters). "
            "Compute the value "
            "in an inner stage and filter in the outer one. An outer stage sees the inner "
            "result's columns with dotted paths flattened to __: an inner dimension "
            "customers.regions.name is customers__regions__name outside; a metric keeps its "
            "name. More: "
            'inspect(reference=["memory:help.queries"], entity_type="memory").'
        ),
    )
    population: SourceSpec | None = Field(
        default=None,
        description=(
            "The population: the rows the result lists. When the question asks for every X (each "
            "region, zero included), root at X so rows without facts appear: rooted at regions, "
            "the metrics count(customers.orders.id) and "
            "{\"expression\": \"sum(iif(customers.orders.order_date >= '2025-01-01', 1, 0))\", "
            '"name": "orders_2025"} list every region, including those without orders (a count '
            "over no rows is 0, any other aggregate NULL). Always name a conditional aggregate. "
            "Otherwise root where the facts are (rows without facts then drop out), or omit to "
            "infer the smallest dataset determining every dimension and row-level condition field "
            "(reported in the response). A saved dataset or stage name. "
            "Write a per-query field as an expression in the position that uses it (a "
            "dimension, a filter, an aggregation's argument)."
        ),
    )
    extensions: list[DatasetExtension] | None = Field(
        default=None,
        description=(
            "Query-scoped metrics and relationships added to any dataset this query object reads "
            "(its population, a joined dataset, or a stage), at most one entry per dataset, visible "
            "only in this query object. A relationship repeating a stored relationship, in either "
            "direction, is that edge, and its name is a local alias. An `inner` relationship keeps only "
            "the rows with a match; it never multiplies rows. Rooted at orders: "
            '[{"source_name": "customers", "metrics": [{"name": "credit_total", "expression": '
            '"sum(credit)"}], "relationships": [{"to": "vip_list", "join_pairs": '
            '[["id", "customer_id"]], "join_type": "inner"}]}] adds customers.credit_total and '
            "keeps only the orders of customers on the vip list."
        ),
    )
    metrics: MetricsField = Field(
        default=None,
        description=(
            'Values to return: expression strings or {"expression": ..., "name": ...} (name sets the '
            "result key; a bare name references a saved dataset metric). Expressions combine "
            "aggregations with arithmetic, also across datasets: rooted at customers, "
            "sum(orders.amount) / count(*) divides the orders' total by the number of customers.\n"
            "- Aggregations: count(*), sum(amount), count_distinct(customer_id), "
            "avg(amount - cost), percentile(amount, p=0.9); a joined dataset's field by dotted path.\n"
            "- agg(x) FILTER (WHERE c) aggregates only the rows where c holds: "
            "count(*) FILTER (WHERE status = 'paid').\n"
            "- partition_by= computes an aggregate at a coarser grain, repeated on every row; "
            "spell its keys exactly like the query's dimensions (a joined field by its dotted "
            "path; its bare name fails). Share of group: "
            "sum(amount) / sum(amount, partition_by=customers.regions.name); share of the grand "
            "total: sum(amount) / sum(amount, partition_by=[]).\n"
            "- window= turns any aggregation into a trailing time window ending at each time "
            "bucket, in compact durations only ('90d', '3m', '1y'; never '3 months'): "
            "count_distinct(customer_id, window='90d').\n"
            "- Nesting aggregates per-group values: "
            "avg(sum(amount, partition_by=[customers.regions.name, customers.id]), "
            "partition_by=[customers.regions.name]) averages the per-customer totals in each region.\n"
            "- Transforms wrap an aggregate; all but the rank family need a time dimension: "
            "cumsum(sum(amount)) running total; change(sum(amount)) / change_pct(sum(amount)) "
            "period-over-period delta / ratio; time_shift(sum(amount), -1) the previous "
            "bucket's value ('year' as a third argument for year-over-year); lag(x, n) / "
            "lead(x, n) n rows back / ahead; rank(x, direction='desc') / "
            "dense_rank(x, direction='asc') with direction required and partition_by= to rank "
            "within groups; ntile(x, n=4) buckets, lowest first; "
            "consecutive_periods(sum(amount) > 0) trailing run length.\n"
            "- A joined dataset's aggregate such as sum(customers.credit) from orders is computed "
            "over that dataset's own rows, each customer once. Sliced by a dimension it cannot "
            "be attributed to (an order's status), each group aggregates the distinct customers "
            "reaching it, with an associated warning (groups may overlap); "
            "to_many_handling='broadcast' repeats the total instead. Rooted at customers, "
            "sum(orders.amount) and sum(returns.amount) in one query are each computed over their "
            "own rows: several child datasets never fan out or double count. A child row without a "
            "parent (an order with no customer) has NULL parent fields: it counts in the NULL "
            "row of a parent dimension when that row exists, and in an ungrouped total; never add "
            "it again. An aggregate over a dataset no relationship path reaches from the root is computed "
            "over that dataset's own rows and repeated on every row (per bucket of a time_spine "
            "time dimension), with an unrelated_dataset warning: rooted at customers, "
            "count(events.id) / count(*).\n"
            "More (every aggregation, empty-input values, nesting and transform rules): "
            'inspect(reference=["memory:help.aggregations", "memory:help.transforms"], '
            'entity_type="memory").'
        ),
    )

    _deprecated_inputs: tuple[DeprecatedInputWarning, ...] = PrivateAttr(default=())

    @model_validator(mode="wrap")
    @classmethod
    def _migrate_and_rewrite(cls, data: Any, handler: ModelWrapValidatorHandler["SlayerQuery"]) -> "SlayerQuery":
        # One validator sequences convert → migrate → functional-granularity rewrite.
        payloads: list[DeprecatedInputWarning] = []
        if isinstance(data, dict):
            data, payloads = prepare_query_input(data)
        data = _migrate_schema(entity="SlayerQuery", data=data)
        if isinstance(data, dict):
            if is_legacy_root_extension(data.get("population")):
                raise ValueError(
                    "`population` names a dataset; put a query-scoped extension in `extensions`."
                )
            data = _rewrite_functional_granularity(data)
        query = handler(data)
        if payloads:
            query._deprecated_inputs = tuple(payloads)
        return query

    @property
    def deprecated_inputs(self) -> tuple[DeprecatedInputWarning, ...]:
        """Deprecations of the input forms converted while building this query."""
        return self._deprecated_inputs

    @field_validator("name")
    @classmethod
    def _validate_query_name(cls, v: str | None) -> str | None:
        # Same rules as SlayerDataset.name: query names share the naming space when
        # persisted as query-backed datasets (rejects __, ., :).
        if v is None:
            return v
        return _validate_dataset_name(v, "Query")
    dimensions: DimensionsField = Field(
        default=None,
        description=(
            "Group-by fields, one result row per distinct combination: names, dotted paths to "
            'joined fields (customers.regions.name), or computed {"expression": ..., "name": '
            "...}. date_trunc('<unit>', expr) buckets a timestamp like a time_dimensions entry: "
            "date_trunc('month', created_at); units are second, minute, hour, day, week, week_sunday, "
            "month, quarter, year, or the datasource's custom ones, while month(created_at) is the "
            "month of year (an integer). An aggregate inside a "
            "computed dimension needs partition_by= to fix its grain: "
            "{\"expression\": \"iif(sum(amount, partition_by=customers.id) > 150, 'high', "
            "'low')\", \"name\": \"spend_band\"}. Group by naturally readable labels: a name "
            "(customers.regions.name), never by an id alone unless ids are asked for; when a name "
            "may repeat (people, products), by the id and the name."
        ),
    )
    time_dimensions: list[TimeDimension] | None = Field(
        default=None,
        json_schema_extra=_advertise_string_time_dimensions,
        description=(
            "Time-bucketed group-bys — one result row per bucket. Each entry is a "
            "TimeDimension dict or the string date_trunc('<unit>', expr), e.g. "
            "\"date_trunc('month', created_at)\"; the field may be any DATE/TIMESTAMP expression, with an "
            "optional name. Buckets without rows are absent. To return every bucket, "
            "empty ones included, bucket time_spine.timestamp with a lower date_range bound and "
            "measure datasets by dotted path: "
            '{"field": "time_spine.timestamp", "granularity": "month", "date_range": "2025"} '
            "with sum(orders.amount) returns all twelve months. It also lines several datasets up "
            "on one time axis."
        ),
    )
    main_time_dimension: str | None = Field(
        default=None,
        description=(
            "Name (or field) of the time dimension that time-ordered transforms (change, lag, ...) "
            "key off; overrides auto-detection when the query has multiple time dimensions."
        ),
    )
    filter: list[str] | None = Field(
        default=None,
        description=(
            "Conditions, AND-ed; each routes automatically to row filtering (WHERE), aggregate "
            "filtering (HAVING) or filtering after transforms. SQL operators: = <> < <= > >=, in, "
            "between, like / ilike, is null / is not null, IS [NOT] TRUE / FALSE (NULL counts as "
            "not true), IS DISTINCT FROM / IS NOT DISTINCT FROM (NULL-safe =), and / or / not, "
            "CAST(x AS DATE), regex x ~ '^A[0-9]{3}', "
            "JSON payload ->> 'key'; strings are single-quoted ('paid'); build them from the "
            "field's sampled values.\n"
            "- Aggregates and transforms filter too, by expression or by a metric's name: "
            "sum(amount) > 100; top N per group: "
            "rank(sum(amount), partition_by=customers.regions.name, direction='desc') <= 1.\n"
            "- Anti-join: rooted at customers, orders.id is null keeps the customers without "
            "orders.\n"
            "- A list tags: contains(tags, 'x') or tags.value = 'x'; contains_any(tags, ['x', "
            "'y']) shares an element, contains_all(tags, ['x', 'y']) holds every one.\n"
            "- Time points: a string compared with a date/time field is a half-open period: "
            "'2025', '2025-Q1', '2025-03', '2025-W05' (ISO week), or "
            "relative: 'today', 'last month', 'last 7 days' (excludes the current day), "
            "'year to date'. order_date >= '2025-Q1' starts at the period, <= ends with it, "
            "order_date in '2025-Q1' is inside it. A date '2025-03-01' is its midnight; "
            "created_at in '2025-03-01' is the whole day.\n"
            "- {variable} placeholders take literal values from variables.\n"
            'More: inspect(reference=["memory:help.time", "memory:help.relationships"], '
            'entity_type="memory").'
        ),
    )
    # The effective variables a whole-``IN`` placeholder left unwritten by binding reads (``parse_bound_expr``).
    _bound_variables: dict[str, Any] = PrivateAttr(default_factory=dict)

    @property
    def bound_variables(self) -> dict[str, Any]:
        return self._bound_variables

    # (field path, op, date, literal on the left) per comparison a date-only variable binds → the variables binding it (``apply_variables_to_query``).
    _date_variables: dict[tuple[tuple[str, ...], str, str, bool], tuple[str, ...]] = PrivateAttr(default_factory=dict)

    @property
    def date_variables(self) -> dict[tuple[tuple[str, ...], str, str, bool], tuple[str, ...]]:
        return self._date_variables

    variables: dict[str, Any] | None = Field(
        default=None,
        description=(
            "{placeholder} values scoped to this query object / stage; the "
            "tool-level variables argument overrides. A placeholder binds a literal value only: "
            "its text inside quotes ('{s}'), a number, numeric string or boolean in a bare slot, "
            "a list as the whole right-hand side of IN (status in ({statuses}); over a list field, one "
            "list: tags in ({wanted})) or as a list literal anywhere else (tags = {wanted}, a "
            "list-function argument). A placeholder in a name position is an error."
        ),
    )
    order: OrderField = Field(
        default=None,
        description=(
            'Sort keys, [{"field": ..., "direction": "asc" | "desc", "nulls": "first" | "last"}] '
            "(nulls optional). field is a result column name or any aggregate or transform "
            "expression, displayed or not: with "
            'metrics ["count(*)"], [{"field": "sum(amount)", "direction": "desc"}] sorts by '
            "an amount it does not return. Add limit for the top N. Return only the columns the "
            "question asks for: never add the sort value to metrics just to sort by it."
        ),
    )
    limit: int | None = Field(
        default=None,
        description=(
            "Max rows to return: the top N, or a limit covering a list longer than "
            "the 20 rows an uncapped MCP response returns (with a notice). Never "
            "use it to cut a list asked for in full."
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
            "raw per-record rows — requires empty `metrics` and no metric "
            "reference in `filter`/`order`. For rows plus a count, keep the "
            "default and add `count(*)`."
        ),
    )

    # Filters keep EXISTS pushdown in every mode.
    to_many_handling: Literal["broadcast", "associate", "error"] | None = Field(
        default=None,
        description=(
            "What happens when an aggregation is sliced by a dimension not "
            "attributable to it. Omitted (default): aggregate per cell over the "
            "distinct associated rows, e.g. per list element tags.value, falling "
            "back to broadcast with a warning where association is unsupported "
            "(window=, first/last) | associate (the same, but refuse what it cannot "
            "associate) | broadcast (repeat the value across the cells, with a "
            "warning) | error (refuse)."
        ),
    )

    @model_validator(mode="after")
    def _validate_dsl_user_input(self) -> "SlayerQuery":
        """Enforce DSL-mode rules on user-input strings at construction — raw ``OVER (...)``
        in a filter is caught here; bare-name/raw-SQL-function rejection happen at binding.
        ``__`` is not rejected: virtual-dataset fields flatten relationship paths (``kpis__total_amount_sum``)."""
        if self.filter:
            for f in self.filter:
                _validate_query_filter_string(f)
        if self.main_time_dimension is not None:
            _parsed_or_none(self.main_time_dimension)
        self._validate_distinct_dimension_values()
        check_unique_extensions(self.extensions)
        return self

    @model_validator(mode="after")
    def _dedupe_time_dimensions(self) -> "SlayerQuery":
        """Collapse exact-duplicate time dimensions; reject same dimension+granularity differing
        in date range, name or label. Dimensions compare by root-relative parsed meaning, so
        ``created_at`` and ``orders.created_at`` are one."""
        if not (tds := self.time_dimensions):
            return self
        root = self.population_name

        def _canon(td: TimeDimension) -> tuple:
            return (_parsed_identity(td.template, root), td.granularity, tuple(td.date_range or ()), td.name, td.label)

        seen: dict[tuple[str, str], tuple] = {}
        result: list[TimeDimension] = []
        for td in tds:
            canon = _canon(td)
            base = (canon[0], str(canon[1]))
            prior = seen.get(base)
            if prior is None:
                seen[base] = canon
                result.append(td)
            elif prior != canon:
                raise GranularityCallError(
                    f"Conflicting time dimensions on {td.field!r} at "
                    f"{td.granularity} granularity: same dimension and "
                    f"granularity must not differ in date range, name or label."
                )
        if len(result) != len(tds):
            self.time_dimensions = result
        self._check_unique_dimension_names()
        return self

    def _check_unique_dimension_names(self) -> None:
        """Time-dimension names, an unnamed expression's included, are unique across dimensions and
        time dimensions; a name repeats only for its own expression — an auto-name at any granularity
        (suffixes tell them apart), an explicit one at the same granularity — which binding dedupes or
        rejects."""
        taken: set[str] = set()
        for dim in self.dimensions or ():
            if (name := dim.name if isinstance(dim, ComputedDimension) else dim.full_name) is not None:
                taken.add(name)
        identities: dict[str, tuple] = {}
        for td in self.time_dimensions or ():
            if (named := _time_dimension_name_identity(td)) is None:
                continue
            name, identity = named
            if name in taken or identities.setdefault(name, identity) != identity:
                raise ValueError(
                    f"Dimension name {name!r} is used twice; give the time dimension {td.field!r} a "
                    f"distinct `name`."
                )

    def _validate_distinct_dimension_values(self) -> None:
        """Cheap, dataset-free rejection for ``distinct_dimension_values=False``: non-empty
        ``metrics``, or both ``dimensions``/``time_dimensions`` empty. Deep filter/order
        metric-reference checks happen at binding, where post-substitution text is available."""
        if self.distinct_dimension_values:
            return
        if self.metrics:
            n = len(self.metrics)
            raise DistinctDimensionValuesError(
                summary=f"distinct_dimension_values=False requires an empty `metrics` "
                f"field, but {n} metric(s) were supplied.",
                suggestion="Either remove the metrics (and any other metric "
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
    def population_name(self) -> str | None:
        """The population's dataset name before resolution."""
        return self.population


def _refinement_field(name: str) -> Any:
    """``SlayerQuery``'s field ``name`` with a ``None`` default (supplied-ness is ``model_fields_set``)."""
    info = SlayerQuery.model_fields[name]
    return Field(
        default=None, description=f"Same syntax as the query object's {name}.",
        json_schema_extra=info.json_schema_extra,
    )


_NON_NULLABLE_SETTINGS = ("whole_periods_only", "distinct_dimension_values")


class QueryRefinement(BaseModel):
    """Clauses merged into a saved query's final stage when it runs by name."""

    model_config = ConfigDict(extra="forbid")

    dimensions: DimensionsField = _refinement_field("dimensions")
    time_dimensions: list[TimeDimension] | None = _refinement_field("time_dimensions")
    metrics: MetricsField = _refinement_field("metrics")
    filter: list[str] | None = _refinement_field("filter")
    order: OrderField = _refinement_field("order")
    limit: int | None = _refinement_field("limit")
    offset: int | None = _refinement_field("offset")
    main_time_dimension: str | None = _refinement_field("main_time_dimension")
    whole_periods_only: bool | None = _refinement_field("whole_periods_only")
    distinct_dimension_values: bool | None = _refinement_field("distinct_dimension_values")
    to_many_handling: Literal["broadcast", "associate", "error"] | None = _refinement_field("to_many_handling")

    _deprecated_inputs: tuple[DeprecatedInputWarning, ...] = PrivateAttr(default=())

    @model_validator(mode="wrap")
    @classmethod
    def _convert_and_rewrite(
        cls, data: Any, handler: ModelWrapValidatorHandler["QueryRefinement"],
    ) -> "QueryRefinement":
        payload = None
        if isinstance(data, dict):
            if "strict" in data:
                data, payload = _convert_strict(data)
            data = _rewrite_functional_granularity(data)
        refinement = handler(data)
        if payload is not None:
            refinement._deprecated_inputs = (payload,)
        return refinement

    @property
    def deprecated_inputs(self) -> tuple[DeprecatedInputWarning, ...]:
        """Deprecations of the input forms converted while building this refinement."""
        return self._deprecated_inputs

    @model_validator(mode="after")
    def _validate_inputs(self) -> "QueryRefinement":
        for f in self.filter or []:
            _validate_query_filter_string(f)
        for name in _NON_NULLABLE_SETTINGS:
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"refine.{name} cannot be null; omit it to keep the saved value")
        return self


def deprecated_inputs_of(sources: Iterable[SlayerQuery | QueryRefinement | None]) -> list[DeprecatedInputWarning]:
    """The deprecations of the input forms ``sources`` were converted from, each payload once."""
    seen: set[int] = set()
    out: list[DeprecatedInputWarning] = []
    for source in sources:
        for payload in source.deprecated_inputs if source is not None else ():
            if id(payload) not in seen:
                seen.add(id(payload))
                out.append(payload)
    return out


_ENTRY_CONFLICT_SUGGESTION ="Give the refinement entry another name, or repeat the saved entry exactly."
_WINDOW_CONFLICT_SUGGESTION = (
    "Add a filter on the time field instead (filters AND together), or save the window as a "
    "filter with variables, e.g. `ordered_at >= '{start}'`."
)


def _show(value: Any) -> str:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", exclude_none=True)
    return json.dumps(value)


def _union(
    saved: list[_T] | None, refined: list[_T] | None, *, field: str, canon: Callable[[_T], Any],
    identity: Callable[[_T], tuple[str, str]],
) -> list[_T] | None:
    """``saved`` then the ``refined`` entries not present; same ``(kind, key)`` identity must mean equal ``canon``."""
    if not refined:
        return saved
    merged = list(saved or [])
    seen: dict[tuple[str, str], _T] = {}
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


_RESTATABLE_FIELDS = ("label", "type", "description", "meta")


def _restated_name(expression: str, root: str | None) -> str | None:
    """The saved-metric name ``expression`` restates: a bare name, or that name qualified by the root."""
    node = _parsed_or_none(expression)
    node = root_relative(node, root) if node is not None else None
    return node.name if isinstance(node, Ref) else None


def _check_restatement(entry: DatasetMetric, prior: DatasetMetric) -> None:
    for field in _RESTATABLE_FIELDS:
        if field in entry.model_fields_set and getattr(entry, field) != getattr(prior, field):
            raise RefinementConflictError(
                field="metrics", key=prior.name or "", saved=_show(prior), refined=_show(entry),
                suggestion=_ENTRY_CONFLICT_SUGGESTION,
            )


def _drop_restatements(
    saved: list[DatasetMetric] | None, refined: list[DatasetMetric] | None, root: str | None,
) -> list[DatasetMetric] | None:
    """``refined`` without the entries restating a saved entry by name; a differing set field conflicts."""
    by_name = {m.name: m for m in saved or [] if m.name}
    kept: list[DatasetMetric] = []
    for entry in refined or []:
        name = _restated_name(entry.expression, root)
        prior = by_name.get(name) if name is not None else None
        if prior is None or entry.name not in (None, prior.name):
            kept.append(entry)
        else:
            _check_restatement(entry, prior)
    return kept if refined else refined


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
    saved: list[TimeDimension] | None, refined: list[TimeDimension] | None, root: str | None,
) -> list[TimeDimension] | None:
    """Per (dimension, granularity): append new keys, merge ``date_range`` / ``name`` / ``label`` of existing ones."""
    if not refined:
        return saved
    merged = list(saved or [])
    positions: dict[tuple[str, str], int] = {}
    for i, td in enumerate(merged):
        positions.setdefault((_parsed_identity(td.field, root), granularity_key(td.granularity)), i)
    for td in refined:
        slot = (_parsed_identity(td.field, root), granularity_key(td.granularity))
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
            for attribute in ("date_range", "name", "label")
        }
        merged[i] = prior.model_copy(update=update)
    return merged


def refine_query(*, saved: SlayerQuery, refinement: QueryRefinement) -> SlayerQuery:
    """The saved final stage with ``refinement`` merged in, revalidated as a whole."""
    root = saved.population_name
    supplied = refinement.model_fields_set
    data: dict[str, Any] = {name: getattr(saved, name) for name in saved.model_fields_set | {"version"}}

    def canonical_dimension(dim: FieldRef | ComputedDimension) -> Any:
        return dim if isinstance(dim, ComputedDimension) else _root_relative_ref(dim, root)

    def dimension_identity(dim: FieldRef | ComputedDimension) -> tuple[str, str]:
        if isinstance(dim, ComputedDimension):
            return "computed", dim.name or dim.expression
        return "column", _root_relative_ref(dim, root).full_name

    def canonical_metric(m: DatasetMetric) -> DatasetMetric:
        return m.model_copy(update={"expression": _parsed_identity(m.expression, root)})

    merged_lists = {
        "dimensions": _union(
            saved.dimensions, refinement.dimensions, field="dimensions", canon=canonical_dimension,
            identity=dimension_identity,
        ),
        "metrics": _union(
            saved.metrics, _drop_restatements(saved.metrics, refinement.metrics, root), field="metrics",
            canon=canonical_metric,
            identity=lambda m: ("name", m.name) if m.name else ("expression", _parsed_identity(m.expression, root)),
        ),
        "time_dimensions": _merge_time_dimensions(saved.time_dimensions, refinement.time_dimensions, root),
        "filter": _union(saved.filter, refinement.filter, field="filter", canon=str, identity=lambda f: ("filter", f)),
    }
    data.update((name, value) for name, value in merged_lists.items() if value is not getattr(saved, name))
    for name in ("limit", "offset", "main_time_dimension", *_NON_NULLABLE_SETTINGS):
        if name in supplied:
            data[name] = getattr(refinement, name)
    if refinement.to_many_handling is not None:
        data["to_many_handling"] = refinement.to_many_handling
    if "order" in supplied:
        data["order"] = refinement.order or None
    return SlayerQuery.model_validate(data)
