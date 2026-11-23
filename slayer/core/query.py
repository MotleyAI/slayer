"""Query models for SLayer — the user-facing ``SlayerQuery`` and its helpers."""
from __future__ import annotations

import functools
import json
import re
from collections.abc import Callable, Container, Generator, Iterable
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal, TypeVar

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ModelWrapValidatorHandler,
    PrivateAttr,
    SerializerFunctionWrapHandler,
    ValidationInfo,
    field_validator,
    model_serializer,
    model_validator,
)
from typing_extensions import deprecated

from slayer.core.assignment import AtomicAssignment
from slayer.core.deprecation import deprecated_input
from slayer.core.direction import normalize_direction
from slayer.core import vocabulary
from slayer.core.enums import DataType, TimeGranularity, TypeKind, stored_datatype
from slayer.core.errors import (
    ColonSpellingError,
    DistinctDimensionValuesError,
    FieldTextError,
    GranularityCallError,
    RefinementConflictError,
    UnknownKeyError,
)
from slayer.core.granularity import GranularitySpec, granularity_key
from slayer.core.models import (
    DatasetMetric,
    DatasetRelationship,
    MetricBase,
    SlayerDataset,
    _validate_dataset_name,
    check_metric_name,
    check_metric_text,
)
from slayer.core.names import NAME_PATTERN, match_name, quoted_name
from slayer.core.refs import auto_name_from_expression
from slayer.core.strict_flag import strict_flag, without_strict
from slayer.core.time_points import TIME_POINT_FORMS, is_time_point_shape
from slayer.core.vocabulary import (
    FIELD_REF_KEYS,
    ORDER_ITEM_KEYS,
    QUERY_KEYS,
    REFINEMENT_KEYS,
    Spellings,
    attribute_used,
    converted_document,
    converted_input,
    current_spellings,
    name_twins,
    renamed_input,
    same_ignoring_deprecations,
)
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


BROADCAST = "broadcast"
_broadcast_warned: ContextVar[bool] = ContextVar("slayer_broadcast_warned", default=False)


def _broadcast_deprecation(*, emit: bool) -> DeprecatedInputWarning:
    """The deprecated ``"broadcast"`` mode's deprecation; its ``FutureWarning`` only when ``emit``."""
    return deprecated_input(
        'to_many_handling: "broadcast"', "an explicit `partition_by=` on each aggregate", removal="3.0", emit=emit,
    )


def warn_fresh_broadcast(mode: str | None) -> None:
    """Fresh input's ``"broadcast"`` mode: one ``FutureWarning``, none in a stored document or once already warned."""
    if mode == BROADCAST and not in_stored_document() and not _broadcast_warned.get():
        _broadcast_deprecation(emit=True)


@contextmanager
def broadcast_already_warned() -> Generator[None]:
    """Input rebuilt within the block was already warned about ``"broadcast"``."""
    token = _broadcast_warned.set(True)
    try:
        yield
    finally:
        _broadcast_warned.reset(token)


_MODES = (BROADCAST, "associate", "error")


def _check_mode(value: Any) -> Any:
    if value is not None and value not in _MODES:
        raise ValueError(f"to_many_handling must be 'associate' or 'error', got {value!r}")
    return value


def _offered_modes(schema: dict[str, Any]) -> None:
    """The published enum leaves out the deprecated ``"broadcast"``."""
    for option in schema.get("anyOf", [schema]):
        if "enum" in option:
            option["enum"] = [m for m in option["enum"] if m != BROADCAST]


ManyHandling = Annotated[Literal["broadcast", "associate", "error"] | None, BeforeValidator(_check_mode)]


def broadcast_deprecations(stages: Iterable[SlayerQuery]) -> list[DeprecatedInputWarning]:
    """One deprecation per stage whose mode is ``"broadcast"``, as a run or save reports it."""
    return [_broadcast_deprecation(emit=False) for stage in stages if stage.to_many_handling == BROADCAST]


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
        *query.filter_conditions,
        *(m.metric for m in query.metrics or []),
        *(d.field for d in query.dimensions or []),
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
    return {name for e in query.extensions or [] for m in e.metrics or [] for name in variable_names(m.text)}


def mode_a_texts(dataset: SlayerDataset) -> list[tuple[str, str]]:
    """``(field, text)`` of each of ``dataset``'s Mode-A surfaces."""
    out = [(f"Dataset {dataset.name!r} sql", dataset.sql)] if dataset.sql else []
    out += [(f"Dataset {dataset.name!r} filters[{i}]", f) for i, f in enumerate(dataset.filters or []) if f]
    for dataset_field in dataset.fields:
        out += [(f"Field {dataset_field.name!r} expression", text) for text in dataset_field.native_texts if text]
        out += [(f"Field {dataset_field.name!r} filter", dataset_field.filter)] if dataset_field.filter else []
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
        if col.definition_sql:
            surfaces.append(col.definition_sql)
        if col.filter:
            surfaces.append(col.filter)

    bare: set[str] = set()
    blocked: set[str] = set()
    for surface in surfaces:
        s_bare, s_blocked = extract_variable_refs(surface)
        bare |= s_bare
        blocked |= s_blocked
    for metric in dataset.metrics:
        bare |= set(variable_names(metric.text))

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

    @model_validator(mode="wrap")
    @classmethod
    def _old_spellings(cls, data: Any, handler: ModelWrapValidatorHandler["FieldRef"]) -> "FieldRef":
        return renamed_input(data, handler, FIELD_REF_KEYS, stored=in_stored_document())

    @model_validator(mode="after")
    def _parse_dotted_name(self) -> "FieldRef":
        """Split a dotted ``name`` into ``dataset`` prefix + leaf, then validate both."""
        if self.dataset is None and "." in self.name:
            prefix, leaf = self.name.rsplit(".", 1)
            self.dataset = prefix
            self.name = leaf
        if not NAME_PATTERN.match(self.name):
            raise ValueError(
                f"Invalid name '{self.name}': must contain only letters, "
                f"digits, and underscores, and start with a letter or underscore"
            )
        if self.dataset:
            for part in self.dataset.split("."):
                if not NAME_PATTERN.match(part):
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

    @property
    @deprecated("model is deprecated; use dataset", category=None)
    def model(self) -> str | None:
        attribute_used(self, old="model", new="dataset")
        return self.dataset

    @model.setter
    @deprecated("model is deprecated; use dataset", category=None)
    def model(self, value: str | None) -> None:
        attribute_used(self, old="model", new="dataset")
        self.dataset = value


def field_ref_of(text: str) -> FieldRef | None:
    """``text`` as a field reference when it is a bare, possibly quoted, identifier or dotted path, else ``None``."""
    if _is_valid_field_ref_name(text):
        return FieldRef.from_string(text)
    parsed = _parsed_or_none(text)
    if not isinstance(parsed, (Ref, DottedRef)):
        return None
    parts = (parsed.name,) if isinstance(parsed, Ref) else parsed.parts
    if not all(NAME_PATTERN.match(p) for p in parts):
        return None
    return FieldRef(name=parts[-1], dataset=".".join(parts[:-1]) or None, quoted=list(dotted_quoting(parsed)))


@functools.cache
def _advertised_keys(model: type[BaseModel]) -> str:
    """The keys ``model``'s input schema advertises, as a backticked list."""
    return ", ".join(f"`{k}`" for k in model.model_json_schema(mode="validation").get("properties", {}))


class QueryInput(AtomicAssignment):
    """A query input object: a key it does not read is refused; an assignment is validated, and a failed one leaves
    the object unchanged."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    #: How the refusal names the object.
    _WHAT: ClassVar[str]
    #: A dedicated refusal text per known foreign key.
    _HINTS: ClassVar[dict[str, str]] = {}

    @model_validator(mode="before")
    @classmethod
    def _refuse_unknown_keys(cls, data: Any) -> Any:
        """Runs after every subclass conversion, on what remains."""
        if isinstance(data, dict):
            unknown = [k for k in data if k not in cls.model_fields]
            if unknown:
                keys = cls._HINTS.get(unknown[0]) or f"{cls._WHAT} takes {_advertised_keys(cls)}"
                raise UnknownKeyError(key=unknown[0], path=unknown[0], keys=keys)
        return data


def _without_none(data: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in data.items() if v is not None}


@vocabulary.input_object("dimension")
class QueryDimension(QueryInput):
    """A group-by: a field reference or a row-level expression, with an optional result ``alias`` and ``label``."""

    _WHAT: ClassVar[str] = "a query dimension entry"

    field: str = Field(description="A field reference (a name or dotted relationship path) or a row-level expression.")
    alias: str | None = Field(default=None, description="The result key; defaults to the field's.")
    label: str | None = None
    # Pre-substitution text; an unaliased expression is named from it.
    _template: str | None = PrivateAttr(default=None)

    @model_validator(mode="wrap")
    @classmethod
    def _entry_input(cls, data: Any, handler: ModelWrapValidatorHandler["QueryDimension"]) -> "QueryDimension":
        return converted_input(data, handler, vocabulary.dimension_entry, stored=in_stored_document())

    @field_validator("alias")
    @classmethod
    def _check_alias(cls, v: str | None) -> str | None:
        return v if v is None else _validate_dataset_name(v, "Computed dimension")

    @model_serializer(mode="wrap")
    def _bare_text(self, handler: SerializerFunctionWrapHandler) -> Any:
        if self.alias is None and self.label is None:
            return self.field
        return _without_none(handler(self))

    @property
    def template(self) -> str:
        """The field as written, before ``{var}`` substitution."""
        return self._template or self.field

    @property
    def ref(self) -> FieldRef | None:
        """The field as a reference when it is one, else ``None``."""
        return field_ref_of(self.field)

    @property
    def is_computed(self) -> bool:
        """Whether the entry projects as a computed dimension: an expression, or a reference under another name."""
        return self.ref is None or (self.alias is not None and self.alias != computed_dimension_name(self.template))

    @property
    def name(self) -> str:
        """The alias, else the reference's leaf, else the expression's automatic name."""
        if self.alias is not None:
            return self.alias
        ref = self.ref
        return ref.name if ref is not None else computed_dimension_name(self.template)


def computed_dimension_name(expression: str) -> str:
    """An unnamed computed dimension's name: its expression's legacy naming seed read from its references' common
    leading segments, sanitised, so every spelling of one expression names it alike."""
    try:
        parsed = parse_expr(expression)
        for head in () if isinstance(parsed, (Ref, DottedRef)) else _common_head(parsed):
            parsed = root_relative(parsed, head)
        seed = legacy_result_name_seed(parsed)
    except Exception:  # noqa: BLE001 — an unparseable expression is reported at binding
        seed = expression
    return auto_name_from_expression(seed)


def _common_head(parsed: Any) -> tuple[str, ...]:
    """The leading segments every reference in ``parsed`` shares before its leaf; empty with any bare reference."""
    paths = [n.parts[:-1] if isinstance(n, DottedRef) else () for n in walk_parsed(parsed) if isinstance(n, (Ref, DottedRef))]
    if not paths:
        return ()
    head = paths[0]
    for path in paths[1:]:
        k = 0
        while k < min(len(head), len(path)) and head[k] == path[k]:
            k += 1
        head = head[:k]
    return tuple(head)


def _time_dimension_name_identity(td: "TimeDimension") -> tuple[str, tuple] | None:
    """A named or expression time dimension's name and what it may repeat for; ``None`` for an unnamed field."""
    if td.alias is None and td.field_ref is not None:
        return None
    text = _canonical_expression_text(td.template)
    if td.alias is not None:
        return td.alias, ("named", text, str(td.granularity))
    return computed_dimension_name(td.template), ("auto", text)


def _canonical_expression_text(expression: str) -> str:
    try:
        return canonical_metric_text(parse_expr(expression))
    except Exception:  # noqa: BLE001 — an unparseable expression is reported at binding
        return expression


def _order_expression_candidate(v: str) -> str | None:
    """``v`` verbatim if it is an expression rather than a field reference, else ``None``; binding judges whether the
    expression is legal there."""
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


def _order_text(v: Any) -> Any:
    """An order item's ``field`` is text; an object is refused."""
    if isinstance(v, (dict, BaseModel)):
        raise FieldTextError(where="order item", value=v)
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


def _coerce_time_dimension_text(value: Any) -> Any:
    """A time dimension's field as Mode-B text; it must parse as an expression (an object is refused)."""
    if isinstance(value, (dict, BaseModel)):
        raise FieldTextError(where="time dimension", value=value)
    if isinstance(value, str):
        try:
            parse_template(value)
        except Exception as exc:
            raise ValueError(f"Time dimension {value!r} is not a row-level expression: {exc}") from exc
    return value


@vocabulary.input_object("time_dimension")
class TimeDimension(QueryInput):
    """A time-bucketed group-by: ``field`` truncated to ``granularity``."""

    _WHAT: ClassVar[str] = "a time dimension"
    _HINTS: ClassVar[dict[str, str]] = {
        "name": "a time dimension names its result `alias`",
        "compareDateRange": "SLayer compares periods with the `change`, `change_pct` and `time_shift` metrics, "
                            "not with a second date range",
    }

    field: Annotated[str, BeforeValidator(_coerce_time_dimension_text)]
    alias: str | None = Field(default=None, description="The result key; defaults to the field's.")
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
    # Pre-substitution dimension; an unaliased expression is named from it.
    _template: str | None = PrivateAttr(default=None)

    @property
    def template(self) -> str:
        """The dimension as written, before ``{var}`` substitution."""
        return self._template or self.field

    @model_validator(mode="wrap")
    @classmethod
    def _old_spellings(cls, data: Any, handler: ModelWrapValidatorHandler["TimeDimension"]) -> "TimeDimension":
        return converted_input(data, handler, vocabulary.time_dimension, stored=in_stored_document())

    @model_validator(mode="before")
    @classmethod
    def _reject_placeholder_names(cls, data: Any) -> Any:
        if isinstance(data, dict):
            _reject_time_dimension_placeholders(data, granularity=data.get("granularity"), field=data.get("field"))
        return data

    @property
    def field_ref(self) -> FieldRef | None:
        """The dimension as a field reference when it is one, else ``None``."""
        return field_ref_of(self.field)

    @property
    def effective_name(self) -> str | None:
        """``alias``, else an expression's sanitised template; ``None`` for an unaliased field."""
        if self.alias is not None or self.field_ref is not None:
            return self.alias
        return computed_dimension_name(self.template)

    @model_validator(mode="after")
    def _check_date_range(self) -> "TimeDimension":
        if self.alias is not None:
            _validate_dataset_name(self.alias, "Time dimension")
        if self.date_range is not None and (problem := _date_range_problem(self.date_range)):
            raise ValueError(
                f"time dimension {self.field!r}: date_range {self.date_range!r} {problem}."
            )
        return self

    @property
    @deprecated("dimension is deprecated; use field", category=None)
    def dimension(self) -> str:
        attribute_used(self, old="dimension", new="field")
        return self.field

    @dimension.setter
    @deprecated("dimension is deprecated; use field", category=None)
    def dimension(self, value: str) -> None:
        attribute_used(self, old="dimension", new="field")
        self.field = value

    @property
    @deprecated("column_ref is deprecated; use field_ref", category=None)
    def column_ref(self) -> FieldRef | None:
        attribute_used(self, old="column_ref", new="field_ref")
        return self.field_ref


def _advertise_string_time_dimensions(schema: dict[str, Any]) -> None:
    """Add the ``date_trunc('<unit>', expr)`` string alternative to each ``time_dimensions``
    item in the derived JSON/MCP input schema, without widening the field's Python
    type (strings are coerced by the model before-validator). A callable
    ``json_schema_extra`` works on the pydantic >=2.0 floor."""
    for option in schema.get("anyOf", [schema]):
        if option.get("type") == "array" and "items" in option:
            option["items"] = {"anyOf": [option["items"], {"type": "string"}]}


def _advertise_order_item(schema: dict[str, Any]) -> None:
    """Hide the internal ``raw_expression`` and ``implied``."""
    props = schema.get("properties", {})
    props.pop("raw_expression", None)
    props.pop("implied", None)


@vocabulary.input_object("order_item")
class OrderItem(QueryInput):
    """A sort key."""

    model_config = ConfigDict(json_schema_extra=_advertise_order_item)
    _WHAT: ClassVar[str] = "an order entry"

    field: Annotated[str, BeforeValidator(_order_text)] = Field(
        description="A result column name, a field reference, or an aggregate or transform expression.",
    )
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

    @model_validator(mode="wrap")
    @classmethod
    def _old_spellings(cls, data: Any, handler: ModelWrapValidatorHandler["OrderItem"]) -> "OrderItem":
        return converted_input(data, handler, vocabulary.order_item, stored=in_stored_document())

    @model_validator(mode="before")
    @classmethod
    def _capture_raw_expression(cls, data: Any) -> Any:
        """An expression ``field`` is also the item's ``raw_expression``."""
        if isinstance(data, dict) and isinstance(field := data.get("field"), str) and data.get("raw_expression") is None:
            candidate = _order_expression_candidate(field)
            if candidate is not None:
                data = {**data, "raw_expression": candidate}
        return data

    def _before_assign(self, name: str) -> None:
        if name == "field":
            object.__setattr__(self, "raw_expression", None)

    @model_validator(mode="after")
    def _check_reference(self) -> "OrderItem":
        if self.raw_expression is None and field_ref_of(self.field) is None:
            FieldRef.model_validate({"name": self.field})
        return self

    @model_serializer(mode="wrap")
    def _omit_unset(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        return _without_none(handler(self))

    @property
    def ref(self) -> FieldRef | None:
        """The field as a reference, ``None`` for an expression."""
        return None if self.raw_expression else field_ref_of(self.field)

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

    @property
    @deprecated("column is deprecated; use field", category=None)
    def column(self) -> str:
        attribute_used(self, old="column", new="field")
        return self.field

    @column.setter
    @deprecated("column is deprecated; use field", category=None)
    def column(self, value: str) -> None:
        attribute_used(self, old="column", new="field")
        self.field = value

    @property
    @deprecated("raw_formula is deprecated; use raw_expression", category=None)
    def raw_formula(self) -> str | None:
        attribute_used(self, old="raw_formula", new="raw_expression")
        return self.raw_expression

    @raw_formula.setter
    @deprecated("raw_formula is deprecated; use raw_expression", category=None)
    def raw_formula(self, value: str | None) -> None:
        attribute_used(self, old="raw_formula", new="raw_expression")
        self.raw_expression = value


@vocabulary.input_object("metric")
class QueryMetric(MetricBase, QueryInput):
    """A query metric entry: a saved metric's reference or an aggregate expression, with an optional result ``alias``."""

    _TEXT: ClassVar[str] = "metric"
    _WHAT: ClassVar[str] = "a query metric entry"

    metric: str = Field(description="A saved metric's reference or an aggregate expression.")
    alias: str | None = Field(
        default=None, description="Result key; filters and order may use it in place of the metric.",
    )
    label: str | None = None
    description: str | None = None
    datatype: DataType | None = None
    meta: dict[str, Any] | None = None

    @model_validator(mode="wrap")
    @classmethod
    def _entry_input(cls, data: Any, handler: ModelWrapValidatorHandler["QueryMetric"]) -> "QueryMetric":
        return converted_input(data, handler, vocabulary.query_metric, stored=in_stored_document())

    @field_validator("alias")
    @classmethod
    def _check_alias(cls, v: str | None) -> str | None:
        return v if v is None else check_metric_name(v, what="Query metric alias")

    @field_validator("metric")
    @classmethod
    def _check_metric(cls, v: str) -> str:
        return check_metric_text(v, what="Query metric")

    @model_serializer(mode="wrap")
    def _bare_text(self, handler: SerializerFunctionWrapHandler) -> Any:
        dumped = _without_none(handler(self))
        return self.metric if set(dumped) == {"metric"} else dumped

    @property
    @deprecated("formula is deprecated; use metric", category=None)
    def formula(self) -> str:
        attribute_used(self, old="formula", new="metric")
        return self.metric

    @formula.setter
    @deprecated("formula is deprecated; use metric", category=None)
    def formula(self, value: str) -> None:
        attribute_used(self, old="formula", new="metric")
        self.metric = value


_DATASET_METRIC_EXTRAS = ("label", "description", "datatype", "meta")


def _metric_item(item: Any) -> Any:
    """A ``metrics`` entry: a string is its ``metric``; a pre-2.0 ``DatasetMetric`` object converts."""
    if isinstance(item, str):
        return {"metric": item}
    if isinstance(item, DatasetMetric):
        spellings = current_spellings()
        if spellings is not None:
            spellings.note("DatasetMetric in a query's metrics", "QueryMetric")
        extras = {k: getattr(item, k) for k in _DATASET_METRIC_EXTRAS if getattr(item, k) is not None}
        return {"metric": item.text, "alias": item.name, **extras}
    return item


def _coerce_metrics(v: Any) -> Any:
    if v is None:
        return v
    if not isinstance(v, (list, tuple)):
        raise TypeError(f"'metrics' must be a list, got {type(v).__name__}")
    return [_metric_item(item) for item in v]


def _check_dimension_text(text: str) -> None:
    """A string dimension is a field reference or a parseable Mode-B expression."""
    if field_ref_of(text) is not None:
        return
    try:
        parse_expr(text)
    except ColonSpellingError:
        raise
    except Exception as exc:
        raise ValueError(
            f"Dimension {text!r} is neither a field reference (a bare identifier or dotted relationship path) "
            f"nor a parseable Mode-B expression: {exc}"
        ) from exc


def _coerce_dimension_item(item: Any) -> Any:
    """A ``dimensions`` entry: a string is its ``field``; an internal ``FieldRef`` is its spelling."""
    if isinstance(item, str):
        _check_dimension_text(item)
        return {"field": item}
    if isinstance(item, FieldRef):
        return {"field": item.spelling, **({"label": item.label} if item.label is not None else {})}
    return item


def _coerce_dimensions(v: Any) -> Any:
    """Allow plain strings / expression dicts in the dimensions list."""
    if v is None:
        return v
    if not isinstance(v, (list, tuple)):
        raise TypeError(f"'dimensions' must be a list, got {type(v).__name__}")
    return [_coerce_dimension_item(item) for item in v]


_ORDER_ITEM_RESERVED = frozenset({"field", "direction", *(old for old, new in ORDER_ITEM_KEYS.items() if new == "field")})


def _process_order_item(item: Any) -> list:
    """Heal a single ``order`` entry into the canonical items it expands to; a shorthand dict
    field → direction (no ``field``/``direction`` key, all-direction values) expands one item per key."""
    if not isinstance(item, dict):
        return [item]
    # A dict with a reserved key, in either vocabulary, is canonical-intended, never shorthand.
    if _ORDER_ITEM_RESERVED & item.keys():
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


def _coerce_filter(v: Any) -> Any:
    """An empty filter list is no filter."""
    return None if isinstance(v, (list, tuple)) and not v else v


def map_filter(value: str | list[str] | None, fn: Callable[[str], str]) -> str | list[str] | None:
    """``fn`` over each condition of a ``filter``, the shape kept."""
    if value is None:
        return None
    return fn(value) if isinstance(value, str) else [fn(c) for c in value]


def filter_conditions(value: str | list[str] | None) -> list[str]:
    """A ``filter``'s conditions in order: one string is one condition."""
    if value is None:
        return []
    return [value] if isinstance(value, str) else list(value)


# Field types shared by ``SlayerQuery`` and ``QueryRefinement`` (one coercion each).
MetricsField = Annotated[list[QueryMetric] | None, BeforeValidator(_coerce_metrics)]
DimensionsField = Annotated[list[QueryDimension] | None, BeforeValidator(_coerce_dimensions)]
OrderField = Annotated[list[OrderItem] | None, BeforeValidator(_coerce_order)]
FilterField = Annotated[str | list[str] | None, BeforeValidator(_coerce_filter)]


_UNCAST = (TypeKind.TEXT, TypeKind.UNKNOWN)


def extension_field_text(field: dict, *, owner: str, what: str) -> str:
    """A removed extension field as one Mode-B row expression relative to ``owner``: its name when it has no ``sql``,
    else ``sql`` wrapped as ``iif(<filter>, <sql>, NULL)`` and cast to a declared type other than TEXT / UNKNOWN.
    Raises ``ValueError`` naming ``what`` when nothing preserves its meaning."""
    data_type = stored_datatype(field.get("type") or field.get("datatype")) or DataType.STRING
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
    if data_type.kind not in _UNCAST:
        text = f"CAST({text} AS {data_type.kind.value})"
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
        for expression in [_field_expression(column, dataset=vocabulary.extension_dataset(data))]
        if expression is not None and isinstance(column.get("name"), str)
    ] if isinstance(columns, list) else []
    example = f" ({'; '.join(written)})" if written else ""
    return (
        "Extension fields were removed: write a per-query field as an expression in the position that uses "
        f"it — a computed dimension, a filter, an aggregation's argument{example}."
    )


@vocabulary.input_object("extension")
class DatasetExtension(QueryInput):
    """Query-scoped metrics and relationships added to one dataset a query object reads."""

    _WHAT: ClassVar[str] = "an extension"

    dataset: str                                # Dataset/stage to extend
    metrics: list[DatasetMetric] | None = None
    relationships: list[DatasetRelationship] | None = None

    @model_validator(mode="wrap")
    @classmethod
    def _old_spellings(cls, data: Any, handler: ModelWrapValidatorHandler["DatasetExtension"]) -> "DatasetExtension":
        return converted_input(data, handler, vocabulary.extension_keys, stored=in_stored_document())

    @model_serializer(mode="wrap")
    def _compact_metrics(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        dumped = handler(self)
        if isinstance(dumped.get("metrics"), list):
            dumped["metrics"] = [_without_none(m) if isinstance(m, dict) else m for m in dumped["metrics"]]
        return dumped

    @property
    @deprecated("source_name is deprecated; use dataset", category=None)
    def source_name(self) -> str:
        attribute_used(self, old="source_name", new="dataset")
        return self.dataset

    @source_name.setter
    @deprecated("source_name is deprecated; use dataset", category=None)
    def source_name(self, value: str) -> None:
        attribute_used(self, old="source_name", new="dataset")
        self.dataset = value

    @model_validator(mode="before")
    @classmethod
    def _refuse_fields(cls, data: Any) -> Any:
        if isinstance(data, dict) and "columns" in data:
            raise ValueError(_extension_fields_removed(data))
        return data

    @property
    @deprecated("measures is deprecated; use metrics", category=None)
    def measures(self) -> list[DatasetMetric] | None:
        attribute_used(self, old="measures", new="metrics")
        return self.metrics

    @measures.setter
    @deprecated("measures is deprecated; use metrics", category=None)
    def measures(self, value: list[DatasetMetric] | None) -> None:
        attribute_used(self, old="measures", new="metrics")
        self.metrics = value

    @property
    @deprecated("joins is deprecated; use relationships", category=None)
    def joins(self) -> list[DatasetRelationship] | None:
        attribute_used(self, old="joins", new="relationships")
        return self.relationships

    @joins.setter
    @deprecated("joins is deprecated; use relationships", category=None)
    def joins(self, value: list[DatasetRelationship] | None) -> None:
        attribute_used(self, old="joins", new="relationships")
        self.relationships = value





def convert_legacy_root_extension(data: dict) -> tuple[dict, DeprecatedInputWarning | None]:
    """Fresh input's legacy root extension, moved into ``extensions``, with its deprecation (none in a stored document)."""
    source = data.get("population")
    if isinstance(source, DatasetExtension):
        data = with_root_extension(data, extension=source, base=source.dataset, key="population")
    elif "version" not in data and is_legacy_root_extension(source):
        data = hoist_root_extension({**data, "population": Spellings(stored=True).rename(source, {"source_name": "dataset"})},
                                    key="population")
    else:
        return data, None
    if in_stored_document():
        return data, None
    return data, deprecated_input("An extension object in `population`", "`extensions`", removal="3.0")


def without_null_version(data: Any) -> Any:
    """Query input whose null ``version`` reads as omitted; run first on every entry point."""
    if isinstance(data, dict) and "version" in data and data["version"] is None:
        return {k: v for k, v in data.items() if k != "version"}
    return data


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
    names = [n for e in query.extensions or [] for n in (e.dataset, *(j.to for j in e.relationships or []))]
    return list(dict.fromkeys(names))


def check_unique_extensions(extensions: list[DatasetExtension] | None) -> None:
    """Reject a second extension of one dataset."""
    seen: set[str] = set()
    for extension in extensions or ():
        if extension.dataset in seen:
            raise ValueError(
                f"Dataset {extension.dataset!r} is extended twice; merge its metrics and relationships into one "
                f"`extensions` entry."
            )
        seen.add(extension.dataset)


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


@vocabulary.input_object("query")
class SlayerQuery(QueryInput):
    """User-facing query object — what to retrieve from a dataset, as names/references, no SQL."""

    _WHAT: ClassVar[str] = "a query"

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
            "{\"metric\": \"sum(iif(customers.orders.order_date >= '2025-01-01', 1, 0))\", "
            '"alias": "orders_2025"} list every region, including those without orders (a count '
            "over no rows is 0, any other aggregate NULL). Always alias a conditional aggregate. "
            "Otherwise root where the facts are (rows without facts then drop out), or omit to infer "
            "the rows: every dimension combination some metric's rows reach (the dimension values "
            "alone with no metrics, one row with no dimensions), reported in the response. "
            "A saved dataset or stage name. "
            "Write a per-query field as an expression in the position that uses it (a "
            "dimension, a filter, an aggregation's argument)."
        ),
    )
    population_inferred: bool = Field(
        default=False,
        description="Set by saving; keeps the rows inferred while `population` pins the anchor.",
    )
    extensions: list[DatasetExtension] | None = Field(
        default=None,
        description=(
            "Query-scoped metrics and relationships added to any dataset this query object reads "
            "(its population, a joined dataset, or a stage), at most one entry per dataset, visible "
            "only in this query object. A relationship repeating a stored relationship, in either "
            "direction, is that edge, and its name is a local alias. An `inner` relationship keeps only "
            "the rows with a match; it never multiplies rows. Rooted at orders: "
            '[{"dataset": "customers", "metrics": [{"name": "credit_total", "expression": '
            '"sum(credit)"}], "relationships": [{"to": "vip_list", "from_columns": ["id"], '
            '"to_columns": ["customer_id"], "join_type": "inner"}]}] adds customers.credit_total and '
            "keeps only the orders of customers on the vip list."
        ),
    )
    metrics: MetricsField = Field(
        default=None,
        description=(
            'Values to return: expression strings or {"metric": ..., "alias": ...} (alias sets the '
            "result key). Every field, metric and row count names its dataset first: orders.amount, "
            "orders.revenue (a saved metric), count(orders.*); a bare name is an alias of this query, else a "
            "model-scoped metric of a semantic model. AGG(orders.revenue) evaluates the dataset metric. "
            "The engine joins the datasets. Expressions combine aggregations with arithmetic, also "
            "across datasets: rooted at customers, sum(orders.amount) / count(customers.*) divides "
            "the orders' total by the number of customers.\n"
            "- Aggregations: count(orders.*), sum(orders.amount), count_distinct(orders.customer_id), "
            "avg(orders.amount - orders.cost), percentile(orders.amount, p=0.9).\n"
            "- agg(x) FILTER (WHERE c) aggregates only the rows where c holds: "
            "count(orders.*) FILTER (WHERE orders.status = 'paid').\n"
            "- partition_by= computes an aggregate at a coarser grain, repeated on every row; "
            "spell its keys exactly like the query's dimensions. Share of group: "
            "sum(orders.amount) / sum(orders.amount, partition_by=customers.regions.name); share of "
            "the grand total: sum(orders.amount) / sum(orders.amount, partition_by=[]).\n"
            "- window= turns any aggregation into a trailing time window ending at each time "
            "bucket, in compact durations only ('90d', '3m', '1y'; never '3 months'): "
            "count_distinct(orders.customer_id, window='90d').\n"
            "- Nesting aggregates per-group values: "
            "avg(sum(orders.amount, partition_by=[customers.regions.name, customers.id]), "
            "partition_by=[customers.regions.name]) averages the per-customer totals in each region.\n"
            "- Transforms wrap an aggregate; all but the rank family need a time dimension: "
            "cumsum(sum(orders.amount)) running total; change(sum(orders.amount)) / "
            "change_pct(sum(orders.amount)) period-over-period delta / ratio; "
            "time_shift(sum(orders.amount), -1) the previous "
            "bucket's value ('year' as a third argument for year-over-year); lag(x, n) / "
            "lead(x, n) n rows back / ahead; rank(x, direction='desc') / "
            "dense_rank(x, direction='asc') with direction required and partition_by= to rank "
            "within groups; ntile(x, n=4) buckets, lowest first; "
            "consecutive_periods(sum(orders.amount) > 0) trailing run length.\n"
            "- A joined dataset's aggregate such as sum(customers.credit) from orders is computed "
            "over that dataset's own rows, each customer once. Sliced by a dimension it cannot "
            "be attributed to (an order's status), each group aggregates the distinct customers "
            "reaching it, with an associated warning (groups may overlap); "
            "sum(customers.credit, partition_by=[]) repeats the total on every group instead. Rooted at customers, "
            "sum(orders.amount) and sum(returns.amount) in one query are each computed over their "
            "own rows: several child datasets never fan out or double count. A child row without a "
            "parent (an order with no customer) has NULL parent fields: it counts in the NULL "
            "row of a parent dimension when that row exists, and in an ungrouped total; never add "
            "it again. An aggregate over a dataset no relationship path reaches from the root is computed "
            "over that dataset's own rows and repeated on every row (per bucket of a time_spine "
            "time dimension), with an unrelated_dataset warning: rooted at customers, "
            "count(events.id) / count(customers.*).\n"
            "More (every aggregation, empty-input values, nesting and transform rules): "
            'inspect(reference=["memory:help.aggregations", "memory:help.transforms"], '
            'entity_type="memory").'
        ),
    )

    _deprecated_inputs: tuple[DeprecatedInputWarning, ...] = PrivateAttr(default=())

    def __eq__(self, other: object) -> bool:
        """Equal whatever pre-2.0 spellings either was read from."""
        if not isinstance(other, BaseModel) or type(other) is not type(self):
            return NotImplemented
        return same_ignoring_deprecations(self, other)

    @model_validator(mode="wrap")
    @classmethod
    def _migrate_and_rewrite(cls, data: Any, handler: ModelWrapValidatorHandler["SlayerQuery"]) -> "SlayerQuery":
        # One validator sequences convert → migrate → functional-granularity rewrite.
        payloads: list[DeprecatedInputWarning] = []
        data = without_null_version(data)
        is_dict = isinstance(data, dict)
        stored =in_stored_document() or (is_dict and "version" in data)
        with converted_document(stored=stored) as (spellings, _):
            if isinstance(data, dict):
                data = vocabulary.camel_healed(data, "query")
                if not stored:
                    data = spellings.rename(data, QUERY_KEYS)
                data, payloads = prepare_query_input(data)
            data = _migrate_schema(entity="SlayerQuery", data=data)
            if isinstance(data, dict):
                data = spellings.rename(data, QUERY_KEYS)
                if is_legacy_root_extension(data.get("population")):
                    raise ValueError(
                        "`population` names a dataset; put a query-scoped extension in `extensions`."
                    )
                data = _rewrite_functional_granularity(data)
            query = handler(data)
            if is_dict:
                query._deprecated_inputs = (*payloads, *spellings.report())
                if not stored:
                    warn_fresh_broadcast(query.to_many_handling)
        return query

    @model_serializer(mode="wrap")
    def _omit_unset(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        """Unset clauses (``None``, an empty ``filter`` included) are left out of the dump."""
        return _without_none(handler(self))

    @property
    def deprecated_inputs(self) -> tuple[DeprecatedInputWarning, ...]:
        """Deprecations of the input forms converted while building this query."""
        return self._deprecated_inputs

    @field_validator("*", mode="before")
    @classmethod
    def _null_reads_as_default(cls, v: Any, info: ValidationInfo) -> Any:
        """A null on a setting that cannot be null takes its default (after spelling conflicts are checked)."""
        if v is None and info.field_name is not None:
            default = cls.model_fields[info.field_name].get_default(call_default_factory=True)
            return v if default is None else default
        return v

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
            "Group-by fields, one result row per distinct combination: fields named by their "
            "dataset (orders.status), a route from it (customers.regions.name), row-level "
            'expressions, or {"field": ..., "alias": ...}. '
            "date_trunc('<unit>', expr) buckets a timestamp like a time_dimensions entry: "
            "date_trunc('month', orders.created_at); units are second, minute, hour, day, week, "
            "week_sunday, month, quarter, year, or the datasource's custom ones, while "
            "month(orders.created_at) is the month of year (an integer). An aggregate inside a "
            "computed dimension needs partition_by= to fix its grain: "
            "{\"field\": \"iif(sum(orders.amount, partition_by=customers.id) > 150, 'high', "
            "'low')\", \"alias\": \"spend_band\"}. Group by naturally readable labels: a name "
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
            "\"date_trunc('month', orders.created_at)\"; the field may be any date or time expression, with an "
            "optional alias. Buckets without rows are absent. To return every bucket, "
            "empty ones included, bucket time_spine.timestamp with a lower date_range bound and "
            "aggregate datasets by dotted path: "
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
    filter: FilterField = Field(
        default=None,
        description=(
            "A condition, or a list of conditions AND-ed; each routes automatically to row filtering (WHERE), aggregate "
            "filtering (HAVING) or filtering after transforms. SQL operators: = <> < <= > >=, in, "
            "between, like / ilike, is null / is not null, IS [NOT] TRUE / FALSE (NULL counts as "
            "not true), IS DISTINCT FROM / IS NOT DISTINCT FROM (NULL-safe =), and / or / not, "
            "CAST(x AS DATE), regex x ~ '^A[0-9]{3}', "
            "JSON payload ->> 'key'; strings are single-quoted ('paid'); build them from the "
            "field's sampled values.\n"
            "- Aggregates and transforms filter too, by expression or by a metric's name: "
            "sum(orders.amount) > 100; top N per group: "
            "rank(sum(orders.amount), partition_by=customers.regions.name, direction='desc') <= 1.\n"
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
            "a list as the whole right-hand side of IN (items.status in ({statuses}); over a list field, one "
            "list: items.tags in ({wanted})) or as a list literal anywhere else (items.tags = {wanted}, a "
            "list-function argument). A placeholder in a name position is an error."
        ),
    )
    order: OrderField = Field(
        default=None,
        description=(
            'Sort keys, [{"field": ..., "direction": "asc" | "desc", "nulls": "first" | "last"}] '
            "(nulls optional). field is a result column name or any aggregate or transform "
            "expression, displayed or not: with "
            'metrics ["count(orders.*)"], [{"field": "sum(orders.amount)", "direction": "desc"}] sorts by '
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
            "default and add `count(<dataset>.*)`."
        ),
    )

    # Filters keep EXISTS pushdown in every mode.
    to_many_handling: ManyHandling = Field(
        default=None,
        json_schema_extra=_offered_modes,
        description=(
            "What happens when an aggregation is sliced by a dimension not "
            "attributable to it. Omitted (default) or associate: aggregate per cell "
            "over the distinct associated rows, e.g. per list element tags.value | "
            "error (refuse). To repeat a value across the cells, give the aggregate "
            "partition_by= with the dimensions it is attributable to."
        ),
    )

    @model_validator(mode="after")
    def _validate_dsl_user_input(self) -> "SlayerQuery":
        """Enforce DSL-mode rules on user-input strings at construction — raw ``OVER (...)``
        in a filter is caught here; bare-name/raw-SQL-function rejection happen at binding.
        ``__`` is not rejected: virtual-dataset fields flatten relationship paths (``kpis__total_amount_sum``)."""
        for f in self.filter_conditions:
            _validate_query_filter_string(f)
        if self.main_time_dimension is not None:
            _parsed_or_none(self.main_time_dimension)
        self._validate_distinct_dimension_values()
        check_unique_extensions(self.extensions)
        if self.population_inferred and self.population is None:
            raise ValueError("`population_inferred` needs `population`: it marks a pinned anchor")
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
            return (_parsed_identity(td.template, root), td.granularity, tuple(td.date_range or ()), td.alias, td.label)

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
                    f"granularity must not differ in date range, alias or label."
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
            ref = dim.ref
            taken.add(dim.name if dim.is_computed or ref is None else ref.full_name)
        identities: dict[str, tuple] = {}
        for td in self.time_dimensions or ():
            if (named := _time_dimension_name_identity(td)) is None:
                continue
            name, identity = named
            if name in taken or identities.setdefault(name, identity) != identity:
                raise ValueError(
                    f"Dimension name {name!r} is used twice; give the time dimension {td.field!r} a "
                    f"distinct `alias`."
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

    @property
    def filter_conditions(self) -> list[str]:
        """The filter's conditions, all of which must hold."""
        return filter_conditions(self.filter)

    @property
    @deprecated("source_model is deprecated; use population", category=None)
    def source_model(self) -> SourceSpec | None:
        attribute_used(self, old="source_model", new="population")
        return self.population

    @source_model.setter
    @deprecated("source_model is deprecated; use population", category=None)
    def source_model(self, value: SourceSpec | None) -> None:
        attribute_used(self, old="source_model", new="population")
        self.population = value

    @property
    @deprecated("measures is deprecated; use metrics", category=None)
    def measures(self) -> MetricsField:
        attribute_used(self, old="measures", new="metrics")
        return self.metrics

    @measures.setter
    @deprecated("measures is deprecated; use metrics", category=None)
    def measures(self, value: MetricsField) -> None:
        attribute_used(self, old="measures", new="metrics")
        self.metrics = value

    @property
    @deprecated("filters is deprecated; use filter", category=None)
    def filters(self) -> str | list[str] | None:
        attribute_used(self, old="filters", new="filter")
        return self.filter

    @filters.setter
    @deprecated("filters is deprecated; use filter", category=None)
    def filters(self, value: str | list[str] | None) -> None:
        attribute_used(self, old="filters", new="filter")
        self.filter = value

    @property
    @deprecated("source_model_name is deprecated; use population_name", category=None)
    def source_model_name(self) -> str | None:
        attribute_used(self, old="source_model_name", new="population_name")
        return self.population_name


def _refinement_field(name: str) -> Any:
    """``SlayerQuery``'s field ``name`` with a ``None`` default (supplied-ness is ``model_fields_set``)."""
    info = SlayerQuery.model_fields[name]
    return Field(
        default=None, description=f"Same syntax as the query object's {name}.",
        json_schema_extra=info.json_schema_extra,
    )


_NON_NULLABLE_SETTINGS = ("whole_periods_only", "distinct_dimension_values")


@vocabulary.input_object("refinement")
class QueryRefinement(QueryInput):
    """Clauses merged into a saved query's final stage when it runs by name."""

    _WHAT: ClassVar[str] = "a refinement"

    dimensions: DimensionsField = _refinement_field("dimensions")
    time_dimensions: list[TimeDimension] | None = _refinement_field("time_dimensions")
    metrics: MetricsField = _refinement_field("metrics")
    filter: FilterField = _refinement_field("filter")
    order: OrderField = _refinement_field("order")
    limit: int | None = _refinement_field("limit")
    offset: int | None = _refinement_field("offset")
    main_time_dimension: str | None = _refinement_field("main_time_dimension")
    whole_periods_only: bool | None = _refinement_field("whole_periods_only")
    distinct_dimension_values: bool | None = _refinement_field("distinct_dimension_values")
    to_many_handling: ManyHandling = _refinement_field("to_many_handling")

    _deprecated_inputs: tuple[DeprecatedInputWarning, ...] = PrivateAttr(default=())

    def __eq__(self, other: object) -> bool:
        """Equal whatever pre-2.0 spellings either was read from."""
        if not isinstance(other, BaseModel) or type(other) is not type(self):
            return NotImplemented
        return same_ignoring_deprecations(self, other)

    @model_validator(mode="wrap")
    @classmethod
    def _convert_and_rewrite(
        cls, data: Any, handler: ModelWrapValidatorHandler["QueryRefinement"],
    ) -> "QueryRefinement":
        payload = None
        is_dict = isinstance(data, dict)
        with converted_document(stored=in_stored_document()) as (spellings, _):
            if isinstance(data, dict):
                data = spellings.rename(vocabulary.camel_healed(data, "refinement"), REFINEMENT_KEYS)
                if "strict" in data:
                    data, payload = _convert_strict(data)
                data = _rewrite_functional_granularity(data)
            refinement = handler(data)
            if is_dict:
                refinement._deprecated_inputs = (*((payload,) if payload is not None else ()), *spellings.report())
                warn_fresh_broadcast(refinement.to_many_handling)
        return refinement

    @property
    def deprecated_inputs(self) -> tuple[DeprecatedInputWarning, ...]:
        """Deprecations of the input forms converted while building this refinement."""
        return self._deprecated_inputs

    @property
    def filter_conditions(self) -> list[str]:
        """The filter's conditions, all of which must hold."""
        return filter_conditions(self.filter)

    @model_validator(mode="after")
    def _validate_inputs(self) -> "QueryRefinement":
        for f in filter_conditions(self.filter):
            _validate_query_filter_string(f)
        for name in _NON_NULLABLE_SETTINGS:
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"refine.{name} cannot be null; omit it to keep the saved value")
        return self

    @property
    @deprecated("measures is deprecated; use metrics", category=None)
    def measures(self) -> MetricsField:
        attribute_used(self, old="measures", new="metrics")
        return self.metrics

    @measures.setter
    @deprecated("measures is deprecated; use metrics", category=None)
    def measures(self, value: MetricsField) -> None:
        attribute_used(self, old="measures", new="metrics")
        self.metrics = value

    @property
    @deprecated("filters is deprecated; use filter", category=None)
    def filters(self) -> str | list[str] | None:
        attribute_used(self, old="filters", new="filter")
        return self.filter

    @filters.setter
    @deprecated("filters is deprecated; use filter", category=None)
    def filters(self, value: str | list[str] | None) -> None:
        attribute_used(self, old="filters", new="filter")
        self.filter = value


def declared_aliases(*sources: "SlayerQuery | QueryRefinement") -> list[str]:
    """The names the entries of ``sources`` declare: aliases and unaliased computed dimensions' names."""
    names: list[str] = []
    for source in sources:
        names.extend(m.alias for m in source.metrics or [] if m.alias)
        for d in source.dimensions or []:
            if d.alias or d.ref is None:
                names.append(d.name)
        names.extend(td.alias for td in source.time_dimensions or [] if td.alias)
    return names


BarePosition = Literal["entry", "filter"]
BareReading = Literal["alias", "population", "unresolved"]


def pre_qualified_reading(
    name: str, *, quoted: bool, position: BarePosition, aliases: Iterable[str], fields: Iterable[str],
    metrics: Iterable[str],
) -> BareReading:
    """What a pre-2.0 bare ``name`` read, by position: in an entry (``aliases`` without its own) the population's field,
    metric, then an alias; in a filter or order its field, an alias, then its metric. Ambiguity raises."""
    order: list[tuple[BareReading, Iterable[str]]] = [
        ("population", fields), ("population", metrics), ("alias", aliases)] if position == "entry" else [
        ("population", fields), ("alias", aliases), ("population", metrics)]
    for reading, declared in order:
        if match_name(name=name, declared=declared, quoted=quoted) is not None:
            return reading
    return "unresolved"


def deprecated_inputs_of(
    sources: Iterable[SlayerDataset | SlayerQuery | QueryRefinement | None],
) -> list[DeprecatedInputWarning]:
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


_RESTATABLE_FIELDS = ("label", "datatype", "description", "meta")


def _restated_name(expression: str, root: str | None) -> str | None:
    """The saved-metric name ``expression`` restates: a bare name, or that name qualified by the root."""
    node = _parsed_or_none(expression)
    node = root_relative(node, root) if node is not None else None
    return node.name if isinstance(node, Ref) else None


def _check_restatement(entry: QueryMetric, prior: QueryMetric) -> None:
    for field in _RESTATABLE_FIELDS:
        if field in entry.model_fields_set and getattr(entry, field) != getattr(prior, field):
            raise RefinementConflictError(
                field="metrics", key=prior.alias or "", saved=_show(prior), refined=_show(entry),
                suggestion=_ENTRY_CONFLICT_SUGGESTION,
            )


def _drop_restatements(
    saved: list[QueryMetric] | None, refined: list[QueryMetric] | None, root: str | None,
) -> list[QueryMetric] | None:
    """``refined`` without the entries restating a saved entry by alias; a differing set field conflicts."""
    by_alias = {m.alias: m for m in saved or [] if m.alias}
    kept: list[QueryMetric] = []
    for entry in refined or []:
        name = _restated_name(entry.metric, root)
        prior = by_alias.get(name) if name is not None else None
        if prior is None or entry.alias not in (None, prior.alias):
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
    """Per (dimension, granularity): append new keys, merge ``date_range`` / ``alias`` / ``label`` of existing ones."""
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
            for attribute in ("date_range", "alias", "label")
        }
        merged[i] = prior.model_copy(update=update)
    return merged


def _merged_filter(saved: str | list[str] | None, refined: str | list[str] | None) -> str | list[str] | None:
    """The saved filter with the refinement's new conditions appended (a list); the saved one as written when none is
    new, the refinement's as written onto no filter."""
    kept = filter_conditions(saved)
    added = list(dict.fromkeys(c for c in filter_conditions(refined) if c not in kept))
    if not added:
        return saved
    return [*kept, *added] if kept else refined


def refine_query(*, saved: SlayerQuery, refinement: QueryRefinement) -> SlayerQuery:
    """The saved final stage with ``refinement`` merged in, revalidated as a whole."""
    root = saved.population_name
    supplied = refinement.model_fields_set
    data: dict[str, Any] = {name: getattr(saved, name) for name in saved.model_fields_set | {"version"}}

    def reference(dim: QueryDimension) -> FieldRef | None:
        ref = dim.ref
        return None if dim.is_computed or ref is None else _root_relative_ref(ref, root)

    def canonical_dimension(dim: QueryDimension) -> Any:
        ref = reference(dim)
        return (dim.template, dim.name, dim.label) if ref is None else (ref.spelling, dim.label)

    def dimension_identity(dim: QueryDimension) -> tuple[str, str]:
        ref = reference(dim)
        return ("computed", dim.name) if ref is None else ("column", ref.full_name)

    def canonical_metric(m: QueryMetric) -> QueryMetric:
        return m.model_copy(update={"metric": _parsed_identity(m.metric, root)})

    merged_lists = {
        "dimensions": _union(
            saved.dimensions, refinement.dimensions, field="dimensions", canon=canonical_dimension,
            identity=dimension_identity,
        ),
        "metrics": _union(
            saved.metrics, _drop_restatements(saved.metrics, refinement.metrics, root), field="metrics",
            canon=canonical_metric,
            identity=lambda m: ("name", m.alias) if m.alias else ("expression", _parsed_identity(m.metric, root)),
        ),
        "time_dimensions": _merge_time_dimensions(saved.time_dimensions, refinement.time_dimensions, root),
        "filter": _merged_filter(saved.filter, refinement.filter),
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


if TYPE_CHECKING:
    @deprecated("ColumnRef is deprecated; use QueryDimension", category=None)
    class ColumnRef(QueryDimension): ...

    @deprecated("ComputedDimension is deprecated; use QueryDimension", category=None)
    class ComputedDimension(QueryDimension): ...

    @deprecated("ModelExtension is deprecated; use DatasetExtension", category=None)
    class ModelExtension(DatasetExtension): ...

    @deprecated("ModelVariables is deprecated; use DatasetVariables", category=None)
    class ModelVariables(DatasetVariables): ...

    @deprecated("ModelMeasure is deprecated; use DatasetMetric", category=None)
    class ModelMeasure(DatasetMetric): ...


__getattr__ = name_twins(namespace=globals(), renames={
    "ColumnRef": "QueryDimension", "ComputedDimension": "QueryDimension", "ModelExtension": "DatasetExtension",
    "ModelVariables": "DatasetVariables",
    "ModelMeasure": "DatasetMetric",
})
