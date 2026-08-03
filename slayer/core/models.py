"""Core domain models for SLayer."""

import logging
import os
import re
from typing import Annotated, Any, Optional

from pydantic import BaseModel, BeforeValidator, Field, field_validator, model_validator
from sqlalchemy.engine import URL as _SA_URL

from slayer.core.enums import (
    BUILTIN_AGGREGATIONS,
    DataType,
    JoinType,
    _coerce_legacy_datatype,
)
from slayer.core.format import NumberFormat
from slayer.core.formula import ALL_TRANSFORMS
from slayer.sql.sql_predicate import parse_sql_predicate
from slayer.sql.window_detect import WINDOW_IN_FILTER_ERROR, has_window_function
from slayer.storage.migrations import migrate as _migrate_schema

_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

logger = logging.getLogger(__name__)

_MULTIDOT_COLUMN_RE = re.compile(r'\b([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*){2,})\b')
_STRING_LITERAL_RE = re.compile(r"'[^']*'")


class _SubstringRule:
    """Single source of truth for a forbidden substring inside a name.

    Each rule pairs the forbidden character / digraph with the rationale.
    Every validator that rejects the same substring uses the same rule
    so the wording (and the rejection rationale) lives in one place.
    """

    __slots__ = ("substring", "reason")

    def __init__(self, *, substring: str, reason: str) -> None:
        self.substring = substring
        self.reason = reason

    def check(self, name: str, context: str) -> None:
        if self.substring in name:
            raise ValueError(
                f"{context} '{name}' must not contain "
                f"{self.substring!r}; {self.reason}"
            )


_NO_DUNDER = _SubstringRule(
    substring="__",
    reason="double underscores are reserved for join path aliases in "
           "generated SQL.",
)
_NO_DOT = _SubstringRule(
    substring=".",
    reason="dots are the canonical-id namespace delimiter "
           "(``<ds>.<model>.<leaf>``) and the dotted-path reference "
           "syntax in queries.",
)
_NO_COLON = _SubstringRule(
    substring=":",
    reason="colons are reserved as the aggregation separator "
           "(``revenue:sum``) and the ``memory:<int>`` canonical-id "
           "prefix.",
)
_NO_FWD_SLASH = _SubstringRule(
    substring="/",
    reason="path separators break the storage layout.",
)
_NO_BACK_SLASH = _SubstringRule(
    substring="\\",
    reason="path separators break the storage layout.",
)
_NO_NUL = _SubstringRule(
    substring="\x00",
    reason="NUL bytes are filesystem-unsafe.",
)


def _require_non_empty_trimmed(v: str, context: str) -> None:
    """Reject empty / whitespace-only inputs and inputs with
    leading or trailing whitespace."""
    if not v or not v.strip():
        raise ValueError(
            f"{context} must be a non-empty string; got {v!r}."
        )
    if v.strip() != v:
        raise ValueError(
            f"{context} must not have leading/trailing whitespace; "
            f"got {v!r}."
        )


def _validate_model_name(name: str, context: str) -> str:
    """Reject model/query names containing ``__``, ``.``, or ``:``."""
    label = f"{context} name"
    _NO_DUNDER.check(name=name, context=label)
    _NO_DOT.check(name=name, context=label)
    _NO_COLON.check(name=name, context=label)
    return name


def _validate_column_name(name: str, context: str) -> str:
    """Reject dimension/measure names containing ``.`` or ``:``.

    ``__`` is allowed — it encodes flattened join paths in virtual
    models created by ``_query_as_model`` (e.g., ``stores__name``).
    """
    label = f"{context} name"
    _NO_DOT.check(name=name, context=label)
    _NO_COLON.check(name=name, context=label)
    return name


def _convert_multidot_ref(match: re.Match) -> str:
    """Convert a multi-dot reference like ``a.b.c`` to ``a__b.c``."""
    ref = match.group(1)
    parts = ref.split(".")
    return "__".join(parts[:-1]) + "." + parts[-1]


def _fix_multidot_sql(sql: str, context: str) -> str:
    """Auto-convert multi-dot references in a SQL snippet to __ alias syntax.

    Single-dot references (``table.column``) are left as-is.
    Multi-dot references (``a.b.c``) are converted to ``a__b.c`` with a warning.
    String literals are skipped.
    """
    # Build a map of string-literal spans to skip
    literal_spans = [m.span() for m in _STRING_LITERAL_RE.finditer(sql)]

    def _in_literal(start: int) -> bool:
        return any(s <= start < e for s, e in literal_spans)

    result = sql
    for match in list(_MULTIDOT_COLUMN_RE.finditer(sql)):
        if _in_literal(match.start()):
            continue
        ref = match.group(1)
        fixed = _convert_multidot_ref(match)
        logger.warning(
            "%s: auto-converting multi-dot reference '%s' to '%s'. "
            "Use '__' for join paths in SQL snippets (e.g., '%s').",
            context, ref, fixed, fixed,
        )
        result = result.replace(ref, fixed)
    return result


class Column(BaseModel):
    """A row-level column on a model.

    Carries the metadata needed to use the column either as a GROUP BY key
    (a "dimension") or as the input to an aggregation (a "measure"). What it's
    used as is decided per-query, gated by data type and ``allowed_aggregations``.

    Replaces v1 ``Dimension`` and ``Measure`` (which were merged in v2).
    """
    name: str
    sql: str | None = None
    type: DataType = DataType.TEXT
    primary_key: bool = False
    description: str | None = None
    label: str | None = None
    hidden: bool = False
    format: NumberFormat | None = None
    allowed_aggregations: list[str] | None = None
    filter: str | None = None  # Applied inside CASE WHEN at aggregation time only
    meta: dict[str, Any] | None = None
    sampled: str | None = None  # DEV-1375: cached sample-value snapshot
    sampled_values: list[str] | None = None  # DEV-1480: structured top-N
    distinct_count: int | None = None  # DEV-1480: true cardinality at profile time

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_type(cls, data: Any) -> Any:
        # DEV-1361: absorb legacy lowercase ``type`` strings ("string",
        # "number", "integer", "time", "date", "boolean") and drop pseudo-
        # type values ("count"/"sum"/...) so older agent input keeps working.
        if isinstance(data, dict) and "type" in data:
            mapped = _coerce_legacy_datatype(data["type"])
            if mapped is None:
                data = {k: v for k, v in data.items() if k != "type"}
            elif mapped is not data["type"]:
                data = {**data, "type": mapped}
        return data

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _validate_column_name(v, "Column")

    @field_validator("sql")
    @classmethod
    def _fix_multidot_sql(cls, v: str | None) -> str | None:
        if v is not None:
            v = _fix_multidot_sql(v, context="Column sql")
        return v

    @field_validator("filter")
    @classmethod
    def _fix_multidot_filter(cls, v: str | None) -> str | None:
        if v is not None:
            v = _fix_multidot_sql(v, context="Column filter")
            # DEV-1369: Column.filter is SQL-mode — validate at construction
            # time so DSL constructs (aggregation colon, transform calls) are
            # caught early. Result is discarded; we only care about the
            # side effect of raising on a violation.
            parse_sql_predicate(v)
        return v


class ModelMeasure(BaseModel):
    """A named formula on a model (or a query-level computed measure).

    A formula is a string that evaluates to an aggregated value: a column-with-
    aggregation reference (``"revenue:sum"``), arithmetic over such references
    (``"revenue:sum / *:count"``), a transform call (``"cumsum(revenue:sum)"``),
    or a bare reference to another ``ModelMeasure`` by name. See
    ``slayer/core/formula.py`` for full grammar.

    Stored in ``SlayerModel.measures`` for reuse, and in ``SlayerQuery.measures``
    for inline / query-specific definitions. The shape is identical in both
    contexts; the difference is scope.
    """
    formula: str
    name: str | None = None
    label: str | None = None
    description: str | None = None
    type: DataType | None = None
    meta: dict[str, Any] | None = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_type(cls, data: Any) -> Any:
        # DEV-1361: ``type`` declares the formula's result data type for outer
        # CAST emission at aggregation time. Legacy lowercase strings get
        # mapped to canonical values; pseudo-types drop to None.
        if isinstance(data, dict) and "type" in data:
            mapped = _coerce_legacy_datatype(data["type"])
            if mapped is None:
                data = {k: v for k, v in data.items() if k != "type"}
            elif mapped is not data["type"]:
                data = {**data, "type": mapped}
        return data

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        if v is not None and not _NAME_PATTERN.match(v):
            raise ValueError(
                f"Invalid name '{v}': must contain only letters, digits, "
                f"and underscores, and start with a letter or underscore"
            )
        return v

    @field_validator("name")
    @classmethod
    def _reject_transform_shadowing(cls, v: str | None) -> str | None:
        """A saved measure named after a built-in transform (``cumsum`` etc.)
        would shadow the transform when written as ``cumsum(...)`` in another
        formula. Reject these names at construction time.
        """
        if v is None:
            return v
        if v in ALL_TRANSFORMS:
            raise ValueError(
                f"ModelMeasure name '{v}' is a reserved transform name. "
                f"Reserved: {', '.join(sorted(ALL_TRANSFORMS))}"
            )
        return v

    @field_validator("formula")
    @classmethod
    def _reject_raw_window_function(cls, v: str) -> str:
        """DEV-1336: a measure formula containing raw ``OVER (...)`` SQL cannot
        be parsed by SLayer's formula grammar (Python AST rejects ``OVER`` as a
        keyword) and produces invalid SQL on every dialect if used as a filter.
        Reject at construction time with an actionable error.
        """
        if has_window_function(v):
            raise ValueError(f"ModelMeasure formula '{v}' {WINDOW_IN_FILTER_ERROR}")
        return v

    # DEV-1369: ModelMeasure formulas may legitimately contain ``__`` —
    # virtual-model columns produced by ``_query_as_model`` flatten join
    # paths into names like ``kpis__total_amount_sum``, which downstream
    # stages reference directly. Strict resolution at enrichment time
    # catches typos like ``customers__region`` that don't resolve to any
    # Column on the model.


class AggregationParam(BaseModel):
    """A named parameter for an aggregation formula."""
    name: str
    sql: str  # default value — column name or SQL expression


class Aggregation(BaseModel):
    """A named aggregation, either overriding a built-in or fully custom.

    For built-in overrides (e.g., setting default weight for weighted_avg),
    ``formula`` may be omitted — the built-in formula is used.
    For fully custom aggregations, ``formula`` is required.
    """
    name: str
    formula: str | None = None  # SQL template; None = use built-in formula
    params: list[AggregationParam] = Field(default_factory=list)
    description: str | None = None
    meta: dict[str, Any] | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        # DEV-1567: same identifier rule as Column.name / ModelMeasure.name.
        # The pg-facade catalog flattens cross-model entries with a dotted
        # prefix; the local/cross-model split would misclassify a dotted
        # Aggregation.name as cross-model.
        if not _NAME_PATTERN.match(v):
            raise ValueError(
                f"Invalid name '{v}': must contain only letters, digits, "
                f"and underscores, and start with a letter or underscore"
            )
        return v

    @model_validator(mode="after")
    def _require_formula_for_custom(self) -> "Aggregation":
        if self.name not in BUILTIN_AGGREGATIONS and self.formula is None:
            raise ValueError(
                f"Aggregation '{self.name}' is not a built-in aggregation; "
                f"a 'formula' is required. Built-in aggregations: "
                f"{', '.join(sorted(BUILTIN_AGGREGATIONS))}"
            )
        return self

    @model_validator(mode="after")
    def _reject_transform_names(self) -> "Aggregation":
        from slayer.core.formula import ALL_TRANSFORMS
        # Names that are ONLY transforms (not also built-in aggregations) are
        # forbidden as custom aggregation names to avoid ambiguity with the
        # formula parser's transform detection.
        transform_only = ALL_TRANSFORMS - BUILTIN_AGGREGATIONS
        if self.name in transform_only:
            raise ValueError(
                f"Aggregation name '{self.name}' conflicts with a built-in "
                f"transform function. Reserved names: "
                f"{', '.join(sorted(transform_only))}"
            )
        return self


def _coerce_source_queries(v: Any) -> Any:
    """Parse source_queries entries: dicts → SlayerQuery instances.

    Imports SlayerQuery lazily to avoid the slayer.core.models ↔
    slayer.core.query import cycle (query.py imports ModelMeasure from
    models.py).

    Raises ``ValueError`` (not ``TypeError``) for bad input so Pydantic v2
    wraps it into a ``ValidationError`` — required for REST/MCP/CLI callers
    to get structured error responses instead of raw tracebacks.
    """
    if v is None:
        return v
    if not isinstance(v, list):
        raise ValueError(f"source_queries must be a list, got {type(v).__name__}")
    from slayer.core.query import SlayerQuery
    result = []
    for i, item in enumerate(v):
        if isinstance(item, SlayerQuery):
            result.append(item)
        elif isinstance(item, dict):
            result.append(SlayerQuery.model_validate(item))
        else:
            raise ValueError(
                f"source_queries[{i}] must be a SlayerQuery or dict, "
                f"got {type(item).__name__}"
            )
    return result


class SourceModelOrigin(BaseModel):
    """Lineage breadcrumb for virtual stage models produced by
    ``_query_as_model``. Records the chain of upstream source models so
    outer-stage dotted-ref lookup can strip the right prefix and find
    the flat column in the wrapped subquery's projection (DEV-1449).

    Linked-list shape: each virtual model points at its direct
    upstream's name; walking ``parent`` reaches the original
    table-backed (or sql-backed) root.

    ``agg_column_names`` (Codex review on PR #137 round 9) records the
    flat names of columns on this stage that came from
    ``cross_model_measures`` or aggregated ``measures`` in the inner
    query's enrichment — i.e. the columns the cross-stage intercept
    is safe to re-aggregate. Without this provenance, a user-defined
    dimension whose name happens to look like an aggregation canonical
    (e.g. ``customers__revenue_sum``) would be silently re-summed by
    the intercept. Empty by default; only ``_query_as_model``
    populates it.

    The field is in-memory only — virtual stage models are not
    persisted, and `SlayerModel.source_model_origin` carries
    ``exclude=True`` so accidental save paths drop it cleanly.
    """
    name: str
    data_source: str | None = None
    parent: Optional["SourceModelOrigin"] = None
    agg_column_names: frozenset[str] = Field(default_factory=frozenset)


class ModelJoin(BaseModel):
    """A join relationship to another model."""
    target_model: str                               # Name of the joined model
    join_pairs: list[list[str]] = Field(...)        # [["source_dim", "target_dim"], ...]
    join_type: JoinType = JoinType.LEFT             # LEFT (default) or INNER

    @field_validator("join_pairs")
    @classmethod
    def _validate_join_pairs(cls, v: list[list[str]]) -> list[list[str]]:
        if not v:
            raise ValueError("join_pairs must be non-empty")
        for i, pair in enumerate(v):
            if len(pair) != 2 or not all(isinstance(s, str) and s for s in pair):
                raise ValueError(
                    f"join_pairs[{i}] must be [source_dim, target_dim] with non-empty strings, got {pair}"
                )
        return v


class SlayerModel(BaseModel):
    version: int = 7
    name: str
    sql_table: str | None = None
    sql: str | None = None
    source_queries: Annotated[
        list | None, BeforeValidator(_coerce_source_queries)
    ] = None  # List of SlayerQuery — query-backed source mode
    query_variables: dict[str, Any] = Field(default_factory=dict)
    backing_query_sql: str | None = None
    data_source: str = ""
    columns: list[Column] = Field(default_factory=list)
    measures: list[ModelMeasure] = Field(default_factory=list)
    aggregations: list[Aggregation] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _apply_schema_migrations(cls, data: Any) -> Any:
        return _migrate_schema(entity="SlayerModel", data=data)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _validate_model_name(v, "Model")

    @field_validator("data_source")
    @classmethod
    def _validate_data_source_format(cls, v: str) -> str:
        # Format-only checks (run on every input). Emptiness is enforced
        # in ``_require_data_source_unless_query_backed`` below so
        # query-backed models can be constructed before their cache
        # populator fills in ``data_source`` from the resolved virtual
        # model. Whitespace-strip mismatch and substring rules mirror
        # ``DatasourceConfig.name`` so the two canonical-id ingress
        # points share validation logic via the shared ``_NO_*`` rules.
        if not v:
            return v
        if v.strip() != v:
            raise ValueError(
                f"Model 'data_source' must not have leading/trailing "
                f"whitespace; got {v!r}."
            )
        label = "Model 'data_source'"
        _NO_NUL.check(name=v, context=label)
        _NO_FWD_SLASH.check(name=v, context=label)
        _NO_BACK_SLASH.check(name=v, context=label)
        _NO_DOT.check(name=v, context=label)
        _NO_COLON.check(name=v, context=label)
        return v

    @model_validator(mode="after")
    def _require_data_source_unless_query_backed(self) -> "SlayerModel":
        # Table-backed models (sql_table / sql) must have data_source up
        # front — it's part of the v4 storage key. Query-backed models are
        # allowed to start with an empty data_source because
        # ``engine._validate_and_populate_cache`` fills it in from the
        # resolved virtual model, and ``engine.save_model`` re-runs the
        # check before persisting.
        if not self.data_source.strip() and not self.source_queries:
            raise ValueError(
                f"Model '{self.name}': 'data_source' must be a non-empty "
                f"string. Set it to the name of the DatasourceConfig the "
                f"model belongs to."
            )
        return self
    joins: list[ModelJoin] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)  # Model-level filters (always applied)
    default_time_dimension: str | None = None
    description: str | None = None
    hidden: bool = False
    meta: dict[str, Any] | None = None
    # DEV-1449: in-memory breadcrumb for virtual stage models produced by
    # ``_query_as_model``. ``exclude=True`` keeps it out of YAML/SQLite
    # roundtrips; virtual stage models are not persisted in the first place.
    source_model_origin: SourceModelOrigin | None = Field(default=None, exclude=True)

    @field_validator("filters")
    @classmethod
    def _fix_multidot_filters(cls, v: list[str]) -> list[str]:
        """Auto-convert multi-dot column references in model filters and
        validate each entry as a SQL-mode predicate (DEV-1369).

        Model filters are SQL snippets: joined column references use the
        ``__`` alias syntax (``customers__regions.name``), not the
        multi-dot query syntax (``customers.regions.name``). Single-dot
        references like ``customers.name`` (table.column) are left as-is.

        After the multi-dot rewrite each entry is parsed with
        :func:`parse_sql_predicate` so DSL constructs (aggregation colon,
        transform calls, raw OVER) are caught at construction time.
        """
        rewritten = [_fix_multidot_sql(f, context="Model filter") for f in v]
        for f in rewritten:
            parse_sql_predicate(f)
        return rewritten

    @model_validator(mode="after")
    def _validate_column_measure_disjoint(self) -> "SlayerModel":
        """Names within ``columns`` and within ``measures`` must each be unique,
        and the two lists must not overlap.

        A query formula like ``{"formula": "revenue"}`` resolves by looking up
        the name in both lists; allowing duplicates within a list or collisions
        across lists would make resolution ambiguous.
        """
        col_names_seq = [c.name for c in self.columns]
        col_dupes = sorted({n for n in col_names_seq if col_names_seq.count(n) > 1})
        if col_dupes:
            raise ValueError(
                f"Model '{self.name}': duplicate column names: {col_dupes}. "
                f"Each column name must be unique within a model."
            )
        unnamed = [m.formula for m in self.measures if m.name is None]
        if unnamed:
            raise ValueError(
                f"Model '{self.name}': every ModelMeasure in 'measures' must "
                f"have a name. Unnamed formulas: {unnamed}."
            )
        measure_names_seq = [m.name for m in self.measures if m.name is not None]
        measure_dupes = sorted({n for n in measure_names_seq if measure_names_seq.count(n) > 1})
        if measure_dupes:
            raise ValueError(
                f"Model '{self.name}': duplicate measure names: {measure_dupes}. "
                f"Each named ModelMeasure must have a unique name within a model."
            )
        col_names = set(col_names_seq)
        measure_names = set(measure_names_seq)
        overlap = sorted(col_names & measure_names)
        if overlap:
            raise ValueError(
                f"Model '{self.name}': name collision between columns and "
                f"measures: {overlap}. Each name must be unique within a model "
                f"(columns and measures share a namespace)."
            )
        return self

    @model_validator(mode="after")
    def _validate_allowed_aggregations(self) -> "SlayerModel":
        """Enforce the intersection contract on ``Column.allowed_aggregations``.

        A whitelist entry is accepted iff:

        1. It is a known aggregation name (built-in or custom on this model).
        2. **PK rule** — if the column is a primary key, the entry must be in
           ``PRIMARY_KEY_AGGREGATIONS`` (``count`` / ``count_distinct`` only),
           regardless of type or whether the entry is a custom aggregation.
        3. **Type-default rule** — for non-PK columns, built-in aggregations
           must be eligible under ``DEFAULT_AGGREGATIONS_BY_TYPE`` for the
           column's type. Custom aggregations bypass this check (their formula
           determines applicability).

        Together these turn the whitelist into a guaranteed subset of the
        type/PK eligibility set, so query-time gating reduces to a whitelist
        membership check.
        """
        from slayer.core.enums import (
            DEFAULT_AGGREGATIONS_BY_TYPE,
            PRIMARY_KEY_AGGREGATIONS,
        )

        custom_agg_names = {a.name for a in self.aggregations}
        valid_names = BUILTIN_AGGREGATIONS | custom_agg_names
        for c in self.columns:
            if c.allowed_aggregations is None:
                continue
            for agg_name in c.allowed_aggregations:
                if agg_name not in valid_names:
                    raise ValueError(
                        f"Column '{c.name}': allowed_aggregations contains "
                        f"'{agg_name}', which is not a built-in aggregation "
                        f"or defined in this model's aggregations. "
                        f"Valid: {sorted(valid_names)}"
                    )
                if c.primary_key:
                    if agg_name not in PRIMARY_KEY_AGGREGATIONS:
                        raise ValueError(
                            f"Column '{c.name}': '{agg_name}' is not allowed "
                            f"on a primary-key column. PK columns can only be "
                            f"aggregated with {sorted(PRIMARY_KEY_AGGREGATIONS)}."
                        )
                    continue
                if agg_name in custom_agg_names and agg_name not in BUILTIN_AGGREGATIONS:
                    # Custom aggregations are exempt from type-default eligibility;
                    # the formula determines applicability. Built-in name overrides
                    # (e.g., a model-defined ``sum``) keep their type semantics.
                    continue
                allowed_for_type = DEFAULT_AGGREGATIONS_BY_TYPE.get(
                    c.type, frozenset()
                )
                if agg_name not in allowed_for_type:
                    raise ValueError(
                        f"Column '{c.name}': aggregation '{agg_name}' is not "
                        f"applicable to {c.type} columns. allowed_aggregations "
                        f"must be a subset of the type-default set "
                        f"{sorted(allowed_for_type)} (plus any custom "
                        f"aggregations defined on this model)."
                    )
        return self

    @model_validator(mode="after")
    def _validate_source_mode_exclusivity(self) -> "SlayerModel":
        """Exactly one of sql_table, sql, source_queries must be populated.

        Empty source_queries=[] is rejected with a specific message. None / 0
        / 2+ populated source modes raise a generic exclusivity error listing
        which modes were set.
        """
        if self.source_queries is not None and len(self.source_queries) == 0:
            raise ValueError(
                f"Model '{self.name}': source_queries cannot be an empty list. "
                f"Provide one or more stages, or omit the field entirely."
            )
        populated = []
        if self.sql_table:
            populated.append("sql_table")
        if self.sql:
            populated.append("sql")
        if self.source_queries:
            populated.append("source_queries")
        if len(populated) == 0:
            raise ValueError(
                f"Model '{self.name}' must specify exactly one source: "
                f"sql_table, sql, or source_queries (none specified)."
            )
        if len(populated) > 1:
            raise ValueError(
                f"Model '{self.name}' must specify exactly one source: "
                f"sql_table, sql, or source_queries (got: {populated})."
            )
        return self

    # NOSONAR S3516 — Pydantic v2 @model_validator(mode="after") is required to
    # return ``self``; the rule's "always returns same value" warning doesn't
    # apply to validator methods.
    @model_validator(mode="after")
    def _validate_source_query_stages(self) -> "SlayerModel":
        """Validate stage-name rules on source_queries.

        - All non-final stages must have a non-empty name (so later stages
          and the outer query can reference them by name).
        - Stage names (across all stages, not just non-final) must be unique.
        """
        if not self.source_queries:
            return self
        stages = self.source_queries
        if len(stages) > 1:
            for i, stage in enumerate(stages[:-1]):
                if not getattr(stage, "name", None):
                    raise ValueError(
                        f"Model '{self.name}': non-final stage at index {i} "
                        f"in source_queries must have a 'name'."
                    )
        seen: set = set()
        dupes: list[str] = []
        for stage in stages:
            n = getattr(stage, "name", None)
            if not n:
                continue
            if n in seen and n not in dupes:
                dupes.append(n)
            seen.add(n)
        if dupes:
            raise ValueError(
                f"Model '{self.name}': duplicate stage name(s) in "
                f"source_queries: {sorted(dupes)}."
            )
        return self

    def get_column(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    def get_measure(self, name: str) -> ModelMeasure | None:
        for m in self.measures:
            if m.name == name:
                return m
        return None

    def get_aggregation(self, name: str) -> Aggregation | None:
        for a in self.aggregations:
            if a.name == name:
                return a
        return None


class DatasourceConfig(BaseModel):
    version: int = 2
    name: str
    type: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None
    connection_string: str | None = None
    schema_name: str | None = None
    # ``schema_name`` (above) is the UPSTREAM physical schema this datasource
    # reads from (e.g. a Snowflake/Postgres source schema). ``postgres_schema``
    # is unrelated: it's the schema name the Postgres *facade* advertises this
    # datasource's models under. Default ``None`` => "public", so by default
    # every datasource's models share one schema; set it to separate them.
    postgres_schema: str | None = None
    description: str | None = None
    # Snowflake-specific (v2, DEV-1551). Other dialects ignore them.
    # ``connection_name`` is the primary auth path — when set, all credentials
    # come from ``~/.snowflake/connections.toml`` via
    # ``snowflake.connector.connect(connection_name=...)``. Inline form
    # (host as account, username, password, database, schema_name) is the
    # secondary path; ``warehouse`` and ``role`` populate the URL's query
    # string for that path. See docs/configuration/datasources.md#snowflake.
    connection_name: str | None = None
    warehouse: str | None = None
    role: str | None = None
    # BigQuery-specific. Other dialects ignore it. The contents of a Google
    # service-account key file as a JSON string. When set, the BigQuery dialect
    # constructs the SQLAlchemy engine with ``credentials_info=json.loads(...)``
    # so the connection authenticates against that service account directly.
    # When unset, BigQuery falls back to Application Default Credentials
    # (``GOOGLE_APPLICATION_CREDENTIALS`` env var or attached compute identity).
    credentials_json: str | None = Field(default=None, repr=False)

    @model_validator(mode="before")
    @classmethod
    def _apply_schema_migrations_and_aliases(cls, data: Any) -> Any:
        data = _migrate_schema(entity="DatasourceConfig", data=data)
        if isinstance(data, dict) and "user" in data and "username" not in data:
            data["username"] = data.pop("user")
        return data

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        # Datasource names are the leading segment of every canonical-id
        # (``<ds>``, ``<ds>.<model>``, ``<ds>.<model>.<leaf>``) and a
        # path component in YAML storage (``datasources/<name>.yaml``,
        # ``models/<name>/...``). The substring rules are shared with
        # ``SlayerModel.data_source`` via the module-level ``_NO_*``
        # rules so the rationale lives in one place.
        #
        # ``__`` is intentionally NOT rejected: datasource names never
        # become SQL table aliases, so the join-path-alias reservation
        # that applies to model and query names doesn't apply here.
        label = "Datasource 'name'"
        _require_non_empty_trimmed(v=v, context=label)
        _NO_NUL.check(name=v, context=label)
        _NO_FWD_SLASH.check(name=v, context=label)
        _NO_BACK_SLASH.check(name=v, context=label)
        _NO_DOT.check(name=v, context=label)
        _NO_COLON.check(name=v, context=label)
        return v

    @field_validator("postgres_schema")
    @classmethod
    def _validate_postgres_schema(cls, v: str | None) -> str | None:
        # The facade advertises models under this schema, so it must be a
        # plain unquoted Postgres identifier. Postgres folds unquoted
        # identifiers to lowercase, so we require lowercase to avoid the
        # quoting ambiguity that would otherwise surprise BI tools.
        if v is None:
            return v
        _require_non_empty_trimmed(v=v, context="Datasource 'postgres_schema'")
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", v):
            raise ValueError(
                f"Datasource 'postgres_schema' must be a lowercase Postgres "
                f"identifier matching [a-z_][a-z0-9_]*, got {v!r}"
            )
        return v

    def _get_tsql_connection_string(self) -> str:
        return _SA_URL.create(
            "mssql+pyodbc",
            username=self.username or None,
            password=self.password or None,
            host=self.host or "localhost",
            port=self.port,
            database=self.database or "",
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "TrustServerCertificate": "yes",
            },
        ).render_as_string(hide_password=False)

    def get_connection_string(self) -> str:
        if self.connection_string:
            return self.connection_string
        # Dialect-specific hook (DEV-1551): SnowflakeDialect builds the
        # sentinel ``snowflake://?connection_name=<name>`` URL or the
        # full snowflake-sqlalchemy URL from inline fields. Tier-1
        # dialects without a custom hook return None and fall through
        # to the standard branches below.
        from slayer.sql.dialects import dialect_for_ds_type  # noqa: PLC0415
        dialect = dialect_for_ds_type(self.type)
        url_from_dialect = dialect.build_connection_url(self)
        if url_from_dialect is not None:
            return str(url_from_dialect)
        if self.type in ("sqlite", "duckdb"):
            return f"{self.type}:///{self.database}"
        if self.type in ("mssql", "sqlserver", "tsql"):
            return self._get_tsql_connection_string()
        driver_map = {
            "postgres": "postgresql",
            "postgresql": "postgresql",
            "mysql": "mysql+pymysql",
            "mariadb": "mysql+pymysql",
            "clickhouse": "clickhouse+http",
        }
        driver = driver_map.get(self.type, self.type)
        auth = ""
        if self.username:
            auth = self.username
            if self.password:
                auth += f":{self.password}"
            auth += "@"
        host_port = self.host or "localhost"
        if self.port:
            host_port += f":{self.port}"
        db = self.database or ""
        return f"{driver}://{auth}{host_port}/{db}"

    def resolve_env_vars(self) -> "DatasourceConfig":
        data = self.model_dump()
        unresolved = []
        for key, value in data.items():
            if isinstance(value, str):
                resolved = _resolve_env_string(value)
                data[key] = resolved
                for match in re.finditer(r"\$\{(\w+)\}", resolved):
                    unresolved.append(match.group(1))
        if unresolved:
            raise ValueError(
                f"Datasource '{self.name}': unresolved environment variable(s): "
                f"{', '.join(unresolved)}"
            )
        return DatasourceConfig(**data)


def _resolve_env_string(value: str) -> str:
    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))

    return re.sub(r"\$\{(\w+)\}", replacer, value)
