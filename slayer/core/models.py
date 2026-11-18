"""Core domain models for SLayer."""

import logging
import os
import re
from typing import TYPE_CHECKING, AbstractSet, Annotated, Any, Optional, Sequence

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ModelWrapValidatorHandler,
    PrivateAttr,
    field_validator,
    model_validator,
)
from sqlalchemy.engine import URL as _SA_URL
from sqlglot.errors import SqlglotError
from sqlglot.tokenizer_core import TokenType
from sqlglot.tokens import Tokenizer
from typing_extensions import deprecated

from slayer.core.enums import BUILTIN_AGGREGATION_EXPRESSIONS, BUILTIN_AGGREGATIONS, DEFAULT_AGGREGATIONS_BY_TYPE, GRANULARITY_NAMES, LIST_AGGREGATIONS, DataType, type_display, RelationshipCardinality, JoinType, ObjectKind, PRIMARY_KEY_AGGREGATIONS, RANKED_AGGREGATIONS, RESERVED_DATE_SYNTAX_NAMES, RESERVED_NAMES, _coerce_legacy_datatype, builtin_empty_value, ALL_TRANSFORMS
from slayer.core.errors import AmbiguousAggregationError, AmbiguousReferenceError, FieldSqlStatementError, GranularityDefinitionError, RelationshipKeyError
from slayer.core.names import folded_collisions, match_name
from slayer.core.vocabulary import AGGREGATION_KEYS, DATASET_KEYS, FIELD_KEYS, METRIC_KEYS, RELATIONSHIP_KEYS, SOURCE_ORIGIN_KEYS, attribute_used, converted_document, method_twin, name_twins, renamed_input, same_ignoring_deprecations
from slayer.core.warnings import DeprecatedInputWarning, NameCollisionWarning
from slayer.core.format import NumberFormat
from slayer.core.granularity import CustomGranularity, GranularitySpec, definition_error, granularity_key
from slayer.core.keys import RESERVED_SCALAR_NAMES
from slayer.core.refs import IDENTIFIER_RE
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.sql_predicate import parse_sql_predicate
from slayer.sql.window_detect import WINDOW_IN_FILTER_ERROR, has_window_function
from slayer.storage.migrations import CURRENT_VERSIONS, in_stored_document, migrate as _migrate_schema, migrating_document

if TYPE_CHECKING:
    from slayer.core.query import SlayerQuery

    _SourceQueries = list[SlayerQuery] | None
else:
    _SourceQueries = list | None  # items are SlayerQuery (``_coerce_source_queries``); the import is circular

_NAME_PATTERN = re.compile(r"^[a-zA-Z_]\w*$", re.ASCII)

logger = logging.getLogger(__name__)

# Split an embedded port out of the host field for ``URL.create``.
# ``[<ipv6>]`` or ``[<ipv6>]:<port>`` — bracketed IPv6, optional port.
_BRACKETED_HOST_RE = re.compile(r"^\[(.+)\](?::(\d+))?$")
# ``<host>:<numeric-port>`` — single-colon numeric-tail host only.
_HOST_EMBEDDED_PORT_RE = re.compile(r"^([^:\[]+):(\d+)$")


class _SubstringRule:
    """A forbidden substring inside a name, paired with its rejection rationale."""

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


# ``__`` is allowed in names; only the internal ``__slayer_`` prefix
# is reserved, so user input cannot spoof the parser's placeholder identifiers.
_RESERVED_NAME_PREFIX = "__slayer_"


def _reject_reserved_prefix(name: str, label: str) -> None:
    """Reject the SLayer-internal ``__slayer_`` name prefix (P3)."""
    if name.startswith(_RESERVED_NAME_PREFIX):
        raise ValueError(
            f"{label} '{name}' must not start with {_RESERVED_NAME_PREFIX!r}; "
            f"that prefix is reserved for SLayer-internal identifiers."
        )


_NO_DOT = _SubstringRule(
    substring=".",
    reason="dots are the canonical-id namespace delimiter "
           "(``<ds>.<dataset>.<leaf>``) and the dotted-path reference "
           "syntax in queries.",
)
_NO_COLON = _SubstringRule(
    substring=":",
    reason="colons are reserved as the legacy aggregation separator "
           "and the ``memory:<int>`` canonical-id prefix.",
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
    """Reject empty/whitespace-only inputs and leading/trailing whitespace."""
    if not v or not v.strip():
        raise ValueError(
            f"{context} must be a non-empty string; got {v!r}."
        )
    if v.strip() != v:
        raise ValueError(
            f"{context} must not have leading/trailing whitespace; "
            f"got {v!r}."
        )


def _validate_dataset_name(name: str, context: str) -> str:
    """Reject dataset/query names containing ``.``/``:`` or the ``__slayer_`` prefix."""
    label = f"{context} name"
    _NO_DOT.check(name=name, context=label)
    _NO_COLON.check(name=name, context=label)
    _reject_reserved_prefix(name, label)
    return name


_DUNDER_RUN_RE = re.compile(r"_{2,}")


def sanitize_dataset_name(name: str) -> str:
    """Collapse runs of 2+ underscores to one (regex handles overlaps; the fallback match key for re-ingest)."""
    return _DUNDER_RUN_RE.sub("_", name)


def _validate_field_name(name: str, context: str) -> str:
    """Reject dimension/metric names containing ``.`` or ``:`` (``__`` allowed for flattened relationship paths)."""
    label = f"{context} name"
    _NO_DOT.check(name=name, context=label)
    _NO_COLON.check(name=name, context=label)
    _reject_reserved_prefix(name, label)
    return name


def _bare_identifier(sql: str) -> str | None:
    """The identifier ``sql`` names — bare or double-quoted — else ``None``."""
    text = sql.strip()
    if len(text) > 1 and text[0] == text[-1] == '"':
        text = text[1:-1]
    return text if IDENTIFIER_RE.match(text) else None


def is_base_field_sql(sql: str | None) -> bool:
    """A field ``sql`` that names one physical column (unset, bare or double-quoted identifier)."""
    return sql is None or _bare_identifier(sql) is not None


def physical_column_sql(*, sql: str | None, name: str) -> str:
    """The unquoted physical identifier of a base field, else ``name``."""
    return (_bare_identifier(sql) if sql is not None else None) or name


def check_element_type(*, name: str, data_type: DataType, element_type: DataType | None) -> None:
    """``element_type`` is rejected off a list and is never itself a list (the dataset requires it on one)."""
    if data_type is not DataType.ARRAY and element_type is not None:
        raise ValueError(
            f"DatasetField {name!r} declares element_type {element_type.value} but its type is {data_type.value}; "
            f"only an ARRAY field has an element_type."
        )
    if element_type is DataType.ARRAY:
        raise ValueError(
            f"DatasetField {name!r}: element_type ARRAY is not supported; a nested list's elements are UNKNOWN."
        )


def _coerce_legacy_type_field(data: Any) -> Any:
    """Map a legacy lowercase ``datatype`` string in a raw payload; drop pseudo-types."""
    if isinstance(data, dict) and "datatype" in data:
        mapped = _coerce_legacy_datatype(data["datatype"])
        if mapped is None:
            data = {k: v for k, v in data.items() if k != "datatype"}
        elif mapped is not data["datatype"]:
            data = {**data, "datatype": mapped}
    return data


#: Words opening a statement rather than an expression (a following ``.`` makes the word a qualifier instead).
_STATEMENT_WORDS = frozenset({
    "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "REPLACE", "DROP", "CREATE", "ALTER", "GRANT",
    "REVOKE", "TRUNCATE", "COPY", "SET", "USE", "CALL", "EXEC", "EXECUTE", "PRAGMA", "VALUES", "SHOW", "DESCRIBE",
    "EXPLAIN", "BEGIN", "COMMIT", "ROLLBACK", "ANALYZE", "ATTACH", "DETACH", "VACUUM", "LOAD", "CACHE", "UNCACHE",
    "REFRESH", "KILL", "DECLARE", "TABLE",
})


def _check_one_expression(*, field: str, sql: str) -> None:
    """Refuse a statement, or several, as ``sql``; text the generic tokenizer cannot read is left to the dialect."""
    try:
        tokens = Tokenizer().tokenize(sql)
    except (SqlglotError, ValueError):
        return
    while tokens and tokens[-1].token_type is TokenType.SEMICOLON:
        tokens = tokens[:-1]
    several = any(t.token_type is TokenType.SEMICOLON for t in tokens)
    opens_statement = len(tokens) > 1 and tokens[0].token_type is not TokenType.IDENTIFIER and (
        tokens[0].text.upper() in _STATEMENT_WORDS) and not (len(tokens) > 1 and tokens[1].token_type is TokenType.DOT)
    if several or opens_statement:
        raise FieldSqlStatementError(field=field, sql=sql)


class DatasetField(BaseModel):
    """A row-level field, usable per-query as a GROUP BY dimension or an aggregation metric."""
    name: str
    sql: str | None = None
    datatype: DataType = Field(
        default=DataType.TEXT,
        description="TEXT, INT, DOUBLE, BOOLEAN, DATE, TIMESTAMP, UNKNOWN (opaque), or ARRAY (a list).",
    )
    element_type: DataType | None = Field(
        default=None,
        description=(
            "A list's element type, any type but ARRAY; forbidden off ARRAY. Required on a table-backed "
            "dataset's list; an sql-mode dataset's undeclared one is UNKNOWN."
        ),
    )
    db_type: str | None = Field(
        default=None,
        description=(
            "Raw database type string (e.g. 'point', 'DECIMAL(18, 2)'), "
            "retained when the declared DataType loses information. Populated "
            "by ingestion for UNKNOWN (opaque), exact NUMERIC/DECIMAL, JSON, "
            "zoned-timestamp and list fields; None when the mapped type carries everything needed."
        ),
    )
    primary_key: bool = False
    unique: bool = False  # single-field uniqueness; a sole primary key implies it, a composite member does not
    description: str | None = None
    label: str | None = None
    hidden: bool = False
    format: NumberFormat | None = None
    granularity: GranularitySpec | None = Field(
        default=None,
        description=(
            "Time bucket the field's values are already truncated to: a built-in "
            "granularity or a custom granularity defined on the dataset's datasource. "
            "Query-backed datasets stamp it from the final stage; on a table-backed "
            "field set it by hand only when you are sure the values are bucketed "
            "at that granularity. A finer or non-nesting time dimension over the "
            "field is then a typed error."
        ),
    )
    allowed_aggregations: list[str] | None = None
    filter: str | None = None  # Desugars to CASE WHEN (filter) THEN (value) END in every position
    meta: dict[str, Any] | None = None
    sampled: str | None = None  # cached sample-value snapshot
    sampled_values: list[str] | None = None  # structured top-N
    distinct_count: int | None = None  # true cardinality at profile time
    # Runtime-only (never persisted): stale respellings of a generated query-backed field.
    _respellings: tuple[str, ...] = PrivateAttr(default=())

    @model_validator(mode="wrap")
    @classmethod
    def _old_spellings(cls, data: Any, handler: ModelWrapValidatorHandler["DatasetField"]) -> "DatasetField":
        return renamed_input(data, handler, FIELD_KEYS, stored=in_stored_document())

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_type(cls, data: Any) -> Any:
        return _coerce_legacy_type_field(data)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _validate_field_name(v, "Field")

    @field_validator("filter")
    @classmethod
    def _validate_filter_predicate(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            # SQL-mode: validate at construction so DSL constructs raise early.
            parse_sql_predicate(v)
        return v

    @model_validator(mode="after")
    def _validate_sql_expression(self) -> "DatasetField":
        if self.sql is not None:
            _check_one_expression(field=self.name, sql=self.sql)
        return self

    @model_validator(mode="after")
    def _validate_granularity_temporal(self) -> "DatasetField":
        # A bucket only makes sense on a temporal field (like filter/aggregations, fail at construction).
        if self.granularity is not None and self.datatype not in (DataType.DATE, DataType.TIMESTAMP):
            raise ValueError(
                f"DatasetField {self.name!r} declares granularity "
                f"'{self.granularity}' but its type is {self.datatype.value}; "
                f"a granularity is only valid on a temporal (DATE / TIMESTAMP) field."
            )
        return self

    @model_validator(mode="after")
    def _validate_element_type(self) -> "DatasetField":
        check_element_type(name=self.name, data_type=self.datatype, element_type=self.element_type)
        return self

    @property
    def _sql_is_nontrivial(self) -> bool:
        """The value SQL is not the bare self-name; a QUOTED self-name counts, since
        only the expansion door re-qualifies it with its quoting intact."""
        return self.sql is not None and self.sql.strip() != self.name

    @property
    def is_base(self) -> bool:
        return is_base_field_sql(self.sql)

    @property
    def physical_name(self) -> str:
        """The physical column this base field reads; raises on an expression field."""
        if not self.is_base:
            raise RelationshipKeyError(
                summary=f"field {self.name!r} is not a base field (sql={self.sql!r}) "
                        f"and has no physical spelling",
            )
        return physical_column_sql(sql=self.sql, name=self.name)

    @property
    def physical_quoted(self) -> bool:
        """Whether this base field's ``sql`` quotes its physical name (an exact-case name)."""
        text = (self.sql or "").strip()
        return self.is_base and len(text) > 1 and text[0] == text[-1] == '"'

    @property
    def needs_expansion(self) -> bool:
        """The reference resolves to more than a bare physical column — a derived
        ``sql`` or an attached ``filter`` — so its uses expand to that definition."""
        return self._sql_is_nontrivial or self.filter is not None

    @property
    def respellings(self) -> tuple[str, ...]:
        return self._respellings

    def with_respellings(self, respellings: tuple[str, ...]) -> "DatasetField":
        out = self.model_copy()
        out._respellings = tuple(respellings)
        return out

    @property
    @deprecated("type is deprecated; use datatype", category=None)
    def type(self) -> DataType:
        attribute_used(self, old="type", new="datatype")
        return self.datatype

    @type.setter
    @deprecated("type is deprecated; use datatype", category=None)
    def type(self, value: DataType) -> None:
        attribute_used(self, old="type", new="datatype")
        self.datatype = value


def list_operations_hint(field: DatasetField) -> str:
    """One line naming what a list field takes."""
    n = field.name
    return (f"list: contains({n}, x), contains_any / contains_all({n}, [...]), length({n}) (distinct elements); "
            f"it groups and compares as the set of its distinct elements; its elements are {n}.value")


def field_type_text(field: DatasetField) -> str:
    """``ARRAY<TEXT> (list: …)`` for a list field, else its type."""
    if field.datatype is not DataType.ARRAY:
        return str(field.datatype)
    return f"{type_display(field.datatype, field.element_type)} ({list_operations_hint(field)})"


def is_identifier(*, field: DatasetField, fields: list[DatasetField]) -> bool:
    """``field`` is its dataset's sole primary key (composite-key members are not identifiers)."""
    return field.primary_key and sum(1 for c in fields if c.primary_key) == 1


def is_key_set_unique(
    *, key_fields: list[str], unique_key_sets: list[list[str]]
) -> bool:
    """Is the ``key_fields`` tuple unique given the known unique key-sets?

    Unique iff some key-set is a non-empty SUBSET: unique ``(a)`` makes
    ``(a, b)`` unique, but unique ``(a, b)`` says nothing about ``(a)``.
    """
    key_set = set(key_fields)
    return any(uks and set(uks) <= key_set for uks in unique_key_sets)


def _check_allowed_aggregation(
    *, field: DatasetField, agg_name: str, identifier: bool,
    custom_agg_names: AbstractSet[str], valid_names: AbstractSet[str],
) -> None:
    """One ``allowed_aggregations`` entry must be known and eligible for the field's type / identifier role."""
    if agg_name not in valid_names:
        raise ValueError(
            f"DatasetField '{field.name}': allowed_aggregations contains "
            f"'{agg_name}', which is not a built-in aggregation "
            f"or defined in this dataset's aggregations. "
            f"Valid: {sorted(valid_names)}"
        )
    if identifier:
        if agg_name not in PRIMARY_KEY_AGGREGATIONS:
            raise ValueError(
                f"DatasetField '{field.name}': '{agg_name}' is not allowed "
                f"on a primary-key field. PK fields can only be "
                f"aggregated with {sorted(PRIMARY_KEY_AGGREGATIONS)}."
            )
        return
    if agg_name in custom_agg_names and agg_name not in BUILTIN_AGGREGATIONS:
        # Custom aggregations bypass type-default eligibility; the expression decides.
        return
    allowed_for_type = DEFAULT_AGGREGATIONS_BY_TYPE.get(field.datatype, frozenset())
    if agg_name not in allowed_for_type:
        raise ValueError(
            f"DatasetField '{field.name}': aggregation '{agg_name}' is not "
            f"applicable to {field.datatype} fields. allowed_aggregations "
            f"must be a subset of the type-default set "
            f"{sorted(allowed_for_type)} (plus any custom "
            f"aggregations defined on this dataset)."
        )


class DatasetMetric(BaseModel):
    """A named aggregation expression, inline in a query or saved on a dataset and referenced by bare name."""
    expression: str = Field(description="Same syntax as the query object's metrics.")
    name: str | None = Field(default=None, description="Result key; filters and order may use it in place of the expression.")
    label: str | None = None
    description: str | None = None
    datatype: DataType | None = None
    meta: dict[str, Any] | None = None
    # Pre-substitution expression; names derive from it so result keys never carry variable values.
    _template: str | None = PrivateAttr(default=None)
    # The effective variables a whole-``IN`` placeholder left unwritten by binding reads (``parse_bound_expr``).
    _bound_variables: dict[str, Any] = PrivateAttr(default_factory=dict)

    @property
    def template(self) -> str:
        """The expression as written, before ``{var}`` substitution."""
        return self._template or self.expression

    @property
    def bound_variables(self) -> dict[str, Any]:
        return self._bound_variables

    def substituted(self, expression: str, *, variables: dict[str, Any]) -> "DatasetMetric":
        """A copy carrying the substituted ``expression`` and its ``variables``, remembering its template."""
        out = self.model_copy(update={"expression": expression})
        out._template = self.template
        out._bound_variables = dict(variables)
        return out

    @model_validator(mode="wrap")
    @classmethod
    def _old_spellings(cls, data: Any, handler: ModelWrapValidatorHandler["DatasetMetric"]) -> "DatasetMetric":
        return renamed_input(data, handler, METRIC_KEYS, stored=in_stored_document())

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_type(cls, data: Any) -> Any:
        # ``datatype`` declares the expression's result type; an explicit null stays set.
        if isinstance(data, dict) and "datatype" in data and data["datatype"] is None:
            return data
        return _coerce_legacy_type_field(data)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        if v is not None and not _NAME_PATTERN.match(v):
            raise ValueError(
                f"Invalid name '{v}': must contain only letters, digits, "
                f"and underscores, and start with a letter or underscore"
            )
        if v is not None:
            _reject_reserved_prefix(v, "DatasetMetric name")
        return v

    @field_validator("expression")
    @classmethod
    def _reject_raw_window_function(cls, v: str) -> str:
        """Reject raw ``OVER (...)`` window SQL — unparseable by the expression grammar."""
        if has_window_function(v):
            raise ValueError(f"DatasetMetric expression '{v}' {WINDOW_IN_FILTER_ERROR}")
        return v

    # Expressions may contain ``__`` (flattened virtual-dataset relationship paths); typos are
    # caught by strict resolution at binding time.

    @property
    @deprecated("type is deprecated; use datatype", category=None)
    def type(self) -> DataType | None:
        attribute_used(self, old="type", new="datatype")
        return self.datatype

    @type.setter
    @deprecated("type is deprecated; use datatype", category=None)
    def type(self, value: DataType | None) -> None:
        attribute_used(self, old="type", new="datatype")
        self.datatype = value

    @property
    @deprecated("formula is deprecated; use expression", category=None)
    def formula(self) -> str:
        attribute_used(self, old="formula", new="expression")
        return self.expression

    @formula.setter
    @deprecated("formula is deprecated; use expression", category=None)
    def formula(self, value: str) -> None:
        attribute_used(self, old="formula", new="expression")
        self.expression = value


VALUE_PLACEHOLDER = "value"


def reserved_value_param_message(agg_name: str) -> str:
    return (
        f"Aggregation '{agg_name}': a parameter may not be named '{VALUE_PLACEHOLDER}'. "
        f"'{{{VALUE_PLACEHOLDER}}}' in an aggregation expression always stands for the "
        f"aggregated field, so such a parameter could never be used; rename it."
    )


WINDOW_PARAM = "window"


def reserved_window_param_message(agg_name: str) -> str:
    return (
        f"Aggregation '{agg_name}': a parameter or placeholder may not be named "
        f"'{WINDOW_PARAM}'. '{WINDOW_PARAM}' is the trailing-window argument "
        f"(e.g. {WINDOW_PARAM}='90d'), so such a parameter could never receive a value; rename it."
    )


class AggregationParam(BaseModel):
    """A named parameter for an aggregation expression."""
    name: str
    sql: str  # default value — field name or SQL expression

    @model_validator(mode="after")
    def _require_sql(self) -> "AggregationParam":
        if not self.sql.strip():
            raise ValueError(f"Aggregation parameter '{self.name}' has an empty sql default.")
        return self


class Aggregation(BaseModel):
    """A named aggregation overriding a built-in (``expression`` optional) or fully custom (``expression`` required)."""
    name: str
    expression: str | None = None  # SQL template; None = use built-in expression
    params: list[AggregationParam] = Field(default_factory=list)
    description: str | None = None
    meta: dict[str, Any] | None = None

    @model_validator(mode="wrap")
    @classmethod
    def _old_spellings(cls, data: Any, handler: ModelWrapValidatorHandler["Aggregation"]) -> "Aggregation":
        return renamed_input(data, handler, AGGREGATION_KEYS, stored=in_stored_document())

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        # Same identifier rule as DatasetField.name; a dotted name would misclassify in the pg-facade catalog.
        if not _NAME_PATTERN.match(v):
            raise ValueError(
                f"Invalid name '{v}': must contain only letters, digits, "
                f"and underscores, and start with a letter or underscore"
            )
        return v

    @model_validator(mode="after")
    def _require_expression_for_custom(self) -> "Aggregation":
        if self.name not in BUILTIN_AGGREGATIONS and self.expression is None:
            raise ValueError(
                f"Aggregation '{self.name}' is not a built-in aggregation; "
                f"an 'expression' is required. Built-in aggregations: "
                f"{', '.join(sorted(BUILTIN_AGGREGATIONS))}"
            )
        if self.expression is not None and not self.expression.strip():
            raise ValueError(f"Aggregation '{self.name}' has an empty expression.")
        if any(p.name == VALUE_PLACEHOLDER for p in self.params):
            raise ValueError(reserved_value_param_message(self.name))
        return self

    @model_validator(mode="after")
    def _reject_transform_names(self) -> "Aggregation":
        # Transform-only names are forbidden to avoid ambiguity with transform detection.
        transform_only = ALL_TRANSFORMS - BUILTIN_AGGREGATIONS
        if self.name in transform_only:
            raise ValueError(
                f"Aggregation name '{self.name}' conflicts with a built-in "
                f"transform function. Reserved names: "
                f"{', '.join(sorted(transform_only))}"
            )
        # A granularity name reads as a time bucket unit.
        if self.name.lower() in GRANULARITY_NAMES:
            raise ValueError(
                f"Aggregation name '{self.name}' conflicts with a time "
                f"granularity. Reserved granularity names: "
                f"{', '.join(sorted(GRANULARITY_NAMES))}"
            )
        if self.name.lower() in RESERVED_DATE_SYNTAX_NAMES:
            raise ValueError(
                f"Aggregation name '{self.name}' is a reserved date function or "
                f"date-part alias: {', '.join(sorted(RESERVED_DATE_SYNTAX_NAMES))}"
            )
        return self

    @property
    @deprecated("formula is deprecated; use expression", category=None)
    def formula(self) -> str | None:
        attribute_used(self, old="formula", new="expression")
        return self.expression

    @formula.setter
    @deprecated("formula is deprecated; use expression", category=None)
    def formula(self, value: str | None) -> None:
        attribute_used(self, old="formula", new="expression")
        self.expression = value


def aggregation_definition(*, owner: "SlayerDataset | None", agg: str) -> Aggregation | None:
    """``owner``'s declaration of ``agg``: an exact-case match, else the unique case-insensitive one (a built-in name
    matches only its exact lowercase definition); ``None`` when it declares none. Several case-insensitive matches
    and no exact one raise ``AmbiguousAggregationError``."""
    if owner is None:
        return None
    declared = owner.aggregations or []
    if agg.lower() in BUILTIN_AGGREGATIONS:
        return next((a for a in declared if a.name == agg.lower()), None)
    try:
        name = match_name(name=agg, declared=[a.name for a in declared], quoted=False)
    except AmbiguousReferenceError as e:
        raise AmbiguousAggregationError(dataset=owner.name, call=agg, candidates=e.candidates) from None
    return None if name is None else next(a for a in declared if a.name == name)


def shadows_library_function(aggregation: Aggregation) -> bool:
    """Whether ``aggregation`` takes a Mode-B function or function-spelling name."""
    return aggregation.name.lower() in RESERVED_SCALAR_NAMES


def builtin_semantics(definition: Aggregation | None) -> bool:
    """Whether a call keeps its built-in aggregation's semantics: its owner declares no expression for it."""
    return definition is None or definition.expression is None


class EmptyList(BaseModel):
    """The empty list of ``element_type`` elements (``None``: unknown) — ``collect``'s value over no rows."""

    model_config = ConfigDict(frozen=True)

    element_type: DataType | None = None


EmptyValue = int | EmptyList | None


def empty_value(*, agg: str, definition: Aggregation | None, element_type: DataType | None = None) -> EmptyValue:
    """``agg``'s value over no rows (``element_type``: a list aggregation's input type); a declared ``expression``
    overrides the built-in's."""
    if not builtin_semantics(definition):
        return None
    if agg in LIST_AGGREGATIONS:
        return EmptyList(element_type=element_type)
    return builtin_empty_value(agg)


def rendered_expression(*, agg: str, definition: Aggregation | None) -> str | None:
    """The template ``agg`` renders through (a definition's override wins); ``None``: a built-in builder."""
    if agg in RANKED_AGGREGATIONS and builtin_semantics(definition):
        return None
    return (definition.expression if definition else None) or BUILTIN_AGGREGATION_EXPRESSIONS.get(agg)


def _coerce_source_queries(v: Any) -> Any:
    """Parse source_queries dicts → SlayerQuery (lazy import breaks a cycle; raises ValueError for Pydantic)."""
    if v is None:
        return v
    if not isinstance(v, list):
        raise ValueError(f"source_queries must be a list, got {type(v).__name__}")
    from slayer.core.query import SlayerQuery  # ALLOW(import-not-top): circular — slayer.core.query imports SlayerDataset/DatasetMetric from this module
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


class SourceDatasetOrigin(BaseModel):
    """In-memory lineage breadcrumb for virtual stage datasets: a linked list of
    upstream names so outer-stage dotted-ref lookup can strip prefixes.

    ``agg_field_names`` marks fields safe for the cross-stage intercept to
    re-aggregate, so a dimension that merely looks like an aggregation canonical
    isn't silently re-summed.
    """
    name: str
    data_source: str | None = None
    parent: Optional["SourceDatasetOrigin"] = None
    agg_field_names: frozenset[str] = Field(default_factory=frozenset)

    @model_validator(mode="wrap")
    @classmethod
    def _old_spellings(cls, data: Any, handler: ModelWrapValidatorHandler["SourceDatasetOrigin"]) -> "SourceDatasetOrigin":
        return renamed_input(data, handler, SOURCE_ORIGIN_KEYS, stored=in_stored_document())

    @property
    @deprecated("agg_column_names is deprecated; use agg_field_names", category=None)
    def agg_column_names(self) -> frozenset[str]:
        attribute_used(self, old="agg_column_names", new="agg_field_names")
        return self.agg_field_names

    @agg_column_names.setter
    @deprecated("agg_column_names is deprecated; use agg_field_names", category=None)
    def agg_column_names(self, value: frozenset[str]) -> None:
        attribute_used(self, old="agg_column_names", new="agg_field_names")
        self.agg_field_names = value


class DatasetRelationship(BaseModel):
    """A relationship to another dataset."""
    to: str                               # Name of the joined dataset
    join_pairs: list[list[str]] = Field(...)        # [[source field name, target field name], ...]
    join_type: JoinType = JoinType.LEFT             # LEFT (default) or INNER
    # Relationship arity, read source->target; None = undetermined.
    cardinality: RelationshipCardinality | None = None
    # Optional edge name, usable as a path segment in either direction — the
    # disambiguator for parallel edges. Dataset-name identifier rules.
    name: str | None = None
    # Optional human/agent metadata; additive, so no schema-version bump needed.
    description: str | None = None
    meta: dict[str, Any] | None = None
    # Runtime-only: a query object's local aliases of this edge.
    _aliases: tuple[str, ...] = PrivateAttr(default=())
    # Runtime-only: declared by a query extension, not stored.
    _query_local: bool = PrivateAttr(default=False)

    @model_validator(mode="wrap")
    @classmethod
    def _old_spellings(cls, data: Any, handler: ModelWrapValidatorHandler["DatasetRelationship"]) -> "DatasetRelationship":
        return renamed_input(data, handler, RELATIONSHIP_KEYS, stored=in_stored_document())

    @property
    def aliases(self) -> tuple[str, ...]:
        return self._aliases

    @property
    def query_local(self) -> bool:
        return self._query_local

    def with_aliases(self, aliases: tuple[str, ...]) -> "DatasetRelationship":
        out = self.model_copy()
        out._aliases = aliases
        return out

    def as_query_local(self) -> "DatasetRelationship":
        out = self.model_copy()
        out._query_local = True
        return out

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        if v is not None:
            _validate_dataset_name(v, "Relationship")
        return v

    @field_validator("join_pairs")
    @classmethod
    def _validate_join_pairs(cls, v: list[list[str]]) -> list[list[str]]:
        if not v:
            raise ValueError(
                "join_pairs must be non-empty; to combine a dataset no relationship path reaches, drop the relationship and "
                "aggregate its fields in the query (e.g. count(events.id) / count(*))"
            )
        for i, pair in enumerate(v):
            if len(pair) != 2 or not all(isinstance(s, str) and s for s in pair):
                raise ValueError(
                    f"join_pairs[{i}] must be [source_field, target_field] with non-empty strings, got {pair}"
                )
            for key in pair:
                _validate_field_name(key, "join_pairs key")
        return v

    @property
    @deprecated("target_model is deprecated; use to", category=None)
    def target_model(self) -> str:
        attribute_used(self, old="target_model", new="to")
        return self.to

    @target_model.setter
    @deprecated("target_model is deprecated; use to", category=None)
    def target_model(self, value: str) -> None:
        attribute_used(self, old="target_model", new="to")
        self.to = value


def relationship_key_error(
    *, dataset: str, target: str, key: str, side: str, fields: list[DatasetField],
) -> RelationshipKeyError | None:
    """Why ``key`` is not a declared base field of ``side`` (whose ``fields`` are given)."""
    head = f"relationship {dataset} → {target} key {key!r}"
    col = next((c for c in fields if c.name == key), None)
    if col is None:
        renamed = next((c.name for c in fields
                        if c.is_base and c.name != key and c.physical_name == key), None)
        hint = (f"name the field by its name {renamed!r}, not its physical sql" if renamed
                else f"declare DatasetField(name={key!r}) on '{side}' (sql= its physical column if renamed)")
        return RelationshipKeyError(summary=f"{head} is not a declared field of '{side}'",
                            suggestion=hint)
    if not col.is_base:
        return RelationshipKeyError(
            summary=f"{head} is an expression field of '{side}' (sql={col.sql!r})",
            suggestion="key the relationship on a base field: sql unset or a single field identifier",
        )
    if col.filter is not None:
        return RelationshipKeyError(
            summary=f"{head} carries a filter on '{side}'",
            suggestion="key the relationship on an unfiltered base field",
        )
    return None


def _check_relationship_keys(*, dataset_name: str, fields: list[DatasetField], relationships: list[DatasetRelationship]) -> None:
    """Every relationship's source-side key names a declared, unfiltered base field."""
    for relationship in relationships:
        for src, _ in relationship.join_pairs:
            err = relationship_key_error(dataset=dataset_name, target=relationship.to, key=src,
                                 side=dataset_name, fields=fields)
            if err is not None:
                raise err


def _check_field_metric_namespace(
    *, dataset_name: str, fields: list[DatasetField], metrics: list[DatasetMetric]
) -> None:
    """Fields and metrics share one namespace: unique within each, disjoint across."""
    col_names_seq = [c.name for c in fields]
    col_dupes = sorted({n for n in col_names_seq if col_names_seq.count(n) > 1})
    if col_dupes:
        raise ValueError(
            f"Dataset '{dataset_name}': duplicate field names: {col_dupes}. "
            f"Each field name must be unique within a dataset."
        )
    unnamed = [m.expression for m in metrics if m.name is None]
    if unnamed:
        raise ValueError(
            f"Dataset '{dataset_name}': every DatasetMetric in 'metrics' must "
            f"have a name. Unnamed expressions: {unnamed}."
        )
    metric_names_seq = [m.name for m in metrics if m.name is not None]
    metric_dupes = sorted(
        {n for n in metric_names_seq if metric_names_seq.count(n) > 1}
    )
    if metric_dupes:
        raise ValueError(
            f"Dataset '{dataset_name}': duplicate metric names: {metric_dupes}. "
            f"Each named DatasetMetric must have a unique name within a dataset."
        )
    overlap = sorted(set(col_names_seq) & set(metric_names_seq))
    if overlap:
        raise ValueError(
            f"Dataset '{dataset_name}': name collision between fields and "
            f"metrics: {overlap}. Each name must be unique within a dataset "
            f"(fields and metrics share a namespace)."
        )


class SlayerDataset(BaseModel):
    version: int = CURRENT_VERSIONS["SlayerModel"]
    name: str
    sql_table: str | None = None
    # Kind of DB object ``sql_table`` names; only auto-ingestion sets it. ``None`` = unknown.
    source_kind: Optional[ObjectKind] = None
    sql: str | None = None
    source_queries: Annotated[_SourceQueries, BeforeValidator(_coerce_source_queries)] = None
    query_variables: dict[str, Any] = Field(default_factory=dict)
    data_source: str = ""
    fields: list[DatasetField] = Field(default_factory=list)
    metrics: list[DatasetMetric] = Field(default_factory=list)
    aggregations: list[Aggregation] = Field(default_factory=list)

    _deprecated_inputs: tuple[DeprecatedInputWarning, ...] = PrivateAttr(default=())

    def __eq__(self, other: object) -> bool:
        """Equal whatever pre-2.0 spellings either was read from."""
        if not isinstance(other, BaseModel) or type(other) is not type(self):
            return NotImplemented
        return same_ignoring_deprecations(self, other)

    @model_validator(mode="wrap")
    @classmethod
    def _name_nested_migrations(cls, data: Any, handler: ModelWrapValidatorHandler["SlayerDataset"]) -> "SlayerDataset":
        is_dict = isinstance(data, dict)
        with migrating_document(data.get("name") if is_dict else None, stored=is_dict and "version" in data):
            stored = in_stored_document()
            with converted_document(stored=stored) as (spellings, _):
                if is_dict and not stored:
                    data = spellings.rename(data, DATASET_KEYS)
                dataset = handler(data)
                if is_dict:
                    dataset._deprecated_inputs = spellings.report()
        return dataset

    @model_validator(mode="before")
    @classmethod
    def _apply_schema_migrations(cls, data: Any) -> Any:
        return _migrate_schema(entity="SlayerModel", data=data)

    @property
    def deprecated_inputs(self) -> tuple[DeprecatedInputWarning, ...]:
        """Deprecations of the pre-2.0 spellings converted while building this dataset, nested parts included."""
        return self._deprecated_inputs

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _validate_dataset_name(v, "Dataset")

    @field_validator("data_source")
    @classmethod
    def _validate_data_source_format(cls, v: str) -> str:
        # Format-only checks; emptiness is enforced below so query-backed datasets
        # can be constructed before their cache populator fills data_source.
        if not v:
            return v
        if v.strip() != v:
            raise ValueError(
                f"Dataset 'data_source' must not have leading/trailing "
                f"whitespace; got {v!r}."
            )
        label = "Dataset 'data_source'"
        _NO_NUL.check(name=v, context=label)
        _NO_FWD_SLASH.check(name=v, context=label)
        _NO_BACK_SLASH.check(name=v, context=label)
        _NO_DOT.check(name=v, context=label)
        _NO_COLON.check(name=v, context=label)
        return v

    @model_validator(mode="after")
    def _require_data_source_unless_query_backed(self) -> "SlayerDataset":
        # Table-backed datasets need data_source up front (storage key); query-backed
        # ones may start empty — the engine fills it in before persisting.
        if not self.data_source.strip() and not self.source_queries:
            raise ValueError(
                f"Dataset '{self.name}': 'data_source' must be a non-empty "
                f"string. Set it to the name of the DatasourceConfig the "
                f"dataset belongs to."
            )
        return self
    relationships: list[DatasetRelationship] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)  # Dataset-level filters (always applied)
    default_time_dimension: str | None = None
    description: str | None = None
    hidden: bool = False
    # Callers sharing a tag may read the dataset; empty = public.
    access_tags: list[str] = Field(default_factory=list)
    meta: dict[str, Any] | None = None
    # In-memory breadcrumb for virtual stage datasets; ``exclude=True`` keeps it unpersisted.
    source_dataset_origin: SourceDatasetOrigin | None = Field(default=None, exclude=True)
    # Runtime-only (never persisted): a query stage's user spelling in relationship paths.
    _spelling: str | None = PrivateAttr(default=None)  # NOSONAR(S5890) — pydantic PrivateAttr descriptor; the attribute holds str | None
    # Runtime-only: this dataset is the P factor of a ``time_spine × P`` population.
    _population_spine: bool = PrivateAttr(default=False)
    # Runtime-only: datasets no relationship path reaches that this query frame's population names.
    _unrelated: frozenset[str] = PrivateAttr(default=frozenset())
    # Runtime-only: a list child's ``(parent dataset, list field)``.
    _list_of: tuple[str, str] | None = PrivateAttr(default=None)
    # Runtime-only: the list children built from this dataset (``relationship_walker.list_child``).
    _list_children: dict[tuple[str, str | None, str, DataType | None], "SlayerDataset"] = PrivateAttr(
        default_factory=dict)
    # Runtime-only: the field and metric names a query extension added.
    _query_local: frozenset[str] = PrivateAttr(default=frozenset())

    @field_validator("filters")
    @classmethod
    def _validate_filter_predicates(cls, v: list[str]) -> list[str]:
        """Validate each dataset filter as a SQL-mode predicate at construction time."""
        for f in v:
            parse_sql_predicate(f)
        return v

    @model_validator(mode="after")
    def _validate_field_metric_disjoint(self) -> "SlayerDataset":
        """Fields and metrics share one namespace (see ``_check_field_metric_namespace``)."""
        _check_field_metric_namespace(
            dataset_name=self.name, fields=self.fields, metrics=self.metrics
        )
        return self

    @model_validator(mode="after")
    def _validate_relationship_keys(self) -> "SlayerDataset":
        # An unexpanded query-backed dataset has no fields yet; checked once populated.
        if not self.awaits_fields:
            _check_relationship_keys(dataset_name=self.name, fields=self.fields, relationships=self.relationships)
        return self

    @model_validator(mode="after")
    def _reject_self_relationships(self) -> "SlayerDataset":
        """Relationships resolve by target-dataset name, so a self-relationship is unaddressable."""
        if any(j.to == self.name for j in self.relationships):
            raise ValueError(
                f"Dataset '{self.name}': relationship to '{self.name}' is the "
                f"dataset itself. Self-relationships are not supported — dotted "
                f"references resolve by dataset name, so a self-relationship can never "
                f"be addressed from a query. Define the second role as a "
                f"separate dataset over the same table (or a view) and declare a "
                f"relationship to that."
            )
        return self

    @model_validator(mode="after")
    def _validate_list_element_types(self) -> "SlayerDataset":
        """A list field needs an ``element_type``; an sql-mode dataset's undeclared one is ``UNKNOWN``."""
        for c in self.fields:
            if c.datatype is not DataType.ARRAY or c.element_type is not None:
                continue
            if not self.sql:
                raise ValueError(f"DatasetField {c.name!r} is a list (type ARRAY) and needs an element_type.")
            c.element_type = DataType.UNKNOWN
        return self

    @model_validator(mode="after")
    def _validate_allowed_aggregations(self) -> "SlayerDataset":
        """Enforce that ``DatasetField.allowed_aggregations`` is a subset of the type/PK eligibility set."""
        custom_agg_names = {a.name for a in self.aggregations}
        valid_names = BUILTIN_AGGREGATIONS | custom_agg_names
        for c in self.fields:
            if c.allowed_aggregations is None:
                continue
            if c.datatype.is_opaque:
                db_type_note = f" (db_type={c.db_type!r})" if c.db_type else ""
                raise ValueError(
                    f"DatasetField '{c.name}'{db_type_note}: allowed_aggregations "
                    f"cannot be declared on a field of type {c.datatype} — "
                    f"aggregations are not supported for that type. Remove "
                    f"allowed_aggregations, or give the field an operable type."
                )
            if c.datatype is DataType.ARRAY and set(c.allowed_aggregations) - DEFAULT_AGGREGATIONS_BY_TYPE[DataType.ARRAY]:
                raise ValueError(
                    f"DatasetField '{c.name}' is a list: its only aggregations are 'count' and 'count_distinct', got "
                    f"allowed_aggregations "
                    f"{c.allowed_aggregations}. Aggregate its elements through '{c.name}.value' instead."
                )
            identifier = is_identifier(field=c, fields=self.fields)
            for agg_name in c.allowed_aggregations:
                _check_allowed_aggregation(
                    field=c, agg_name=agg_name, identifier=identifier,
                    custom_agg_names=custom_agg_names, valid_names=valid_names,
                )
        return self

    @model_validator(mode="after")
    def _validate_source_mode_exclusivity(self) -> "SlayerDataset":
        """Exactly one of sql_table, sql, source_queries must be populated."""
        if self.source_queries is not None and len(self.source_queries) == 0:
            raise ValueError(
                f"Dataset '{self.name}': source_queries cannot be an empty list. "
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
                f"Dataset '{self.name}' must specify exactly one source: "
                f"sql_table, sql, or source_queries (none specified)."
            )
        if len(populated) > 1:
            raise ValueError(
                f"Dataset '{self.name}' must specify exactly one source: "
                f"sql_table, sql, or source_queries (got: {populated})."
            )
        return self

    # NOSONAR S3516 — Pydantic v2 @model_validator(mode="after") is required to
    # return ``self``; the rule's "always returns same value" warning doesn't
    # apply to validator methods.
    @model_validator(mode="after")
    def _validate_source_query_stages(self) -> "SlayerDataset":
        """Non-final stages must be named; all stage names must be unique."""
        if not self.source_queries:
            return self
        stages = self.source_queries
        if len(stages) > 1:
            for i, stage in enumerate(stages[:-1]):
                if not getattr(stage, "name", None):
                    raise ValueError(
                        f"Dataset '{self.name}': non-final stage at index {i} "
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
                f"Dataset '{self.name}': duplicate stage name(s) in "
                f"source_queries: {sorted(dupes)}."
            )
        return self

    # NOSONAR S3516 — Pydantic v2 @model_validator(mode="after") must return self.
    @model_validator(mode="after")
    def _reject_metrics_on_query_backed(self) -> "SlayerDataset":
        """A query-backed dataset may not declare ``metrics`` directly — they never take effect."""
        if self.source_queries and self.metrics:
            raise ValueError(
                f"Dataset '{self.name}': a query-backed dataset (source_queries) "
                f"cannot declare metrics directly — they never take effect. "
                f"Declare the metric in the backing query's final stage, or add "
                f"it with a DatasetExtension at query time."
            )
        return self

    @property
    def explicit_spelling(self) -> str | None:
        """A query stage's user spelling in relationship paths, when it differs from ``name``."""
        return self._spelling

    @property
    def spelling(self) -> str:
        return self._spelling or self.name

    def with_spelling(self, spelling: str | None) -> "SlayerDataset":
        out = self.model_copy()
        out._spelling = spelling
        return out

    @property
    def population_spine(self) -> bool:
        """This dataset is the P factor of a spine population (its spine hop is the product)."""
        return self._population_spine

    def as_population_factor(self, factor: bool = True) -> "SlayerDataset":
        out = self.model_copy()
        out._population_spine = factor
        return out

    @property
    def unrelated_datasets(self) -> frozenset[str]:
        """Names of the datasets this query frame's population meets only as a product."""
        return self._unrelated

    def with_unrelated(self, names: frozenset[str]) -> "SlayerDataset":
        out = self.model_copy()
        out._unrelated = names
        return out

    @property
    def query_local_names(self) -> frozenset[str]:
        """The field and metric names a query extension added to this dataset."""
        return self._query_local

    def with_query_local(self, names: frozenset[str]) -> "SlayerDataset":
        out = self.model_copy()
        out._query_local = names
        return out

    @property
    def extended(self) -> bool:
        """Whether a query extension added anything to this dataset."""
        return bool(self._query_local) or any(j.query_local or j.aliases for j in self.relationships)

    def definition_view(self) -> "SlayerDataset":
        """This dataset as stored definitions see it: without anything a query extension added."""
        if not self.extended:
            return self
        local = self._query_local
        out = self.model_copy(update={
            "fields": [c for c in self.fields if c.name not in local],
            "metrics": [m for m in self.metrics if m.name not in local],
            "relationships": [j.with_aliases(()) if j.aliases else j for j in self.relationships if not j.query_local],
        })
        out._query_local = frozenset()
        return out

    @property
    def list_of(self) -> tuple[str, str] | None:
        """``(parent dataset, list field)`` when this is a list field's element child (semantics Axiom 3.2)."""
        return self._list_of

    def list_fields(self) -> list[DatasetField]:
        """The list fields whose elements are reachable as a child (opaque elements excluded)."""
        return [c for c in self.fields if c.datatype is DataType.ARRAY and not (c.element_type or DataType.UNKNOWN).is_opaque]

    @property
    def awaits_fields(self) -> bool:
        """Query-backed with its output columns not yet populated."""
        return bool(self.source_queries) and not self.fields

    @property
    def effective_default_time_dimension(self) -> str | None:
        """The declared ``default_time_dimension``, else the dataset's only DATE / TIMESTAMP field."""
        if self.default_time_dimension:
            return self.default_time_dimension
        temporal = [c.name for c in self.fields if c.datatype in (DataType.DATE, DataType.TIMESTAMP)]
        return temporal[0] if len(temporal) == 1 else None

    def get_field(self, name: str, *, quoted: bool = False) -> DatasetField | None:
        """The field ``name`` reaches (``slayer.core.names.match_name``)."""
        found = match_name(name=name, declared=[c.name for c in self.fields], quoted=quoted)
        return None if found is None else next(c for c in self.fields if c.name == found)

    def get_metric(self, name: str, *, quoted: bool = False) -> DatasetMetric | None:
        """The saved metric ``name`` reaches (``slayer.core.names.match_name``)."""
        found = match_name(name=name, declared=[m.name for m in self.metrics if m.name], quoted=quoted)
        return None if found is None else next(m for m in self.metrics if m.name == found)

    def get_aggregation(self, name: str, *, quoted: bool = False) -> Aggregation | None:
        """The aggregation ``name`` reaches (``slayer.core.names.match_name``)."""
        found = match_name(name=name, declared=[a.name for a in self.aggregations], quoted=quoted)
        return None if found is None else next(a for a in self.aggregations if a.name == found)

    @method_twin("get_field")
    @deprecated("get_column is deprecated; use get_field", category=None)
    def get_column(self, name: str, *, quoted: bool = False) -> DatasetField | None: ...

    @method_twin("get_metric")
    @deprecated("get_measure is deprecated; use get_metric", category=None)
    def get_measure(self, name: str, *, quoted: bool = False) -> DatasetMetric | None: ...

    @method_twin("list_fields")
    @deprecated("list_columns is deprecated; use list_fields", category=None)
    def list_columns(self) -> list[DatasetField]: ...

    @property
    @deprecated("columns is deprecated; use fields", category=None)
    def columns(self) -> list[DatasetField]:
        attribute_used(self, old="columns", new="fields")
        return self.fields

    @columns.setter
    @deprecated("columns is deprecated; use fields", category=None)
    def columns(self, value: list[DatasetField]) -> None:
        attribute_used(self, old="columns", new="fields")
        self.fields = value

    @property
    @deprecated("measures is deprecated; use metrics", category=None)
    def measures(self) -> list[DatasetMetric]:
        attribute_used(self, old="measures", new="metrics")
        return self.metrics

    @measures.setter
    @deprecated("measures is deprecated; use metrics", category=None)
    def measures(self, value: list[DatasetMetric]) -> None:
        attribute_used(self, old="measures", new="metrics")
        self.metrics = value

    @property
    @deprecated("joins is deprecated; use relationships", category=None)
    def joins(self) -> list[DatasetRelationship]:
        attribute_used(self, old="joins", new="relationships")
        return self.relationships

    @joins.setter
    @deprecated("joins is deprecated; use relationships", category=None)
    def joins(self, value: list[DatasetRelationship]) -> None:
        attribute_used(self, old="joins", new="relationships")
        self.relationships = value

    @property
    @deprecated("source_model_origin is deprecated; use source_dataset_origin", category=None)
    def source_model_origin(self) -> SourceDatasetOrigin | None:
        attribute_used(self, old="source_model_origin", new="source_dataset_origin")
        return self.source_dataset_origin

    @source_model_origin.setter
    @deprecated("source_model_origin is deprecated; use source_dataset_origin", category=None)
    def source_model_origin(self, value: SourceDatasetOrigin | None) -> None:
        attribute_used(self, old="source_model_origin", new="source_dataset_origin")
        self.source_dataset_origin = value

    @property
    @deprecated("awaits_columns is deprecated; use awaits_fields", category=None)
    def awaits_columns(self) -> bool:
        attribute_used(self, old="awaits_columns", new="awaits_fields")
        return self.awaits_fields


class DatasourceConfig(BaseModel):
    version: int = CURRENT_VERSIONS["DatasourceConfig"]
    name: str
    type: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None
    connection_string: str | None = None
    schema_name: str | None = None
    # Unlike ``schema_name`` (upstream physical schema), this is the schema the
    # Postgres facade advertises this datasource's datasets under; ``None`` => "public".
    postgres_schema: str | None = None
    description: str | None = None
    # Snowflake-specific. ``connection_name`` is the primary auth path (creds from
    # connections.toml); inline fields + ``warehouse``/``role`` are the secondary path.
    connection_name: str | None = None
    warehouse: str | None = None
    role: str | None = None
    # BigQuery-specific. A service-account key file as JSON; when set, the engine
    # authenticates against it, else falls back to Application Default Credentials.
    credentials_json: str | None = Field(default=None, repr=False)
    # BigQuery-specific. A per-end-user OAuth authorized-user grant as JSON;
    # mutually exclusive with ``credentials_json`` (which carries a service account).
    oauth_credentials_json: str | None = Field(default=None, repr=False)
    # Postgres-specific: reached through a transaction-mode pooler (e.g. PgBouncer), so session state is never set.
    transaction_pooler: bool = False
    granularities: list[CustomGranularity] = Field(
        default_factory=list,
        description=(
            "Custom time granularities: {name, base, multiple, origin}, buckets starting "
            "at origin + k × multiple × base; usable wherever a built-in granularity is."
        ),
    )

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
        # Leading segment of every canonical-id and a YAML storage path component;
        # substring rules shared with ``SlayerDataset.data_source``. ``__`` is allowed.
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
        # Must be a lowercase unquoted Postgres identifier (avoids quoting ambiguity).
        if v is None:
            return v
        _require_non_empty_trimmed(v=v, context="Datasource 'postgres_schema'")
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", v):
            raise ValueError(
                f"Datasource 'postgres_schema' must be a lowercase Postgres "
                f"identifier matching [a-z_][a-z0-9_]*, got {v!r}"
            )
        return v

    def check_granularities(self) -> None:
        """Raise ``GranularityDefinitionError`` for the first entry breaking a save-time rule."""
        reserved = reserved_granularity_names()
        seen: set[str] = set()
        for g in self.granularities:
            rule = definition_error(g, reserved=reserved)
            if rule is None and granularity_key(g.name) in seen:
                rule = "the name duplicates another entry (names compare case-insensitively)"
            if rule is not None:
                raise GranularityDefinitionError(name=g.name, rule=rule)
            seen.add(granularity_key(g.name))

    @property
    def granularity_definitions(self) -> dict[str, CustomGranularity]:
        """The custom granularities keyed by ``granularity_key``."""
        return {granularity_key(g.name): g for g in self.granularities}

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
        """The connection URL; one naming no driver gets the type's default driver."""
        return dialect_for_ds_type(self.type).driver_facts(self.type).pin_default_driver(
            self._unpinned_connection_string()
        )

    def _unpinned_connection_string(self) -> str:
        if self.connection_string:
            return self.connection_string
        # Dialect-specific hook (e.g. Snowflake); others return None and fall through.
        dialect = dialect_for_ds_type(self.type)
        url_from_dialect = dialect.build_connection_url(self)
        if url_from_dialect is not None:
            return str(url_from_dialect)
        if self.type in ("sqlite", "duckdb"):
            return f"{self.type}:///{self.database}"
        if self.type in ("mssql", "sqlserver", "tsql"):
            return self._get_tsql_connection_string()
        driver = dialect.driver_facts(self.type).url_scheme
        host, port = self._split_host_port()
        if driver is None:
            # Mirrors ``URL.create``'s own rejection of a non-string drivername.
            raise TypeError("drivername must be a string")
        # Structured builder: reserved chars in credentials are percent-encoded, not misparsed.
        return _SA_URL.create(
            drivername=driver,
            username=self.username or None,
            password=self.password or None,
            host=host,
            port=port,
            database=self.database or "",
        ).render_as_string(hide_password=False)

    def _split_host_port(self) -> tuple[str, int | None]:
        """Raw host + port for ``URL.create``, lifting a port embedded in ``host``; a port set in both places is contradictory."""
        host, port = self.host or "localhost", self.port
        embedded_port: str | None = None
        bracketed = _BRACKETED_HOST_RE.match(host)
        if bracketed:
            host, embedded_port = bracketed.group(1), bracketed.group(2)
        else:
            embedded = _HOST_EMBEDDED_PORT_RE.match(host)
            if embedded:
                host, embedded_port = embedded.group(1), embedded.group(2)
        if embedded_port is None:
            return host, port
        if port is not None:
            raise ValueError(
                f"Datasource '{self.name}': port is set both in the host "
                f"field ({self.host!r}) and in the 'port' field ({port}); "
                f"specify it in only one place."
            )
        return host, int(embedded_port)

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


def reserved_granularity_names() -> dict[str, str]:
    """Names a custom granularity may not take, lowercased, with the kind they belong to."""
    out: dict[str, str] = dict.fromkeys(RESERVED_SCALAR_NAMES, "Mode-B function")
    out.update(dict.fromkeys(RESERVED_DATE_SYNTAX_NAMES, "Mode-B date function"))
    out.update(dict.fromkeys(ALL_TRANSFORMS, "transform"))
    out.update(dict.fromkeys(BUILTIN_AGGREGATIONS, "aggregation"))
    out.update(dict.fromkeys(GRANULARITY_NAMES, "granularity"))
    out.update(dict.fromkeys(RESERVED_NAMES, "reserved function"))
    return out


def _resolve_env_string(value: str) -> str:
    def replacer(match: re.Match[str]) -> str:
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))

    return re.sub(r"\$\{(\w+)\}", replacer, value)


def case_collisions(dataset: SlayerDataset) -> list[tuple[str, list[str]]]:
    """Each group of ``dataset``'s names that differ only in case, per namespace: fields and metrics (one namespace),
    aggregations, relationship names, and a saved query's stage names and each stage's result names."""
    namespaces: list[tuple[str, list[str]]] = [
        ("fields", [c.name for c in dataset.fields] + [m.name for m in dataset.metrics if m.name]),
        ("aggregations", [a.name for a in dataset.aggregations]),
        ("relationships", [j.name for j in dataset.relationships if j.name]),
    ]
    stages: Sequence[SlayerQuery | dict[str, Any]] = dataset.source_queries or []
    namespaces.append(("stages", [n for q in stages if (n := _item_name(q))]))
    for q in stages:
        where = f"result names of stage {_item_name(q)!r}" if _item_name(q) else "result names"
        metrics = q.get("metrics") if isinstance(q, dict) else q.metrics
        namespaces.append((where, [n for m in metrics or [] if (n := _item_name(m))]))
    return [(where, group) for where, names in namespaces for group in folded_collisions(names)]


def _item_name(item: Any) -> str | None:
    """A stage's or metric's name, validated or a raw dict."""
    return item.get("name") if isinstance(item, dict) else getattr(item, "name", None)


def name_collision_warnings(dataset: SlayerDataset) -> list[NameCollisionWarning]:
    """The ``NameCollisionWarning`` per case collision ``dataset`` holds."""
    return [NameCollisionWarning(dataset=dataset.name, namespace=where, names=names) for where, names in case_collisions(dataset)]


if TYPE_CHECKING:
    @deprecated("SlayerModel is deprecated; use SlayerDataset", category=None)
    class SlayerModel(SlayerDataset): ...

    @deprecated("Column is deprecated; use DatasetField", category=None)
    class Column(DatasetField): ...

    @deprecated("ModelMeasure is deprecated; use DatasetMetric", category=None)
    class ModelMeasure(DatasetMetric): ...

    @deprecated("ModelJoin is deprecated; use DatasetRelationship", category=None)
    class ModelJoin(DatasetRelationship): ...

    @deprecated("SourceModelOrigin is deprecated; use SourceDatasetOrigin", category=None)
    class SourceModelOrigin(SourceDatasetOrigin): ...

    @deprecated("join_key_error is deprecated; use relationship_key_error", category=None)
    def join_key_error(
        *, dataset: str, target: str, key: str, side: str, fields: list[DatasetField],
    ) -> RelationshipKeyError | None: ...


__getattr__ = name_twins(
    namespace=globals(),
    renames={
        "SlayerModel": "SlayerDataset", "Column": "DatasetField", "ModelMeasure": "DatasetMetric",
        "ModelJoin": "DatasetRelationship", "SourceModelOrigin": "SourceDatasetOrigin",
        "join_key_error": "relationship_key_error",
    },
    arguments={"join_key_error": {"model": "dataset", "columns": "fields"}},
)
