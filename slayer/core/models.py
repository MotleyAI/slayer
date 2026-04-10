"""Core domain models for SLayer."""

import logging
import os
import re
from typing import Any, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from slayer.core.enums import BUILTIN_AGGREGATIONS, DataType

logger = logging.getLogger(__name__)

_MULTIDOT_COLUMN_RE = re.compile(r'\b([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*){2,})\b')
_STRING_LITERAL_RE = re.compile(r"'[^']*'")


def _validate_model_name(name: str, context: str) -> str:
    """Reject model/query names containing ``__`` or ``.``.

    Model and query names become SQL table aliases where ``__`` encodes
    join paths, so both separators are reserved.
    """
    if "__" in name:
        raise ValueError(
            f"{context} name '{name}' must not contain '__'. "
            f"Double underscores are reserved for join path aliases in generated SQL."
        )
    if "." in name:
        raise ValueError(
            f"{context} name '{name}' must not contain '.'. "
            f"Dots are path syntax for referencing joined models in queries."
        )
    return name


def _validate_column_name(name: str, context: str) -> str:
    """Reject dimension/measure names containing ``.``.

    Dots are path syntax in queries (``customers.name``), not part of names.
    ``__`` is allowed — it encodes flattened join paths in virtual models
    created by ``_query_as_model`` (e.g., ``stores__name``).
    """
    if "." in name:
        raise ValueError(
            f"{context} name '{name}' must not contain '.'. "
            f"Dots are path syntax for referencing joined models in queries, "
            f"not part of dimension or measure names."
        )
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


class Dimension(BaseModel):
    name: str
    sql: Optional[str] = None
    type: DataType = DataType.STRING
    primary_key: bool = False
    description: Optional[str] = None
    hidden: bool = False

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _validate_column_name(v, "Dimension")

    @field_validator("sql")
    @classmethod
    def _fix_multidot_sql(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v = _fix_multidot_sql(v, context="Dimension sql")
        return v


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
    formula: Optional[str] = None  # SQL template; None = use built-in formula
    params: List[AggregationParam] = Field(default_factory=list)
    description: Optional[str] = None

    @model_validator(mode="after")
    def _require_formula_for_custom(self) -> "Aggregation":
        if self.name not in BUILTIN_AGGREGATIONS and self.formula is None:
            raise ValueError(
                f"Aggregation '{self.name}' is not a built-in aggregation; "
                f"a 'formula' is required. Built-in aggregations: "
                f"{', '.join(sorted(BUILTIN_AGGREGATIONS))}"
            )
        return self


class Measure(BaseModel):
    name: str
    sql: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False
    allowed_aggregations: Optional[List[str]] = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _validate_column_name(v, "Measure")

    @field_validator("sql")
    @classmethod
    def _fix_multidot_sql(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v = _fix_multidot_sql(v, context="Measure sql")
        return v


class ModelJoin(BaseModel):
    """A LEFT JOIN relationship to another model."""
    target_model: str                               # Name of the joined model
    join_pairs: List[List[str]] = Field(...)        # [["source_dim", "target_dim"], ...]

    @field_validator("join_pairs")
    @classmethod
    def _validate_join_pairs(cls, v: List[List[str]]) -> List[List[str]]:
        if not v:
            raise ValueError("join_pairs must be non-empty")
        for i, pair in enumerate(v):
            if len(pair) != 2 or not all(isinstance(s, str) and s for s in pair):
                raise ValueError(
                    f"join_pairs[{i}] must be [source_dim, target_dim] with non-empty strings, got {pair}"
                )
        return v


class SlayerModel(BaseModel):
    name: str
    sql_table: Optional[str] = None
    sql: Optional[str] = None
    source_queries: Optional[List] = None  # List of SlayerQuery dicts — saved query structure
    data_source: str = ""
    dimensions: List[Dimension] = Field(default_factory=list)
    measures: List[Measure] = Field(default_factory=list)
    aggregations: List[Aggregation] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _validate_model_name(v, "Model")
    joins: List[ModelJoin] = Field(default_factory=list)
    filters: List[str] = Field(default_factory=list)  # Model-level filters (always applied)
    default_time_dimension: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False

    @field_validator("filters")
    @classmethod
    def _fix_multidot_filters(cls, v: List[str]) -> List[str]:
        """Auto-convert multi-dot column references in model filters.

        Model filters are SQL snippets, so joined column references must use
        the __ alias syntax (e.g., ``customers__regions.name``), not the
        multi-dot query syntax (``customers.regions.name``).  Single-dot
        references like ``customers.name`` (table.column) are left as-is.
        """
        return [_fix_multidot_sql(f, context="Model filter") for f in v]

    @model_validator(mode="after")
    def _validate_allowed_aggregations(self) -> "SlayerModel":
        """Validate that allowed_aggregations on measures reference valid names."""
        custom_agg_names = {a.name for a in self.aggregations}
        valid_names = BUILTIN_AGGREGATIONS | custom_agg_names
        for m in self.measures:
            if m.allowed_aggregations is not None:
                for agg_name in m.allowed_aggregations:
                    if agg_name not in valid_names:
                        raise ValueError(
                            f"Measure '{m.name}': allowed_aggregations contains "
                            f"'{agg_name}', which is not a built-in aggregation "
                            f"or defined in this model's aggregations. "
                            f"Valid: {sorted(valid_names)}"
                        )
        return self

    def get_dimension(self, name: str) -> Optional[Dimension]:
        for d in self.dimensions:
            if d.name == name:
                return d
        return None

    def get_measure(self, name: str) -> Optional[Measure]:
        for m in self.measures:
            if m.name == name:
                return m
        return None

    def get_aggregation(self, name: str) -> Optional[Aggregation]:
        for a in self.aggregations:
            if a.name == name:
                return a
        return None


class DatasourceConfig(BaseModel):
    name: str
    type: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    connection_string: Optional[str] = None
    schema_name: Optional[str] = None
    description: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _accept_user_alias(cls, data: Any) -> Any:
        if isinstance(data, dict) and "user" in data and "username" not in data:
            data["username"] = data.pop("user")
        return data

    def get_connection_string(self) -> str:
        if self.connection_string:
            return self.connection_string
        if self.type in ("sqlite", "duckdb"):
            return f"{self.type}:///{self.database}"
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
        for key, value in data.items():
            if isinstance(value, str):
                data[key] = _resolve_env_string(value)
        return DatasourceConfig(**data)


def _resolve_env_string(value: str) -> str:
    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))

    return re.sub(r"\$\{(\w+)\}", replacer, value)
