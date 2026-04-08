"""Core domain models for SLayer."""

import logging
import os
import re
from typing import Any, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from slayer.core.enums import DataType

logger = logging.getLogger(__name__)

_MULTIDOT_COLUMN_RE = re.compile(r'\b([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*){2,})\b')
_STRING_LITERAL_RE = re.compile(r"'[^']*'")


def _validate_no_double_underscore(name: str, context: str) -> str:
    """Reject names containing ``__`` — reserved for join path aliases in SQL."""
    if "__" in name:
        raise ValueError(
            f"{context} name '{name}' must not contain '__'. "
            f"Double underscores are reserved for join path aliases in generated SQL."
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
        return _validate_no_double_underscore(v, "Dimension")

    @field_validator("sql")
    @classmethod
    def _fix_multidot_sql(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v = _fix_multidot_sql(v, context="Dimension sql")
        return v


class Measure(BaseModel):
    name: str
    sql: Optional[str] = None
    type: DataType = DataType.COUNT
    description: Optional[str] = None
    hidden: bool = False

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _validate_no_double_underscore(v, "Measure")

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

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _validate_no_double_underscore(v, "Model")
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
