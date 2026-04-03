"""Core domain models for SLayer."""

import os
import re
from typing import Any, List, Optional

from pydantic import BaseModel, Field, model_validator

from slayer.core.enums import DataType


class Dimension(BaseModel):
    name: str
    sql: Optional[str] = None
    type: DataType = DataType.STRING
    primary_key: bool = False
    description: Optional[str] = None
    hidden: bool = False


class Measure(BaseModel):
    name: str
    sql: Optional[str] = None
    type: DataType = DataType.COUNT
    description: Optional[str] = None
    hidden: bool = False


class ModelJoin(BaseModel):
    """A LEFT JOIN relationship to another model."""
    target_model: str                               # Name of the joined model
    join_pairs: List[List[str]] = Field(...)        # [["source_dim", "target_dim"], ...]


class SlayerModel(BaseModel):
    name: str
    sql_table: Optional[str] = None
    sql: Optional[str] = None
    source_queries: Optional[List] = None  # List of SlayerQuery dicts — saved query structure
    data_source: Optional[str] = None
    dimensions: List[Dimension] = Field(default_factory=list)
    measures: List[Measure] = Field(default_factory=list)
    joins: List[ModelJoin] = Field(default_factory=list)
    default_time_dimension: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False

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
        if self.type in ("sqlite",):
            return f"sqlite:///{self.database}"
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
