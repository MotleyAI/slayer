"""Pydantic v2 models for OSI (Open Semantic Interchange) documents.

Ported from the OSI reference package (``open-semantic-interchange/OSI``,
``python/src/osi/models.py``, Apache-2.0). The schema is stable across OSI spec
versions 1.0 / 0.1.0 / 0.1.1 / 0.2.0.dev0 — the only differences are the version
string and two optional document-level enum arrays, both absorbed by
``extra="ignore"``.

Deviations from the reference package:
- ``extra="ignore"`` everywhere (forward/back-compat across spec versions).
- ``vendor_name`` is a free string (0.2.0 widened it from an enum).
- Models are not frozen (the importer does not mutate them, but leaving them
  mutable avoids friction with Pydantic ``model_validate`` round-trips in tests).
"""

from enum import Enum
from typing import Any, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


class OSIDialect(str, Enum):
    """Supported SQL and expression language dialects."""

    ANSI_SQL = "ANSI_SQL"
    SNOWFLAKE = "SNOWFLAKE"
    MDX = "MDX"
    MAQL = "MAQL"
    TABLEAU = "TABLEAU"
    DATABRICKS = "DATABRICKS"


class OSIAIContextObject(BaseModel):
    """Structured AI context with instructions, synonyms, and examples."""

    model_config = ConfigDict(extra="allow")

    instructions: Optional[str] = None
    synonyms: Optional[list[str]] = None
    examples: Optional[list[str]] = None


# ai_context is either a plain string or the structured object above.
OSIAIContext = Union[str, OSIAIContextObject]


class OSICustomExtension(BaseModel):
    """Vendor-specific metadata as a serialized JSON string."""

    model_config = ConfigDict(extra="ignore")

    vendor_name: str
    data: str


class OSIDialectExpression(BaseModel):
    """Expression in a specific dialect."""

    model_config = ConfigDict(extra="ignore")

    dialect: OSIDialect
    expression: str


class OSIExpression(BaseModel):
    """Expression definition with multi-dialect support."""

    model_config = ConfigDict(extra="ignore")

    dialects: list[OSIDialectExpression]


class OSIDimension(BaseModel):
    """Dimension metadata on a field."""

    model_config = ConfigDict(extra="ignore")

    is_time: Optional[bool] = None


class OSIField(BaseModel):
    """Row-level attribute for grouping, filtering, and metric expressions."""

    model_config = ConfigDict(extra="ignore")

    name: str
    expression: OSIExpression
    dimension: Optional[OSIDimension] = None
    label: Optional[str] = None
    description: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None
    custom_extensions: Optional[list[OSICustomExtension]] = None


class OSIDataset(BaseModel):
    """Logical dataset representing a business entity (fact or dimension table)."""

    model_config = ConfigDict(extra="ignore")

    name: str
    source: str
    primary_key: Optional[list[str]] = None
    unique_keys: Optional[list[list[str]]] = None
    description: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None
    fields: Optional[list[OSIField]] = None
    custom_extensions: Optional[list[OSICustomExtension]] = None


class OSIRelationship(BaseModel):
    """Foreign key relationship between datasets (``from`` = many, ``to`` = one)."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    name: str
    from_dataset: str = Field(..., alias="from")
    to: str
    from_columns: list[str]
    to_columns: list[str]
    ai_context: Optional[OSIAIContext] = None
    custom_extensions: Optional[list[OSICustomExtension]] = None


class OSIMetric(BaseModel):
    """Quantitative measure defined on business data (raw SQL aggregation)."""

    model_config = ConfigDict(extra="ignore")

    name: str
    expression: OSIExpression
    description: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None
    custom_extensions: Optional[list[OSICustomExtension]] = None


class OSISemanticModel(BaseModel):
    """Top-level container representing a complete semantic model."""

    model_config = ConfigDict(extra="ignore")

    name: str
    description: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None
    datasets: list[OSIDataset]
    relationships: Optional[list[OSIRelationship]] = None
    metrics: Optional[list[OSIMetric]] = None
    custom_extensions: Optional[list[OSICustomExtension]] = None


class OSIDocument(BaseModel):
    """Root OSI document."""

    model_config = ConfigDict(extra="ignore")

    version: str = "0.2.0.dev0"
    semantic_model: list[OSISemanticModel]

    @field_validator("version", mode="before")
    @classmethod
    def _coerce_version_to_str(cls, v: Any) -> Any:
        # YAML parses an unquoted ``version: 1.0`` as a float and ``version: 1``
        # as an int; OSI spec versions are strings. Coerce numeric scalars so a
        # valid-but-unquoted version doesn't fail validation (and get skipped).
        if isinstance(v, (int, float)):
            return str(v)
        return v


def ai_context_to_dict(ctx: Optional[OSIAIContext]) -> Optional[dict[str, Any]]:
    """Normalize an ai_context (string or object) into a plain dict, or None."""
    if ctx is None:
        return None
    if isinstance(ctx, str):
        return {"instructions": ctx}
    return ctx.model_dump(exclude_none=True)
