"""Pydantic v2 models for Ossie (OSI) semantic-model documents.

Mirrors the upstream core spec (``core-spec/ossie-schema.json``); every model
uses ``extra="ignore"`` for forward compatibility. One ``OSIDocument`` holds one
semantic model; ``legacy`` marks a model split out of a ``semantic_model``
wrapper by the parser.
"""

from typing import Any, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    """Expression in one dialect; ``dialect`` is any non-blank token."""

    model_config = ConfigDict(extra="ignore")

    dialect: str
    expression: str

    @field_validator("dialect")
    @classmethod
    def _non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("dialect must be a non-blank string")
        return v


class OSIExpression(BaseModel):
    """Portable ``sql`` (OSSIE_SQL_2026) and/or per-dialect entries."""

    model_config = ConfigDict(extra="ignore")

    sql: Optional[str] = None
    dialects: list[OSIDialectExpression] = []


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
    datatype: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None
    custom_extensions: Optional[list[OSICustomExtension]] = None


class OSIMetric(BaseModel):
    """Quantitative measure defined on business data (raw SQL aggregation)."""

    model_config = ConfigDict(extra="ignore")

    name: str
    expression: OSIExpression
    description: Optional[str] = None
    datatype: Optional[str] = None
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
    metrics: Optional[list[OSIMetric]] = None
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


class OSIDocument(BaseModel):
    """One semantic model; ``legacy`` is set by the parser for wrapper-split models."""

    model_config = ConfigDict(extra="ignore")

    version: str = "0.2.0.dev0"
    name: str
    description: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None
    datasets: list[OSIDataset]
    relationships: Optional[list[OSIRelationship]] = None
    metrics: Optional[list[OSIMetric]] = None
    custom_extensions: Optional[list[OSICustomExtension]] = None
    legacy: bool = False

    @field_validator("version", mode="before")
    @classmethod
    def _coerce_version_to_str(cls, v: Any) -> Any:
        # YAML reads an unquoted ``version: 1.0`` as a float.
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
