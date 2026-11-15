"""Pydantic v2 models for Ossie (OSI) semantic-model documents.

Mirrors the upstream core spec (``core-spec/ossie-schema.json``); every model
uses ``extra="ignore"`` for forward compatibility. One ``OSIDocument`` holds one
semantic model; ``legacy`` marks a model split out of a ``semantic_model``
wrapper by the parser.
"""

from typing import Any, Optional, Self, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ModelWrapValidatorHandler,
    PrivateAttr,
    field_validator,
    model_validator,
)

from slayer.core.enums import DataType

OSSIE_DATATYPES: dict[str, DataType] = {
    "String": DataType.TEXT, "Integer": DataType.INT, "Decimal": DataType.DOUBLE,
    "Float": DataType.DOUBLE, "Boolean": DataType.BOOLEAN, "Date": DataType.DATE,
    "DateTime": DataType.TIMESTAMP, "DateTimeTz": DataType.TIMESTAMP,
    "Time": DataType.UNKNOWN, "Opaque": DataType.UNKNOWN,
}
TEMPORAL_DATATYPES = frozenset({"Date", "Time", "DateTime", "DateTimeTz"})


class OSIAIContextObject(BaseModel):
    """Structured AI context with instructions, synonyms, and examples."""

    model_config = ConfigDict(extra="allow")

    instructions: Optional[str] = None
    synonyms: Optional[list[str]] = None
    examples: Optional[list[str]] = None


# ai_context is either a plain string or the structured object above.
OSIAIContext = Union[str, OSIAIContextObject]


class OSINonportableExtension(BaseModel):
    """Vendor-specific state; ``data`` is raw (checked by the importer), ``legacy`` when read from ``custom_extensions``."""

    model_config = ConfigDict(extra="ignore")

    vendor_name: Any = None
    data: Any = None
    _legacy: bool = PrivateAttr(default=False)

    @property
    def legacy(self) -> bool:
        return self._legacy

    @model_validator(mode="before")
    @classmethod
    def _non_mapping_entry(cls, data: Any) -> Any:
        # A non-mapping entry reaches the importer's check (and its report) instead of refusing the document.
        return data if isinstance(data, (dict, OSINonportableExtension)) else {"data": data}


class _Extensible(BaseModel):
    """An entity carrying ``nonportable_extensions``; legacy ``custom_extensions`` entries are appended, marked."""

    nonportable_extensions: Optional[list[OSINonportableExtension]] = None
    _legacy_key: bool = PrivateAttr(default=False)

    @property
    def legacy_key(self) -> bool:
        """Whether the input carried ``custom_extensions``, even an empty one."""
        return self._legacy_key

    @model_validator(mode="wrap")
    @classmethod
    def _read_legacy_key(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        if not isinstance(data, dict) or "custom_extensions" not in data:
            return handler(data)
        data = dict(data)
        legacy = data.pop("custom_extensions")
        if legacy is None:
            legacy = []
        current = data.get("nonportable_extensions")
        if current is None:
            current = []
        if not isinstance(legacy, list) or not isinstance(current, list):
            raise ValueError("custom_extensions and nonportable_extensions must be lists")
        data["nonportable_extensions"] = [*current, *legacy]
        entity = handler(data)
        for entry in (entity.nonportable_extensions or [])[len(current):]:
            entry._legacy = True
        entity._legacy_key = True
        return entity


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


class OSIField(_Extensible):
    """Row-level attribute for grouping, filtering, and metric expressions."""

    model_config = ConfigDict(extra="ignore")

    name: str
    expression: OSIExpression
    dimension: Optional[OSIDimension] = None
    label: Optional[str] = None
    description: Optional[str] = None
    datatype: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None


class OSIMetric(_Extensible):
    """Quantitative measure defined on business data (raw SQL aggregation)."""

    model_config = ConfigDict(extra="ignore")

    name: str
    expression: OSIExpression
    description: Optional[str] = None
    datatype: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None


class OSIDataset(_Extensible):
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


class OSIRelationship(_Extensible):
    """Foreign key relationship between datasets (``from`` = many, ``to`` = one)."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    name: str
    from_dataset: str = Field(..., alias="from")
    to: str
    from_columns: list[str]
    to_columns: list[str]
    ai_context: Optional[OSIAIContext] = None


class OSIDocument(_Extensible):
    """One semantic model; ``legacy`` is set by the parser for wrapper-split models."""

    model_config = ConfigDict(extra="ignore")

    version: Optional[str] = None
    name: str
    description: Optional[str] = None
    ai_context: Optional[OSIAIContext] = None
    datasets: list[OSIDataset]
    relationships: Optional[list[OSIRelationship]] = None
    metrics: Optional[list[OSIMetric]] = None
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


def render_description(explicit: Optional[str], ctx: Optional[OSIAIContext]) -> Optional[str]:
    """Explicit description (lead) + ai_context instructions + synonyms, as the importer stores it."""
    parts: list[str] = []
    if explicit:
        parts.append(explicit)
    ctx_dict = ai_context_to_dict(ctx)
    if ctx_dict:
        instructions = ctx_dict.get("instructions")
        if instructions and instructions != explicit:
            parts.append(instructions)
        synonyms = ctx_dict.get("synonyms")
        if synonyms:
            parts.append("Synonyms: " + ", ".join(synonyms))
    return "\n".join(parts) or None


def explicit_description(stored: Optional[str], ctx: Optional[dict[str, Any]]) -> Optional[str]:
    """The explicit description ``render_description`` turned into ``stored`` (``stored`` when none did)."""
    if not stored or not ctx:
        return stored
    context = OSIAIContextObject.model_validate(ctx)
    lines = stored.split("\n")
    for cut in range(len(lines), -1, -1):
        explicit = "\n".join(lines[:cut]) or None
        if render_description(explicit, context) == stored:
            return explicit
    return stored
