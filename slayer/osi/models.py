"""Pydantic v2 models for Ossie (OSI) semantic-model documents.

Mirrors the upstream core spec (``core-spec/ossie-schema.json``); every model
uses ``extra="ignore"`` for forward compatibility. One ``OSIDocument`` holds one
semantic model; ``legacy`` marks a model split out of a ``semantic_model``
wrapper by the parser.
"""

from typing import Any, Optional, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ModelWrapValidatorHandler,
    PrivateAttr,
    field_validator,
    model_validator,
)

from slayer.core.ossie_expression import OSIExpression, OSINonportableExtension
from slayer.core.ossie_content import OSIAIContext

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


