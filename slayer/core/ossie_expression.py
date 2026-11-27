"""Ossie expression and non-portable extension models shared by storage and the importers."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, PrivateAttr, field_validator, model_validator


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
