"""The versioned ``SLAYER`` custom-extension payload: per Ossie entity, the SLayer content Ossie has no place for.

Each payload carries a fingerprint over the Ossie content it was written with
and over its own content; the importer restores what duplicates Ossie content
only while the fingerprint still matches the document.
"""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Any, Literal, Union

import sqlglot
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from sqlglot.errors import SqlglotError

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat
from slayer.core.granularity import GranularitySpec
from slayer.core.models import Aggregation, Column, ModelJoin, ModelMeasure
from slayer.osi.dialects import PORTABLE_READER, reader_for
from slayer.osi.models import OSICustomExtension, OSIExpression

VENDOR = "SLAYER"
PAYLOAD_VERSION = 1
_FINGERPRINT_VERSION = 1


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = PAYLOAD_VERSION
    #: The name of the Ossie entity the payload was written for.
    entity: str
    fingerprint: str = ""


class JoinDeclaration(BaseModel):
    """One SLayer join a relationship stands for, on its declaring model."""

    model_config = ConfigDict(extra="forbid")

    model: str
    join: ModelJoin


class RelationshipPayload(_Payload):
    kind: Literal["relationship"] = "relationship"
    declarations: list[JoinDeclaration]


class MetricPayload(_Payload):
    kind: Literal["metric"] = "metric"
    model: str
    measure: ModelMeasure


class ColumnContent(BaseModel):
    """A field's SLayer column attributes; ``sql`` / ``filter`` are its value/filter split, ``type`` its typing."""

    model_config = ConfigDict(extra="forbid")

    sql: str | None = None
    filter: str | None = None
    type: DataType | None = None
    hidden: bool = False
    format: NumberFormat | None = None
    granularity: GranularitySpec | None = None
    allowed_aggregations: list[str] | None = None
    meta: dict[str, Any] | None = None


class FieldPayload(_Payload):
    kind: Literal["field"] = "field"
    column: ColumnContent


class SourceStructure(BaseModel):
    """A model's source: a table or SQL with row filters, or saved queries with their variables."""

    model_config = ConfigDict(extra="forbid")

    sql_table: str | None = None
    sql: str | None = None
    filters: list[str] = []
    source_queries: list[dict[str, Any]] | None = None
    query_variables: dict[str, Any] = {}


class DatasetPayload(_Payload):
    kind: Literal["dataset"] = "dataset"
    source: SourceStructure
    hidden: bool = False
    access_tags: list[str] = []
    default_time_dimension: str | None = None
    meta: dict[str, Any] | None = None
    aggregations: list[Aggregation] = []
    #: SLayer-only content: columns reading joined models, measures Ossie cannot express.
    columns: list[Column] = []
    measures: list[ModelMeasure] = []


Payload = Annotated[
    Union[DatasetPayload, FieldPayload, MetricPayload, RelationshipPayload], Field(discriminator="kind"),
]
_ADAPTER: TypeAdapter[Payload] = TypeAdapter(Payload)


def _content(payload: _Payload) -> dict[str, Any]:
    return payload.model_dump(mode="json", exclude={"fingerprint"}, exclude_none=True)


def _normalized(text: str, reader: str | None) -> str:
    if reader is None:
        return text.strip()
    try:
        return sqlglot.parse_one(text, read=reader).sql(dialect=reader)
    except SqlglotError:
        return text.strip()


def expression_digest(expression: OSIExpression) -> list[Any]:
    """An Ossie expression with each text normalized through its own reader."""
    return [
        _normalized(expression.sql, PORTABLE_READER) if expression.sql else None,
        [[d.dialect.strip().upper(), _normalized(d.expression, reader_for(d.dialect))] for d in expression.dialects],
    ]


def source_digest(source: str, *, reader: str) -> str:
    return _normalized(source, reader)


def relationship_digest(*, from_dataset: str, to: str, from_columns: list[str], to_columns: list[str]) -> list[Any]:
    return [from_dataset, to, list(from_columns), list(to_columns)]


def fingerprint(payload: _Payload, *, ossie: Any) -> str:
    """A versioned hash over the entity's normalized Ossie content and the payload's content."""
    blob = json.dumps({"v": _FINGERPRINT_VERSION, "ossie": ossie, "content": _content(payload)},
                      sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def encode(payload: _Payload, *, ossie: Any) -> OSICustomExtension:
    """``payload`` fingerprinted against ``ossie`` as a ``SLAYER`` custom extension."""
    stamped = payload.model_copy(update={"fingerprint": fingerprint(payload, ossie=ossie)})
    data = json.dumps(stamped.model_dump(mode="json", exclude_none=True), sort_keys=True, separators=(",", ":"))
    return OSICustomExtension(vendor_name=VENDOR, data=data)


def decode(data: str) -> Payload:
    """The payload in ``data``; raises ``ValueError`` when malformed or of an unknown version or kind."""
    return _ADAPTER.validate_json(data)


def matches(payload: _Payload, *, ossie: Any) -> bool:
    return payload.fingerprint == fingerprint(payload, ossie=ossie)


def is_slayer(extension: OSICustomExtension | dict[str, Any]) -> bool:
    vendor = extension.get("vendor_name") if isinstance(extension, dict) else extension.vendor_name
    return isinstance(vendor, str) and vendor.strip().upper() == VENDOR
