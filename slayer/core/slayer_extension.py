"""The versioned ``SLAYER`` non-portable-extension payload: per Ossie entity, the SLayer content Ossie has no place for.

Each payload carries a fingerprint over the Ossie content it was written with
and over its own content; the importer restores what duplicates Ossie content
only while the fingerprint still matches the document.
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, TypeAdapter

from slayer.core.enums import DataType
from slayer.core.field_expression import Expression as FieldExpression
from slayer.core.field_expression import unquoted_identifier
from slayer.core.format import NumberFormat
from slayer.core.granularity import GranularitySpec
from slayer.core.models import Aggregation, DatasetField, DatasetRelationship, DatasetMetric
from slayer.core import vocabulary
from slayer.core.vocabulary import Spellings
from slayer.core.ossie_expression import OSINonportableExtension

VENDOR = "SLAYER"
PAYLOAD_VERSION = 3
#: Every payload version ``decode`` reads.
KNOWN_VERSIONS = (1, 2, PAYLOAD_VERSION)
_FINGERPRINT_VERSION = 1


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[3] = PAYLOAD_VERSION
    #: The name of the Ossie entity the payload was written for.
    entity: str
    fingerprint: str = ""
    #: An older payload's content as written, which its fingerprint covers.
    _written: dict[str, Any] | None = PrivateAttr(default=None)
    #: An older dataset payload's SLayer-only field ``sql`` by field name, converted with the import's dialect.
    _legacy_sql: dict[str, str] = PrivateAttr(default_factory=dict)

    @property
    def legacy_sql(self) -> dict[str, str]:
        return self._legacy_sql


class RelationshipDeclaration(BaseModel):
    """One SLayer relationship an Ossie relationship stands for, on its declaring dataset."""

    model_config = ConfigDict(extra="forbid")

    dataset: str
    relationship: DatasetRelationship


class RelationshipPayload(_Payload):
    kind: Literal["relationship"] = "relationship"
    declarations: list[RelationshipDeclaration]


class MetricPayload(_Payload):
    kind: Literal["metric"] = "metric"
    dataset: str
    metric: DatasetMetric


class FieldContent(BaseModel):
    """A field's SLayer field attributes; ``expression`` / ``filter`` are its value/filter split, ``datatype`` its
    typing; ``sql`` is a version-1 or -2 payload's value, converted with the import's datasource dialect."""

    model_config = ConfigDict(extra="forbid")

    expression: FieldExpression | None = None
    sql: str | None = None
    filter: str | None = None
    datatype: DataType | None = None
    hidden: bool = False
    format: NumberFormat | None = None
    granularity: GranularitySpec | None = None
    allowed_aggregations: list[str] | None = None
    meta: dict[str, Any] | None = None
    element_type: DataType | None = None
    db_type: str | None = None
    sampled: str | None = None
    sampled_values: list[str] | None = None
    distinct_count: int | None = None


class FieldPayload(_Payload):
    kind: Literal["field"] = "field"
    field: FieldContent


class SourceStructure(BaseModel):
    """A dataset's source: a table or SQL with row filters, or saved queries with their variables."""

    model_config = ConfigDict(extra="forbid")

    sql_table: str | None = None
    sql: str | None = None
    filters: list[str] | None = None
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
    #: SLayer-only content: fields reading joined datasets, metrics and relationships Ossie cannot express.
    fields: list[DatasetField] = []
    metrics: list[DatasetMetric] = []
    relationships: list[DatasetRelationship] = []
    enforced_keys: list[list[str]] = []


class ModelMetricContent(BaseModel):
    """A model-scoped metric Ossie core cannot hold (it reads another by name, or a dataset that is gone)."""

    model_config = ConfigDict(extra="forbid")

    name: str
    expression: FieldExpression
    description: str | None = None
    datatype: DataType | None = None
    ai_context: Any = None


class DocumentBoundary(BaseModel):
    """One stored document merged into an exported document: its own top level and the members it declares."""

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    ai_context: Any = None
    datasets: list[str] = []
    relationships: list[str] = []
    metrics: list[str] = []
    nonportable_extensions: list[dict[str, Any]] = []


class DocumentPayload(_Payload):
    kind: Literal["semantic_model"] = "semantic_model"
    metrics: list[ModelMetricContent] = []
    #: The stored documents an export merged, so a re-import splits them back.
    documents: list[DocumentBoundary] = []


Payload = Annotated[
    Union[DatasetPayload, DocumentPayload, FieldPayload, MetricPayload, RelationshipPayload],
    Field(discriminator="kind"),
]
_ADAPTER: TypeAdapter[Payload] = TypeAdapter(Payload)


def _content(payload: _Payload) -> dict[str, Any]:
    if payload._written is not None:
        return payload._written
    return payload.model_dump(mode="json", exclude={"fingerprint"}, exclude_none=True)


def relationship_digest(*, from_dataset: str, to: str, from_columns: list[str], to_columns: list[str]) -> list[Any]:
    return [from_dataset, to, list(from_columns), list(to_columns)]


def fingerprint(payload: _Payload, *, ossie: Any) -> str:
    """A versioned hash over the entity's normalized Ossie content and the payload's content."""
    blob = json.dumps({"v": _FINGERPRINT_VERSION, "ossie": ossie, "content": _content(payload)},
                      sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def encode(payload: _Payload, *, ossie: Any) -> dict[str, Any]:
    """``payload`` fingerprinted against ``ossie`` as a ``SLAYER`` non-portable extension entry."""
    stamped = payload.model_copy(update={"fingerprint": fingerprint(payload, ossie=ossie)})
    return {"vendor_name": VENDOR, "data": stamped.model_dump(mode="json", exclude_none=True)}


def _converted_each(items: Any, convert: Any, spellings: Spellings) -> Any:
    return [convert(i, spellings) for i in items] if isinstance(items, list) else items


def _version_2(data: dict[str, Any]) -> dict[str, Any]:
    """A version-1 payload in the version-2 (Ossie) vocabulary and shapes."""
    spellings = Spellings(stored=True)
    out = {**data, "version": 2}
    kind = out.get("kind")
    if kind == "field":
        out = spellings.rename(out, {"column": "field"})
        out["field"] = vocabulary.field(out.get("field"), spellings)
    elif kind == "metric":
        out = spellings.rename(out, {"model": "dataset", "measure": "metric"})
        out["metric"] = vocabulary.metric(out.get("metric"), spellings)
    elif kind == "relationship" and isinstance(out.get("declarations"), list):
        declarations = [spellings.rename(d, {"model": "dataset", "join": "relationship"}) if isinstance(d, dict) else d
                        for d in out["declarations"]]
        out["declarations"] = [
            {**d, "relationship": vocabulary.relationship(d["relationship"], spellings)}
            if isinstance(d, dict) and isinstance(d.get("relationship"), dict) else d for d in declarations
        ]
    elif kind == "dataset":
        out = vocabulary.dataset(out, spellings)
        if isinstance(out.get("source"), dict):
            source = {**out["source"], "source_queries": _converted_each(
                out["source"].get("source_queries"), vocabulary.query, spellings)}
            out["source"] = {k: v for k, v in source.items() if v is not None and not (k == "filters" and v == [])}
        out = {k: v for k, v in out.items() if v is not None}
    return out


def decode(data: dict[str, Any]) -> Payload:
    """The payload in ``data`` (a version-1 or -2 payload read silently as version 3, its fingerprint over its content
    as written); raises ``ValueError`` when malformed or of an unknown version or kind."""
    version = data.get("version")
    if version not in (1, 2):
        return _ADAPTER.validate_python(data)
    upgraded = {**(_version_2(data) if version == 1 else data), "version": PAYLOAD_VERSION}
    legacy: dict[str, str] = {}
    if upgraded.get("kind") == "dataset" and isinstance(upgraded.get("fields"), list):
        upgraded["fields"] = [_without_sql(f, legacy) if isinstance(f, dict) else f for f in upgraded["fields"]]
    payload = _ADAPTER.validate_python(upgraded)
    payload._written = {k: v for k, v in data.items() if k != "fingerprint"}
    payload._legacy_sql = legacy
    return payload


def _without_sql(field: dict[str, Any], legacy: dict[str, str]) -> dict[str, Any]:
    """``field`` with its ``sql`` as the shorthand when it names one column, else set aside in ``legacy``."""
    sql = field.get("sql")
    if not isinstance(sql, str) or field.get("expression") is not None:
        return field
    out = {k: v for k, v in field.items() if k != "sql"}
    if unquoted_identifier(sql) is not None:
        return {**out, "expression": sql.strip()}
    legacy[str(field.get("name"))] = sql
    return out


def matches(payload: _Payload, *, ossie: Any) -> bool:
    return payload.fingerprint == fingerprint(payload, ossie=ossie)


def sole_declaration(payload: RelationshipPayload, *, matched: bool) -> RelationshipDeclaration | None:
    """The one declaration a relationship's entry restores; ``None`` when the core relationship applies alone."""
    return payload.declarations[0] if matched and len(payload.declarations) == 1 else None


def is_slayer(extension: OSINonportableExtension | dict[str, Any]) -> bool:
    vendor = extension.get("vendor_name") if isinstance(extension, dict) else extension.vendor_name
    return isinstance(vendor, str) and vendor.strip().upper() == VENDOR


def _json_value(value: Any, *, active: set[int], valid: set[int]) -> bool:
    """Containers on the path are ``active`` (a revisit is a YAML-alias cycle); ``valid`` ones passed already."""
    if value is None or isinstance(value, (str, bool, int)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if not isinstance(value, (list, dict)) or id(value) in active:
        return False
    if id(value) in valid:
        return True
    active.add(id(value))
    if isinstance(value, list):
        ok = all(_json_value(v, active=active, valid=valid) for v in value)
    else:
        ok = all(isinstance(k, str) and _json_value(v, active=active, valid=valid) for k, v in value.items())
    active.discard(id(value))
    if ok:
        valid.add(id(value))
    return ok


def _reject_constant(token: str) -> Any:
    raise ValueError(f"{token} is not JSON")


def decoded_legacy(data: Any) -> Any:
    """A legacy entry's JSON-string ``data`` decoded; ``None`` when it does not decode; other ``data`` unchanged."""
    if not isinstance(data, str):
        return data
    try:
        return json.loads(data, parse_constant=_reject_constant)
    except (ValueError, RecursionError):
        return None


def checked(*, vendor_name: Any, data: Any) -> dict[str, Any] | None:
    """The entry ``{vendor_name, data}`` when the vendor is a non-blank string and ``data`` a JSON object, else None."""
    if not (isinstance(vendor_name, str) and vendor_name.strip() and isinstance(data, dict)):
        return None
    try:
        if not _json_value(data, active=set(), valid=set()):
            return None
    except RecursionError:
        return None
    return {"vendor_name": vendor_name, "data": data}
