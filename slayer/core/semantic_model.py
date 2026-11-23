"""Ossie ``0.3.0.dev0`` semantic-model documents: the strict shape and the validity check."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from slayer.core.enums import DataType
from slayer.core.errors import OssieErrorCode
from slayer.core.field_expression import Expression
from slayer.core.ossie_validity import semantic_errors
from slayer.core.slayer_extension import checked

__all__ = [
    "OSSIE_VERSION", "AIContext", "AIContextObject", "DialectExpression", "Dimension", "NonportableExtension",
    "MetricProblem", "ModelScopedAnalysis", "ModelScopedMetric", "OssieDataset", "OssieDatatype", "OssieExpression", "OssieField", "OssieMetric",
    "OssieRelationship", "SemanticModelDocument", "ossie_validity_errors", "shape_problem",
]

OSSIE_VERSION = "0.3.0.dev0"
_UPGRADE = "run `slayer import-osi` on the file to upgrade it"

OssieDatatype = Literal["String", "Integer", "Decimal", "Float", "Boolean", "Date", "Time", "DateTime", "DateTimeTz",
                        "Opaque"]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class AIContextObject(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True)

    instructions: str | None = None
    synonyms: list[str] | None = None
    examples: list[str] | None = None


AIContext = str | AIContextObject


class NonportableExtension(_Strict):
    vendor_name: str = Field(min_length=1)
    data: dict[str, Any]


class DialectExpression(_Strict):
    dialect: str = Field(min_length=1)
    expression: str = Field(min_length=1)


class OssieExpression(_Strict):
    sql: str | None = Field(default=None, min_length=1)
    dialects: list[DialectExpression] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _has_a_form(self) -> OssieExpression:
        if self.sql is None and self.dialects is None:
            raise ValueError("an expression needs `sql` or `dialects`")
        return self


class Dimension(_Strict):
    is_time: bool | None = None


class _Extensible(_Strict):
    nonportable_extensions: list[NonportableExtension] | None = None


class OssieField(_Extensible):
    name: str = Field(min_length=1)
    expression: OssieExpression
    dimension: Dimension | None = None
    label: str | None = None
    description: str | None = None
    datatype: OssieDatatype | None = None
    ai_context: AIContext | None = None


class OssieMetric(_Extensible):
    name: str = Field(min_length=1)
    expression: OssieExpression
    description: str | None = None
    datatype: OssieDatatype | None = None
    ai_context: AIContext | None = None


class OssieDataset(_Extensible):
    name: str = Field(min_length=1)
    source: str = Field(min_length=1)
    primary_key: list[str] | None = Field(default=None, min_length=1)
    unique_keys: list[list[str]] | None = Field(default=None, min_length=1)
    description: str | None = None
    ai_context: AIContext | None = None
    fields: list[OssieField] | None = None
    metrics: list[OssieMetric] | None = None

    @model_validator(mode="after")
    def _keys_name_each_field_once(self) -> OssieDataset:
        keys = [self.primary_key] if self.primary_key is not None else []
        for key in [*keys, *(self.unique_keys or [])]:
            if not key or len(set(key)) != len(key):
                raise ValueError(f"key {key} must name at least one field, each once")
        return self


class OssieRelationship(_Extensible):
    name: str = Field(min_length=1)
    from_dataset: str = Field(alias="from", min_length=1)
    to: str = Field(min_length=1)
    from_columns: list[str] = Field(min_length=1)
    to_columns: list[str] = Field(min_length=1)
    ai_context: AIContext | None = None


class SemanticModelDocument(_Extensible):
    """One semantic model, exactly Ossie's flat shape."""

    version: Literal["0.3.0.dev0"]
    name: str = Field(min_length=1)
    description: str | None = None
    ai_context: AIContext | None = None
    datasets: list[OssieDataset] = Field(min_length=1)
    relationships: list[OssieRelationship] | None = None
    metrics: list[OssieMetric] | None = None


class MetricProblem(BaseModel):
    """Why Ossie Layer 3 rejects a query naming a model-scoped metric; ``evaluable`` when SLayer evaluates it anyway."""

    code: OssieErrorCode
    reason: str
    evaluable: bool = False


class ModelScopedAnalysis(BaseModel):
    """A model-scoped metric's components (each aggregate call's home dataset), the model-scoped metrics it names bare
    (case-folded), and its problems in reading order."""

    homes: list[str] = []
    metrics: list[str] = []
    problems: list[MetricProblem] = []


class ModelScopedMetric(BaseModel):
    """A document's top-level metric: a query-level expression any query names bare."""

    name: str
    semantic_model: str
    data_source: str
    expression: Expression
    description: str | None = None
    ai_context: AIContext | None = None
    datatype: DataType | None = None
    #: Why SLayer cannot evaluate it yet; set on load, read when a query names it.
    load_warning: str | None = None
    analysis: ModelScopedAnalysis = ModelScopedAnalysis()


def _has_custom_extensions(node: Any) -> bool:
    if isinstance(node, dict):
        return "custom_extensions" in node or any(_has_custom_extensions(v) for v in node.values())
    if isinstance(node, list):
        return any(_has_custom_extensions(v) for v in node)
    return False


def shape_problem(data: Any) -> str | None:
    """Why ``data`` is not a flat ``0.3.0.dev0`` semantic-model document by its outline, else ``None``."""
    if not isinstance(data, dict):
        return "the document is not a mapping, so it is not a semantic-model document"
    if "semantic_model" in data:
        if str(data.get("version")) == OSSIE_VERSION:
            return (f"a {OSSIE_VERSION} document is flat: its datasets sit at the root, not in a `semantic_model` "
                    "list")
        return f"the document uses the earlier `semantic_model` wrapper; {_UPGRADE}"
    if "datasets" not in data:
        return "the document has no `datasets`, so it is not a semantic-model document"
    if _has_custom_extensions(data):
        return f"`custom_extensions` is the earlier spelling of `nonportable_extensions`; {_UPGRADE}"
    version = data.get("version")
    if version is None or str(version) != OSSIE_VERSION:
        return f"version {version!r} is not the supported Ossie version {OSSIE_VERSION!r}"
    return None


def _entity(node: Any, kind: str) -> str:
    name = node.get("name") if isinstance(node, dict) else None
    return f"{kind} {name!r}" if isinstance(name, str) else kind


_KINDS = {"datasets": "dataset", "fields": "field", "metrics": "metric", "relationships": "relationship"}


def _indexed(items: Any, index: int | str) -> Any:
    return items[index] if isinstance(items, list) and isinstance(index, int) and index < len(items) else None


def _item_label(step: str, item: Any, index: int | str) -> str:
    if step == "nonportable_extensions":
        vendor = item.get("vendor_name") if isinstance(item, dict) else None
        return f"extension {vendor!r}" if vendor is not None else f"extension #{index}"
    return _entity(item, _KINDS.get(step, step))


def _where(data: Any, loc: tuple[int | str, ...]) -> tuple[str, str | None]:
    """The entity path a schema error's location names, and its last key."""
    parts: list[str] = []
    node, key = data, None
    i = 0
    while i < len(loc):
        step = loc[i]
        if isinstance(step, str) and i + 1 < len(loc) and isinstance(loc[i + 1], int) and isinstance(node, dict):
            index = loc[i + 1]
            item = _indexed(node.get(step), index=index)
            parts.append(_item_label(step, item=item, index=index))
            node, i = item, i + 2
            continue
        key = str(step)
        node = node.get(step) if isinstance(node, dict) else None
        i += 1
    return ", ".join(parts), key


def _schema_error(data: Any, error: Any) -> str:
    loc = tuple(s for s in error["loc"] if s not in ("str", "AIContextObject", "function-after[_has_a_form()]"))
    where, key = _where(data, loc)
    prefix = f"{where}: " if where else ""
    if error["type"] == "extra_forbidden":
        return f"{prefix}unknown key {key!r}; Ossie {OSSIE_VERSION} does not define it"
    if error["type"] == "missing":
        return f"{prefix}missing required key {key!r}"
    detail = error["msg"].removeprefix("Value error, ")
    return f"{prefix}{key}: {detail}" if key else f"{prefix}{detail}"


def _collect_extension_errors(node: Any, *, label: str, out: list[str]) -> None:
    if not isinstance(node, dict):
        return
    for entry in node.get("nonportable_extensions") or []:
        if (isinstance(entry, dict) and isinstance(entry.get("data"), dict)
                and checked(vendor_name=entry.get("vendor_name") or "?", data=entry["data"]) is None):
            out.append(f"{label}extension {entry.get('vendor_name')!r}: `data` holds a value that is not JSON")
    for kind in ("datasets", "fields", "metrics", "relationships"):
        for child in node.get(kind) or []:
            _collect_extension_errors(child, label=f"{label}{_entity(child, _KINDS[kind])}, ", out=out)


def _extension_errors(data: dict[str, Any]) -> list[str]:
    out: list[str] = []
    _collect_extension_errors(data, label="", out=out)
    return out


def ossie_validity_errors(data: Any) -> list[str]:
    """Every reason ``data`` is not a valid Ossie ``0.3.0.dev0`` semantic-model document; empty when it is."""
    problem = shape_problem(data)
    if problem is not None:
        return [problem]
    try:
        SemanticModelDocument.model_validate(data)
    except ValidationError as exc:
        seen: dict[str, None] = {}
        for error in exc.errors():
            seen.setdefault(_schema_error(data, error), None)
        return list(seen) + _extension_errors(data)
    return _extension_errors(data) + semantic_errors(data)
