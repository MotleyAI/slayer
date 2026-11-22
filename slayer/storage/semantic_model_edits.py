"""Edits SLayer authors on a semantic-model document's own level: its top-level fields, model-scoped metrics,
relationships and member datasets."""

from __future__ import annotations

import copy
from collections.abc import Sequence
from typing import Any

from slayer.core import slayer_extension as ext
from slayer.core.models import SlayerDataset
from slayer.storage.document_mapping import document_payload, with_dataset, with_document_payload

__all__ = ["UNSET", "MetricSpec", "edited_document", "metric_spec", "with_datasets"]

#: An argument left out (``None`` clears a top-level field).
UNSET: Any = object()


class MetricSpec:
    """A model-scoped metric to write: its Ossie entity and whether it must live in a ``SLAYER`` entry."""

    def __init__(self, entity: dict[str, Any], *, slayer_only: bool) -> None:
        self.entity = entity
        self.slayer_only = slayer_only

    @property
    def name(self) -> str:
        return str(self.entity["name"])

    @property
    def text(self) -> str:
        expression = self.entity["expression"]
        return expression.get("sql") or next(d["expression"] for d in expression.get("dialects") or [])


def metric_spec(raw: dict[str, Any]) -> dict[str, Any]:
    """A metric argument (its ``expression`` a SQL text or an Ossie expression object) as an Ossie metric entity."""
    expression = raw.get("expression")
    entity = {k: v for k, v in raw.items() if v is not None}
    entity["expression"] = {"sql": expression} if isinstance(expression, str) else dict(expression or {})
    return entity


def _fold(name: Any) -> str:
    return str(name).casefold()


def _set_or_pop(doc: dict[str, Any], *, key: str, value: list[Any]) -> None:
    if value:
        doc[key] = value
    else:
        doc.pop(key, None)


def with_datasets(doc: dict[str, Any], datasets: list[SlayerDataset], *, reader: str) -> dict[str, Any]:
    """``doc`` with ``datasets`` added: first without relationships, then with them, so relationships among the added
    datasets land in core."""
    out = doc
    for dataset in datasets:
        out = with_dataset(out, old=None, new=dataset.model_copy(update={"relationships": []}), reader=reader)
    for dataset in datasets:
        if dataset.relationships:
            out = with_dataset(out, old=dataset.model_copy(update={"relationships": []}), new=dataset, reader=reader)
    return out


def edited_document(doc: dict[str, Any], *, description: Any = UNSET, ai_context: Any = UNSET,
                    metrics: Sequence[MetricSpec] = (), remove_metrics: Sequence[str] = (),
                    relationships: Sequence[dict[str, Any]] = (), remove_relationships: Sequence[str] = (),
                    ) -> dict[str, Any]:
    out = copy.deepcopy(doc)
    for key, value in (("description", description), ("ai_context", ai_context)):
        if value is UNSET:
            continue
        if value is None:
            out.pop(key, None)
        else:
            out[key] = value
    gone = {_fold(n) for n in remove_metrics} | {_fold(m.name) for m in metrics}
    payload = document_payload(out)
    payload.metrics = [m for m in payload.metrics if _fold(m.name) not in gone]
    core = [m for m in out.get("metrics") or [] if _fold(m.get("name")) not in gone]
    held = {_fold(m.get("name")): m for m in out.get("metrics") or []}
    for spec in metrics:
        if spec.slayer_only:
            payload.metrics.append(ext.ModelMetricContent(
                name=spec.name, expression=spec.text, description=spec.entity.get("description"),
                datatype=spec.entity.get("datatype"), ai_context=spec.entity.get("ai_context")))
        else:
            core.append({**held.get(_fold(spec.name), {}), **spec.entity})
    _set_or_pop(out, key="metrics", value=core)
    with_document_payload(out, payload)
    dropped = {_fold(n) for n in remove_relationships} | {_fold(r.get("name")) for r in relationships}
    rels = [r for r in out.get("relationships") or [] if _fold(r.get("name")) not in dropped] + list(relationships)
    _set_or_pop(out, key="relationships", value=rels)
    return out
