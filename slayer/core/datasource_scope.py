"""One datasource's stored state that cross-document references resolve against."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from slayer.core.models import SlayerDataset
from slayer.core.names import name_taken
from slayer.core.semantic_model import ModelScopedMetric


class DatasourceScope(BaseModel):
    """One datasource's semantic models: ``documents`` by name (each its datasets), each document's stored
    model-scoped ``metrics``, and every dataset it holds; and the valid stored ontologies by name."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    data_source: str
    ontologies: dict[str, dict[str, Any]]
    documents: dict[str, list[SlayerDataset]]
    datasets: list[SlayerDataset]
    metrics: dict[str, list[ModelScopedMetric]] = {}

    def with_ontology(self, name: str, data: dict[str, Any] | None) -> DatasourceScope:
        ontologies = {k: v for k, v in self.ontologies.items() if k != name}
        if data is not None:
            ontologies[name] = data
        return self.model_copy(update={"ontologies": ontologies})

    def with_document(self, name: str, datasets: list[SlayerDataset] | None,
                      metrics: list[ModelScopedMetric] | None = None) -> DatasourceScope:
        """The scope with document ``name`` holding ``datasets`` and ``metrics`` (``None``: kept), or deleted when
        ``datasets`` is ``None``."""
        replaced = [d.name for d in [*self.documents.get(name, []), *(datasets or [])]]
        documents = {k: v for k, v in self.documents.items() if k != name}
        kept_metrics = {k: v for k, v in self.metrics.items() if k != name}
        if datasets is not None:
            documents[name] = datasets
            kept_metrics[name] = metrics if metrics is not None else self.metrics.get(name, [])
        kept = [d for d in self.datasets if not name_taken(d.name, replaced)]
        return self.model_copy(update={"documents": documents, "metrics": kept_metrics,
                                       "datasets": [*kept, *(datasets or [])]})

    def with_dataset(self, name: str, dataset: SlayerDataset | None) -> DatasourceScope:
        """The scope with dataset ``name`` replaced by ``dataset`` (``None``: deleted), in its document too; one no
        document holds joins the document its ``semantic_model`` names."""
        def swapped(items: list[SlayerDataset]) -> list[SlayerDataset]:
            kept = [d for d in items if not name_taken(d.name, [name])]
            return [*kept, dataset] if len(kept) < len(items) and dataset is not None else kept

        documents = {k: swapped(v) for k, v in self.documents.items()}
        home = dataset.semantic_model if dataset is not None else None
        if dataset is not None and home is not None and home in documents and not any(
                name_taken(name, [d.name for d in v]) for v in self.documents.values()):
            documents[home] = [*documents[home], dataset]
        datasets = swapped(self.datasets)
        if dataset is not None and not name_taken(name, [d.name for d in self.datasets]):
            datasets.append(dataset)
        return self.model_copy(update={"documents": documents, "datasets": datasets})

    def model_of(self, name: str) -> list[SlayerDataset] | None:
        """The datasets ``semantic_model_ref`` ``name`` means: a document of that name, else the datasource."""
        if name in self.documents:
            return self.documents[name]
        return self.datasets if name == self.data_source else None
