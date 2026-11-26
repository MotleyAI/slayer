"""One datasource's stored state that cross-document references resolve against."""
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, ConfigDict
from slayer.core.models import SlayerDataset


class DatasourceScope(BaseModel):
    """One datasource's semantic models: ``documents`` by name (each its datasets) and every dataset it holds; and the
    valid stored ontologies by name."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    data_source: str
    ontologies: dict[str, dict[str, Any]]
    documents: dict[str, list[SlayerDataset]]
    datasets: list[SlayerDataset]

    def with_ontology(self, name: str, data: dict[str, Any] | None) -> DatasourceScope:
        ontologies = {k: v for k, v in self.ontologies.items() if k != name}
        if data is not None:
            ontologies[name] = data
        return self.model_copy(update={"ontologies": ontologies})

    def with_document(self, name: str, datasets: list[SlayerDataset] | None) -> DatasourceScope:
        """The scope with document ``name`` holding ``datasets`` (``None``: deleted)."""
        held = {d.name.casefold() for d in self.documents.get(name, [])}
        documents = {k: v for k, v in self.documents.items() if k != name}
        if datasets is not None:
            documents[name] = datasets
        kept = [d for d in self.datasets if d.name.casefold() not in held]
        return self.model_copy(update={"documents": documents, "datasets": [*kept, *(datasets or [])]})

    def with_dataset(self, name: str, dataset: SlayerDataset | None) -> DatasourceScope:
        """The scope with dataset ``name`` replaced by ``dataset`` (``None``: deleted), in its document too."""
        def swapped(items: list[SlayerDataset]) -> list[SlayerDataset]:
            hit = any(d.name.casefold() == name.casefold() for d in items)
            kept = [d for d in items if d.name.casefold() != name.casefold()]
            return [*kept, dataset] if hit and dataset is not None else kept

        documents = {k: swapped(v) for k, v in self.documents.items()}
        datasets = swapped(self.datasets)
        if dataset is not None and not any(d.name.casefold() == name.casefold() for d in self.datasets):
            datasets.append(dataset)
        return self.model_copy(update={"documents": documents, "datasets": datasets})

    def model_of(self, name: str) -> list[SlayerDataset] | None:
        """The datasets ``semantic_model_ref`` ``name`` means: a document of that name, else the datasource."""
        if name in self.documents:
            return self.documents[name]
        return self.datasets if name == self.data_source else None
