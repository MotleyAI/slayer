"""What a mapping resolves against in one datasource: the stored ontologies and the datasource's semantic models."""

from __future__ import annotations

from typing import Any


from slayer.ontology.mapping import MappingProblem, MappingResolution, ModelFields, resolve_mapping
from slayer.ontology.ontology import Ontology
from slayer.core.datasource_scope import DatasourceScope


def check_mapping(data: dict[str, Any], *, scope: DatasourceScope) -> MappingResolution:
    """Resolve a schema-valid mapping's references in ``scope``, then its structure and expressions."""
    references: list[MappingProblem] = []
    ontology_name = str((data.get("ontology_ref") or {}).get("name"))
    ontology = scope.ontologies.get(ontology_name)
    if ontology is None:
        references.append(MappingProblem(path="ontology_ref", message=f"no stored ontology is named {ontology_name!r}"))
    model_name = str((data.get("semantic_model_ref") or {}).get("name"))
    model = scope.model_of(model_name)
    if model is None:
        candidates = ", ".join(repr(c) for c in [*sorted(scope.documents), scope.data_source])
        references.append(MappingProblem(path="semantic_model_ref", message=(
            f"datasource {scope.data_source!r} holds no semantic model named {model_name!r}; the candidates are "
            f"{candidates}")))
    resolution = resolve_mapping(data, ontology=Ontology.of(ontology) if ontology is not None else None,
                                 model=ModelFields.of(model) if model is not None else None)
    return resolution.model_copy(update={"problems": [*references, *resolution.problems]})
