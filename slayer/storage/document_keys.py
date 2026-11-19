"""Stored-document key names by vocabulary: a document below its entity's vocabulary version spells the old keys."""

from typing import Any

from pydantic import BaseModel, ConfigDict


class DocumentKeys(BaseModel):
    """The keys one vocabulary spells in stored dataset, query and memory documents."""

    model_config = ConfigDict(frozen=True)

    fields: str
    metrics: str
    relationships: str
    expression: str
    raw_expression: str
    to: str
    population: str
    query_metrics: str
    query_metric_text: str
    dimension_text: str
    query_filter: str
    time_field: str
    order_field: str
    datatype: str


LEGACY_KEYS = DocumentKeys(
    fields="columns", metrics="measures", relationships="joins", expression="formula", raw_expression="raw_formula",
    to="target_model", population="source_model", query_metrics="measures", query_metric_text="formula",
    dimension_text="expression", query_filter="filters",
    time_field="dimension", order_field="column", datatype="type",
)
CURRENT_KEYS = DocumentKeys(
    fields="fields", metrics="metrics", relationships="relationships", expression="expression",
    raw_expression="raw_expression", to="to", population="population", query_metrics="metrics",
    query_metric_text="metric", dimension_text="field", query_filter="filter",
    time_field="field", order_field="field", datatype="datatype",
)

#: The first stored version of each entity that spells ``CURRENT_KEYS``.
VOCABULARY_VERSIONS: dict[str, int] = {"SlayerModel": 26, "SlayerQuery": 16, "Memory": 12}


def keys_for(*, entity: str, version: int) -> DocumentKeys:
    """The key set a stored ``entity`` document at ``version`` spells."""
    return CURRENT_KEYS if version >= VOCABULARY_VERSIONS[entity] else LEGACY_KEYS


def query_keys(query: dict, *, enclosing: DocumentKeys) -> DocumentKeys:
    """The keys a nested stored query spells: its own version's, else (version-less) its enclosing document's."""
    version = query.get("version")
    return keys_for(entity="SlayerQuery", version=version) if isinstance(version, int) else enclosing


def _respelled(item: Any, renames: dict[str, str]) -> Any:
    if not isinstance(item, dict):
        return item
    return {renames.get(k, k) if renames.get(k, k) not in item else k: v for k, v in item.items()}


def dataset_in(raw: Any, keys: DocumentKeys) -> Any:
    """A raw stored dataset of any version spelled in ``keys``' vocabulary, members included (for peer lookups)."""
    if not isinstance(raw, dict):
        return raw
    source = LEGACY_KEYS if keys == CURRENT_KEYS else CURRENT_KEYS
    out = _respelled(raw, {source.fields: keys.fields, source.metrics: keys.metrics,
                           source.relationships: keys.relationships})
    members = {
        keys.fields: {source.datatype: keys.datatype},
        keys.metrics: {source.expression: keys.expression, source.datatype: keys.datatype},
        "aggregations": {source.expression: keys.expression},
        keys.relationships: {source.to: keys.to},
    }
    for key, renames in members.items():
        if isinstance(out.get(key), list):
            out[key] = [_respelled(m, renames) for m in out[key]]
    return out


def relationship_pairs(relationship: Any) -> list[tuple[Any, Any]]:
    """A raw stored relationship's key pairs, from its ``from_columns`` / ``to_columns`` or its pre-2.0 ``join_pairs``."""
    if not isinstance(relationship, dict):
        return []
    if "from_columns" in relationship or "to_columns" in relationship:
        sources, targets = relationship.get("from_columns"), relationship.get("to_columns")
        if isinstance(sources, list) and isinstance(targets, list) and len(sources) == len(targets):
            return list(zip(sources, targets))
        return []
    pairs = relationship.get("join_pairs")
    return [(p[0], p[1]) for p in pairs if isinstance(p, list) and len(p) == 2] if isinstance(pairs, list) else []


def with_relationship_pairs(relationship: dict, pairs: list[tuple[Any, Any]]) -> dict:
    """``relationship`` with its key pairs replaced, in the shape it is written in."""
    if "join_pairs" in relationship and "from_columns" not in relationship:
        return {**relationship, "join_pairs": [[a, b] for a, b in pairs]}
    return {**relationship, "from_columns": [a for a, _ in pairs], "to_columns": [b for _, b in pairs]}
