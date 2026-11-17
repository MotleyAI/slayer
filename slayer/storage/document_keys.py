"""Stored-document key names by vocabulary: a document below its entity's vocabulary version spells the old keys."""

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
    query_filter: str
    time_field: str
    order_field: str
    datatype: str


LEGACY_KEYS = DocumentKeys(
    fields="columns", metrics="measures", relationships="joins", expression="formula", raw_expression="raw_formula",
    to="target_model", population="source_model", query_metrics="measures", query_filter="filters",
    time_field="dimension", order_field="column", datatype="type",
)
CURRENT_KEYS = DocumentKeys(
    fields="fields", metrics="metrics", relationships="relationships", expression="expression",
    raw_expression="raw_expression", to="to", population="population", query_metrics="metrics", query_filter="filter",
    time_field="field", order_field="field", datatype="datatype",
)

#: The first stored version of each entity that spells ``CURRENT_KEYS``.
VOCABULARY_VERSIONS: dict[str, int] = {"SlayerModel": 26, "SlayerQuery": 16, "Memory": 12}


def keys_for(*, entity: str, version: int) -> DocumentKeys:
    """The key set a stored ``entity`` document at ``version`` spells."""
    return CURRENT_KEYS if version >= VOCABULARY_VERSIONS[entity] else LEGACY_KEYS
