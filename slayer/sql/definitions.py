"""Native row-definition text rewrites: sibling references written as paths, and the deprecated field ``sql`` tagged
with its datasource's dialect."""

from __future__ import annotations

from slayer.core.field_expression import DialectEntry, ExpressionObject, unquoted_identifier
from slayer.core.models import DatasetField, SlayerDataset
from slayer.sql.dialects import dialect_for_ds_type, ossie_token
from slayer.sql.root_scope import qualified_siblings

__all__ = ["qualified_siblings", "sibling_names", "with_converted_sql"]


def sibling_names(dataset: SlayerDataset, *, field: str | None) -> set[str]:
    """The fields of ``dataset`` other than ``field`` that a bare name read before 2.0: those reading more than their
    own source column."""
    return {c.name for c in dataset.fields if c.name != field and c.needs_expansion}


def _converted(field: DatasetField, *, dataset: SlayerDataset, dialect: str, token: str) -> DatasetField:
    assert field.sql is not None
    text = qualified_siblings(field.sql, dataset=dataset.name, siblings=sibling_names(dataset, field=field.name),
                              dialect=dialect)
    ident = unquoted_identifier(text)
    expression = text.strip() if ident is not None else ExpressionObject(
        dialects=[DialectEntry(dialect=token, expression=text)])
    return field.model_copy(update={"sql": None, "expression": expression})


def with_converted_sql(dataset: SlayerDataset, *, ds_type: str | None) -> SlayerDataset:
    """``dataset`` with each field's deprecated ``sql`` as its entry for the datasource's dialect, each sibling
    reference written as its path; unchanged when no field carries ``sql``."""
    if not any(c.sql is not None for c in dataset.fields):
        return dataset
    dialect = dialect_for_ds_type(ds_type).sqlglot_name
    token = ossie_token(ds_type)
    fields = [_converted(c, dataset=dataset, dialect=dialect, token=token) if c.sql is not None else c
              for c in dataset.fields]
    return dataset.model_copy(update={"fields": fields})
