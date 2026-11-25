"""SlayerModel v31: a stored ``DateTime`` field over a column type holding instants (BigQuery ``TIMESTAMP``) loads as
``DateTimeTz``. The datasource type decides, so the work runs in the storage load path."""

from typing import Any

from slayer.core.enums import DataType
from slayer.sql.dialects import dialect_for_ds_type
from slayer.storage.migrations import register_migration, stamp_stored

INSTANT_FIELD_VERSION = 31


def _typed(field: Any, *, is_instant: Any) -> Any:
    if not isinstance(field, dict) or field.get("datatype") != DataType.DATETIME.value:
        return field
    db_type = field.get("db_type")
    return {**field, "datatype": DataType.DATETIME_TZ.value} if isinstance(db_type, str) and is_instant(db_type) else field


def typed_instant_fields(data: dict, *, ds_type: str | None) -> dict:
    """``data`` with each ``DateTime`` field over an instant ``db_type`` typed ``DateTimeTz`` when ``ds_type`` is
    BigQuery; other datasources re-type on re-ingest."""
    fields = data.get("fields")
    if ds_type is None or not isinstance(fields, list):
        return data
    dialect = dialect_for_ds_type(ds_type)
    if dialect.sqlglot_name != "bigquery":
        return data
    return {**data, "fields": [_typed(f, is_instant=dialect.is_zoned_db_type) for f in fields]}


def awaits_datasource_type(data: Any) -> bool:
    """Whether typing stored dataset ``data``'s instant fields needs its datasource type: below v31, with a
    ``DateTime`` field over a recorded ``db_type``."""
    data = stamp_stored(data)
    if not isinstance(data, dict) or int(data["version"]) >= INSTANT_FIELD_VERSION:
        return False
    fields = data.get("fields")
    return isinstance(fields, list) and any(
        isinstance(f, dict) and f.get("datatype") == DataType.DATETIME.value and isinstance(f.get("db_type"), str)
        for f in fields
    )


@register_migration(entity="SlayerModel", source_version=30, stored_only=True)
def _dataset_v30_to_v31(data: dict) -> dict:
    """Per-doc no-op; ``typed_instant_fields`` runs in the storage load path."""
    return data
