"""DatasourceConfig v1 → v2 schema migration (DEV-1551).

v2 introduces three new optional fields for Snowflake support:
    - ``connection_name``: profile name in ``~/.snowflake/connections.toml``
    - ``warehouse``: Snowflake warehouse override (URL query string)
    - ``role``: Snowflake role override (URL query string)

The converter is a pure no-op forward — Pydantic's default for missing
optional fields fills them in as ``None`` on validation. We bump the
``version`` field to 2 so subsequent loads short-circuit migration.
Other dialects ignore the new fields entirely.
"""

from typing import Any, Dict

from slayer.storage.migrations import register_migration


@register_migration("DatasourceConfig", 1)
def _datasource_v1_to_v2(data: Dict[str, Any]) -> Dict[str, Any]:
    """v1 → v2: no-op forward; new fields default to None via Pydantic."""
    return data
