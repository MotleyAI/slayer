"""Bundled demo datasets for SLayer.

The Jaffle Shop demo is the canonical getting-started dataset; see
``slayer.demo.jaffle_shop`` for details.
"""

from slayer.demo.jaffle_shop import (
    CENTS_COLUMNS,
    DATE_COLUMNS,
    DEFAULT_TIME_DIMENSIONS,
    DEMO_NAME,
    JAFFLE_SCHEMA_SQL,
    LOAD_ORDER,
    TABLE_NAMES,
    DemoDependencyError,
    build_jaffle_shop,
    create_schema,
    ensure_demo_datasource,
    generate_data,
    load_data,
    resolve_demo_db_path,
    shift_dates_to_today,
    verify,
)

__all__ = [
    "CENTS_COLUMNS",
    "DATE_COLUMNS",
    "DEFAULT_TIME_DIMENSIONS",
    "DEMO_NAME",
    "JAFFLE_SCHEMA_SQL",
    "LOAD_ORDER",
    "TABLE_NAMES",
    "DemoDependencyError",
    "build_jaffle_shop",
    "create_schema",
    "ensure_demo_datasource",
    "generate_data",
    "load_data",
    "resolve_demo_db_path",
    "shift_dates_to_today",
    "verify",
]
