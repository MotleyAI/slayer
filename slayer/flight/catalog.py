"""Backward-compat shim — the catalog moved to ``slayer.facade.catalog``.

The catalog build is now facade-agnostic and shared by the Flight SQL and
Postgres facades (DEV-1486). This module re-exports the shared symbols under
their historical ``Flight*`` names so existing importers keep working.
"""

from __future__ import annotations

from slayer.facade.catalog import (
    CATALOG_NAME,
    DEFAULT_BFS_DEPTH,
    FacadeCatalog,
    FacadeDimension,
    FacadeMetric,
    FacadeSchema,
    FacadeTable,
    build_catalog,
)

# Historical names. The Flight facade and its tests refer to these.
FlightCatalog = FacadeCatalog
FlightSchema = FacadeSchema
FlightTable = FacadeTable
FlightMetric = FacadeMetric
FlightDimension = FacadeDimension

__all__ = [
    "CATALOG_NAME",
    "DEFAULT_BFS_DEPTH",
    "FlightCatalog",
    "FlightSchema",
    "FlightTable",
    "FlightMetric",
    "FlightDimension",
    "build_catalog",
]
