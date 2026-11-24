"""Moved to :mod:`slayer.live_schema.schema_drift` in SLayer 2.0; a known 1.x integrator's names stay, each
deprecated."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from slayer.core.deprecation import deprecated_input, raise_moved
from slayer.live_schema import schema_drift

_HOME = "slayer.live_schema.schema_drift"
_KEPT: dict[str, tuple[Callable[[], object], object]] = {
    "EditModelDelete": (
        lambda: deprecated_input("slayer.engine.schema_drift.EditModelDelete", f"{_HOME}.EditDatasetDelete",
                                 removal="3.0"), schema_drift.EditDatasetDelete),
    "WholeModelDelete": (
        lambda: deprecated_input("slayer.engine.schema_drift.WholeModelDelete", f"{_HOME}.WholeDatasetDelete",
                                 removal="3.0"), schema_drift.WholeDatasetDelete),
    "LiveTable": (
        lambda: deprecated_input("slayer.engine.schema_drift.LiveTable", f"{_HOME}.LiveTable", removal="3.0"),
        schema_drift.LiveTable),
    "compute_datasource_drops": (
        lambda: deprecated_input("slayer.engine.schema_drift.compute_datasource_drops",
                                 f"{_HOME}.compute_datasource_drops", removal="3.0"),
        schema_drift.compute_datasource_drops),
    "data_type_bucket": (
        lambda: deprecated_input("slayer.engine.schema_drift.data_type_bucket", f"{_HOME}.data_type_bucket",
                                 removal="3.0"), schema_drift.data_type_bucket),
    "diff_sql_table_model": (
        lambda: deprecated_input("slayer.engine.schema_drift.diff_sql_table_model", f"{_HOME}.diff_sql_table_dataset",
                                 removal="3.0"), schema_drift.diff_sql_table_dataset),
    "retype_findings": (
        lambda: deprecated_input("slayer.engine.schema_drift.retype_findings", f"{_HOME}.retype_findings",
                                 removal="3.0"), schema_drift.retype_findings),
}

if not TYPE_CHECKING:  # the moved names stay static errors

    def __getattr__(name: str) -> object:
        if name.startswith("__"):
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
        if name not in _KEPT:
            raise_moved("slayer.engine.schema_drift", "slayer.live_schema.schema_drift", removal="3.0", attribute=True)
        warn, value = _KEPT[name]
        warn()
        return value
