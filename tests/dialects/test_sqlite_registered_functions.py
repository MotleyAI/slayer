"""The SQLite dialect's registry of SLayer-registered functions covers every function a connection receives."""

from typing import Any

from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.base import SqlDialect


class _Recorder:
    """A DBAPI connection stand-in recording the names registered on it."""

    def __init__(self) -> None:
        self.names: set[str] = set()

    def create_function(self, name: str, *_args: Any, **_kwargs: Any) -> None:
        self.names.add(name.lower())

    def create_aggregate(self, name: str, *_args: Any, **_kwargs: Any) -> None:
        self.names.add(name.lower())


def test_registry_is_exactly_what_a_connection_receives() -> None:
    dialect = dialect_for_ds_type("sqlite")
    recorder = _Recorder()
    dialect.register_udfs(recorder)
    assert recorder.names
    assert recorder.names == dialect.slayer_registered_functions


def test_registry_holds_slayer_functions_and_math_overrides() -> None:
    registry = dialect_for_ds_type("sqlite").slayer_registered_functions
    assert {"slayer_date_add", "slayer_timestamp_text", "floor"} <= registry


def test_dialects_without_registered_functions_have_an_empty_registry() -> None:
    assert SqlDialect().slayer_registered_functions == frozenset()
    assert dialect_for_ds_type("duckdb").slayer_registered_functions == frozenset()
