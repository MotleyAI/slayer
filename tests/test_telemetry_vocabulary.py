"""Telemetry's fixed lookup tables stay within the report vocabulary and name real things."""

from __future__ import annotations

import importlib
from typing import get_args

import pytest

from slayer.core.models import DatasourceConfig
from slayer.sql.dialects import SQLGLOT_NAMES
from slayer.telemetry import features, recorder
from slayer.telemetry.payload import CLIENTS, DIALECTS, ErrorToken


@pytest.mark.parametrize("dotted", sorted(recorder._ERROR_TOKENS))
def test_error_table_names_real_exception_classes(dotted: str) -> None:
    module, _, name = dotted.rpartition(".")
    cls = getattr(importlib.import_module(module), name)
    assert issubclass(cls, BaseException)
    assert recorder._class_token(cls, recorder._ERROR_TOKENS) == recorder._ERROR_TOKENS[dotted]


@pytest.mark.parametrize("dotted", sorted(recorder._STORAGE_TOKENS))
def test_storage_table_names_real_classes(dotted: str) -> None:
    module, _, name = dotted.rpartition(".")
    assert isinstance(getattr(importlib.import_module(module), name), type)


def test_error_tokens_are_vocabulary() -> None:
    assert set(recorder._ERROR_TOKENS.values()) | {"http_4xx", "http_5xx", "other"} <= set(get_args(ErrorToken))


@pytest.mark.parametrize(("status", "token"), [(404, "http_4xx"), (422, "http_4xx"), (500, "http_5xx")])
def test_http_errors_count_by_status_class(status: int, token: str) -> None:
    assert recorder.error_token(recorder.HttpError(status)) == token


def test_client_table_maps_into_vocabulary() -> None:
    assert set(recorder._CLIENT_NAMES.values()) <= CLIENTS


def test_every_dialect_is_vocabulary() -> None:
    assert set(SQLGLOT_NAMES) <= DIALECTS


@pytest.mark.parametrize(("ds_type", "dialect"), [
    ("sqlite", "sqlite"), ("postgres", "postgres"), ("postgresql", "postgres"),
    ("duckdb", "duckdb"), ("no-such-db", "other"), (None, "other"),
])
def test_dialect_of_datasource_type(ds_type: str | None, dialect: str) -> None:
    assert features.dialect(DatasourceConfig(name="d", type=ds_type)) == dialect


@pytest.mark.parametrize(("database", "demo"), [
    ("/srv/store/demo/jaffle_shop.duckdb", True),
    ("/srv/store/jaffle_shop.duckdb", False),
    ("/srv/store/demo/other.duckdb", False),
])
def test_demo_database_detected_by_bundled_layout(database: str, demo: bool) -> None:
    assert features.is_demo(DatasourceConfig(name="d", type="duckdb", database=database)) is demo


@pytest.mark.parametrize(("formula", "transforms", "aggregations"), [
    ("count(*)", [], ["count"]),
    ("amount:sum", [], ["sum"]),
    ("last(sum(amount))", ["last"], ["sum"]),
    ("last(amount, ordered_at)", [], ["last"]),
    ("cumsum(sum(amount)) / count(*)", ["cumsum"], ["sum", "count"]),
    ("sum(iif(status = 'x(', 1, 0))", [], ["sum"]),
    ("my_agg(amount)", [], ["custom"]),
    ("sum(", [], []),
])
def test_function_names_classified(formula: str, transforms: list[str], aggregations: list[str]) -> None:
    found = features.query_features({"source_model": "m", "measures": [formula]})
    assert (sorted(found.transforms), sorted(found.aggregations)) == (sorted(transforms), sorted(aggregations))
