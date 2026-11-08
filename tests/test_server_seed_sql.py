"""Shared server seed SQL: existing backends stay byte-identical; Trino gets typed temporal literals."""

from __future__ import annotations

import hashlib
from datetime import date

import pytest

from tests._dev1737_fixtures import DT, server_seed_statements, server_table_statements
from tests.integration._consecutive_periods_calendar import seed_statements as calendar_statements
from tests.integration._dev2015_server import server_statements

_TODAY = date(2024, 7, 1)


def _digest(statements: list[str]) -> str:
    return hashlib.sha256("\n".join(statements).encode()).hexdigest()


@pytest.mark.parametrize("backend,digest", [
    ("postgres", "c75b24464cd3c8cbea4801478da2065e4050a0ff81c04b81009f0235ce69dbc6"),
    ("mysql", "4cb027f29bfd29d4eacbc15fa39cfd6454df319ca9eb92c2b4f3fdd7c002f2a9"),
    ("clickhouse", "eaf9c697558d2990dc91adb5c6541e9b97b2aae4d4838d6ccba7fe7a16d7d568"),
    ("tsql", "89fb1ec46e12c5923816ac9d14c028a43f65040fab93d34aa70e7f5c055218f5"),
])
def test_date_matrix_seed_unchanged(backend: str, digest: str) -> None:
    assert _digest(server_seed_statements(backend, today=_TODAY)) == digest


@pytest.mark.parametrize("backend,digest", [
    ("postgres", "0b0e994f4b44f7b41191c7a1768419bdc0007c114271d724c15a591d1155b815"),
    ("mysql", "de6748f58b85a5e7e57d142537d86f2266407be5f89d0f9ad81b7214d325ff22"),
    ("clickhouse", "2d95668ee52dea0b4ca51db653116a000c8f333045eea8b51654b080253d02ce"),
    ("tsql", "7834b1a1e35ad38de7816d7c7e79c21003f54837a5a0e2ea0a73d144da1f23de"),
])
def test_time_spine_seed_unchanged(backend: str, digest: str) -> None:
    assert _digest(server_statements(backend)) == digest


@pytest.mark.parametrize("kwargs,digest", [
    ({}, "cea62cdbfa144accecd12969e2255470883fdc02b82b22424ea3601685c753bc"),
    ({"timestamp_type": "DATETIME"}, "95842339b9127738b9a67d19022b44e0650834798f5022ad6a7caaf7aff47bb3"),
    ({"timestamp_type": "DATETIME2"}, "bc1762d9c3eb8a93b9b2c360be0253126c1db1872159503148ff8fad5fe5edb0"),
    (
        {
            "date_type": "Date", "timestamp_type": "DateTime", "int_type": "Int32",
            "text_type": "String", "float_type": "Float64",
            "table_options": "ENGINE = MergeTree() ORDER BY id",
        },
        "864e250c704c001aa596e60079d3a6720fbc9f491286c47c53e07ed6c468e818",
    ),
])
def test_calendar_seed_unchanged(kwargs: dict, digest: str) -> None:
    assert _digest(calendar_statements(**kwargs)) == digest


def test_trino_seed_uses_trino_types_and_typed_temporals() -> None:
    create, insert = server_table_statements(DT, backend="trino")
    assert create == (
        "CREATE TABLE dt (id INTEGER, d1 DATE, d2 DATE, t1 TIMESTAMP(6), t2 TIMESTAMP(6), n DOUBLE)"
    )
    assert insert.startswith(
        "INSERT INTO dt VALUES (1, DATE '2024-01-31', DATE '2024-02-01', "
        "TIMESTAMP '2024-01-31 23:59:00', TIMESTAMP '2024-02-01 00:01:00', 2.7)"
    )
    assert "(7, NULL, DATE '2024-01-01', NULL, TIMESTAMP '2024-01-01 00:00:00', 3.0)" in insert


def test_trino_seed_keeps_text_dates_untyped() -> None:
    insert = next(s for s in server_seed_statements("trino", today=_TODAY) if s.startswith("INSERT INTO orders"))
    assert ", 'x')" in insert
    assert ", '2024-03-01')" in insert


def test_calendar_seed_typed_temporals() -> None:
    _, insert = calendar_statements(typed_temporals=True)
    assert "(1, 'month', 1, DATE '2024-01-15', TIMESTAMP '2024-01-15 13:45:00')" in insert
