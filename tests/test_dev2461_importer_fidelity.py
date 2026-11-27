"""Ossie importer fidelity: CASE and scalars around aggregates, ARRAY_AGG(DISTINCT x), FIRST_VALUE / LAST_VALUE,
quoted names and SQLite typing of untyped fields.

Spec: models/semantic-model-documents, queries/identifier-case.

orders (id, status, amount, qty): 1 paid 10.25 1 | 2 paid 20.5 2 | 3 open 30 1 | 4 open NULL 3 | 5 void NULL 2 |
   6 void NULL 2 — the ``void`` group has only NULL amounts.
tickets (id, grp, status): 1 a paid | 2 a paid | 3 b NULL | 4 a open.
events (id, ts): 1 2024-01-02 | 2 NULL | 3 2024-01-01.
"""

from __future__ import annotations

import contextlib
import importlib
import importlib.util
import shutil
import sqlite3
import sys
from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import duckdb
import pytest
import yaml

from slayer.core.enums import DataType
from slayer.core.errors import AmbiguousReferenceError, ListTypeError, UnknownReferenceError
from slayer.core.models import DatasetField, DatasetMetric, DatasourceConfig, SlayerDataset
from slayer.database.client import SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.engine.unknown_types import UnknownTypes
from slayer.live_schema.ingestion import introspect_table_to_dataset
from slayer.osi.exporter import export_osi
from slayer.osi.importer import OsiImportResult, import_osi
from slayer.sql.dialects.sqlite import PROBE_SCAN_CAP
from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2215_fixtures import BACKENDS, dev2215_datasets, dev2215_engine, m, num, q, single, sorted_rows
from tests._dev2217_fixtures import fixture_engine, rows, strict, strict_error
from tests._dev2243_fixtures import CORPUS, DS, STORAGES, dataset, document_text, field, make_storage, metric
from tests._engine_helpers import owned_query_engine

TYPINGS = ("typed", "untyped")

ORDERS = TableSpec(
    name="orders", columns=[("id", "INT"), ("status", "TEXT"), ("amount", "DOUBLE"), ("qty", "INT")],
    rows=[(1, "paid", 10.25, 1), (2, "paid", 20.5, 2), (3, "open", 30.0, 1), (4, "open", None, 3),
          (5, "void", None, 2), (6, "void", None, 2)],
)
TICKETS = TableSpec(
    name="tickets", columns=[("id", "INT"), ("grp", "TEXT"), ("status", "TEXT")],
    rows=[(1, "a", "paid"), (2, "a", "paid"), (3, "b", None), (4, "a", "open")],
)
EVENTS = TableSpec(
    name="events", columns=[("id", "INT"), ("ts", "TIMESTAMP")],
    rows=[(1, "2024-01-02 00:00:00"), (2, None), (3, "2024-01-01 00:00:00")],
)
TABLES = [ORDERS, TICKETS, EVENTS]

#: (metric name, Ossie expression, the same value as portable SQL over one group's rows).
SCALARS: list[tuple[str, str, str]] = [
    ("iff", "IFF(SUM(amount) > 0, SUM(amount), 0)", "CASE WHEN SUM(amount) > 0 THEN SUM(amount) ELSE 0 END"),
    ("if_", "IF(SUM(amount) > 20, 1, 0)", "CASE WHEN SUM(amount) > 20 THEN 1 ELSE 0 END"),
    ("zeroifnull", "ZEROIFNULL(SUM(amount))", "COALESCE(SUM(amount), 0)"),
    ("nvl", "NVL(SUM(amount), -1)", "COALESCE(SUM(amount), -1)"),
    ("nvl2", "NVL2(MAX(amount), 1, 0)", "CASE WHEN MAX(amount) IS NOT NULL THEN 1 ELSE 0 END"),
    ("nullifzero", "NULLIFZERO(COUNT(amount))", "NULLIF(COUNT(amount), 0)"),
    ("coalesce", "COALESCE(SUM(amount), MAX(qty))", "COALESCE(SUM(amount), MAX(qty))"),
    ("nullif", "NULLIF(COUNT(*), 2)", "NULLIF(COUNT(*), 2)"),
    ("round", "ROUND(AVG(amount), 1)", "ROUND(AVG(amount), 1)"),
    ("abs", "ABS(SUM(amount) - 50)", "ABS(SUM(amount) - 50)"),
    ("case_no_else", "CASE WHEN COUNT(*) > 1 THEN SUM(amount) END", "CASE WHEN COUNT(*) > 1 THEN SUM(amount) END"),
    ("case_label", "CASE WHEN SUM(amount) > 20 THEN 'hi' ELSE 'lo' END",
     "CASE WHEN SUM(amount) > 20 THEN 'hi' ELSE 'lo' END"),
    ("case_conditions", "CASE WHEN MIN(qty) = MAX(qty) THEN 'same' WHEN SUM(amount) IS NULL THEN 'none' "
     "ELSE 'mixed' END",
     "CASE WHEN MIN(qty) = MAX(qty) THEN 'same' WHEN SUM(amount) IS NULL THEN 'none' ELSE 'mixed' END"),
    ("filtered", "ZEROIFNULL(SUM(amount) FILTER (WHERE status = 'paid'))",
     "COALESCE(SUM(CASE WHEN status = 'paid' THEN amount END), 0)"),
    ("iff_filtered", "IFF(COUNT(*) FILTER (WHERE status = 'paid') > 0, SUM(amount) FILTER (WHERE status = 'paid'), -1)",
     "CASE WHEN COUNT(CASE WHEN status = 'paid' THEN 1 END) > 0 THEN SUM(CASE WHEN status = 'paid' THEN amount END) "
     "ELSE -1 END"),
]

#: (model-scoped metric name, Ossie expression, portable SQL over ``orders``).
MODEL_SCOPED: list[tuple[str, str, str]] = [
    ("label", "CASE WHEN SUM(orders.amount) > 15 THEN 'hi' ELSE 'lo' END",
     "CASE WHEN SUM(amount) > 15 THEN 'hi' ELSE 'lo' END"),
    ("zeroed", "ZEROIFNULL(SUM(orders.amount))", "COALESCE(SUM(amount), 0)"),
]

#: (row-level field name, Ossie expression, portable SQL).
ROW_FIELDS: list[tuple[str, str, str]] = [
    ("tier", "CASE WHEN amount > 10 THEN 'big' WHEN amount IS NULL THEN 'none' ELSE 'small' END",
     "CASE WHEN amount > 10 THEN 'big' WHEN amount IS NULL THEN 'none' ELSE 'small' END"),
    ("size", "IFF(qty > 1, 'multi', 'single')", "CASE WHEN qty > 1 THEN 'multi' ELSE 'single' END"),
]

FULL = "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
#: (window field name, Ossie expression, the same window with Ossie's NULL placement spelled out).
WINDOWS: list[tuple[str, str, str]] = [
    ("fv_asc", "FIRST_VALUE(id) OVER (ORDER BY ts)", "FIRST_VALUE(id) OVER (ORDER BY ts NULLS LAST)"),
    ("lv_asc", f"LAST_VALUE(id) OVER (ORDER BY ts {FULL})", f"LAST_VALUE(id) OVER (ORDER BY ts NULLS LAST {FULL})"),
    ("fv_desc", "FIRST_VALUE(id) OVER (ORDER BY ts DESC)", "FIRST_VALUE(id) OVER (ORDER BY ts DESC NULLS FIRST)"),
    ("lv_desc", f"LAST_VALUE(id) OVER (ORDER BY ts DESC {FULL})",
     f"LAST_VALUE(id) OVER (ORDER BY ts DESC NULLS FIRST {FULL})"),
    ("fv_nulls_first", "FIRST_VALUE(id) OVER (ORDER BY ts NULLS FIRST)",
     "FIRST_VALUE(id) OVER (ORDER BY ts NULLS FIRST)"),
    ("lv_nulls_first", f"LAST_VALUE(id) OVER (ORDER BY ts NULLS FIRST {FULL})",
     f"LAST_VALUE(id) OVER (ORDER BY ts NULLS FIRST {FULL})"),
]


def fidelity_document(typing: str) -> str:
    """The fixture document; ``untyped`` declares no ``datatype`` anywhere."""
    def f(name: str, sql: str | None = None, datatype: str = "String") -> dict[str, Any]:
        return field(name, sql, datatype=datatype if typing == "typed" else None)

    return document_text(
        name="fidelity",
        datasets=[
            dataset("orders", primary_key=["id"], fields=[
                f("id", datatype="Integer"), f("status"), f("amount", datatype="Float"), f("qty", datatype="Integer"),
                *(f(name, sql) for name, sql, _ in ROW_FIELDS),
            ], metrics=[metric(name, sql) for name, sql, _ in SCALARS]),
            dataset("tickets", primary_key=["id"], fields=[f("id", datatype="Integer"), f("grp"), f("status")],
                    metrics=[metric("statuses", "ARRAY_AGG(DISTINCT status)")]),
            dataset("events", fields=[  # no key: first / last refuse a primary key
                f("id", datatype="Integer"), f("ts", datatype="DateTime"),
                *(f(name, sql, datatype="Integer") for name, sql, _ in WINDOWS),
            ]),
        ],
        metrics=[metric(name, sql) for name, sql, _ in MODEL_SCOPED],
    )


# --------------------------------------------------------------------------- #
# Seed, oracle and import
# --------------------------------------------------------------------------- #
def seed_path(tmp: Path, backend: str) -> Path:
    return tmp / f"seed.{'duckdb' if backend == 'duckdb' else 'db'}"


def oracle(backend: str, db: Path, sql: str) -> list[tuple[Any, ...]]:
    """``sql``'s rows straight from the seeded database, before any engine opens it."""
    if backend == "sqlite":
        with contextlib.closing(sqlite3.connect(db)) as con:
            return [tuple(r) for r in con.execute(sql).fetchall()]
    with contextlib.closing(duckdb.connect(str(db))) as con:
        return [tuple(r) for r in con.execute(sql).fetchall()]


def plain(value: Any) -> Any:
    """A cell comparable across drivers: numbers as rounded floats."""
    if value is None or isinstance(value, (str, bool, list)):
        return value
    return round(float(value), 9)


@contextlib.asynccontextmanager
async def imported(tmp: Path, *, backend: str, text: str, kind: str = "yaml",
                   ) -> AsyncGenerator[tuple[SlayerQueryEngine, OsiImportResult]]:
    """``text`` imported through ``import_osi`` into an engine over the seeded ``backend``."""
    db = seed_path(tmp, backend)
    config = DatasourceConfig(name=DS, type=backend, database=str(db))
    path = tmp / "doc" / "fidelity.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    async with owned_query_engine(storage=make_storage(kind, tmp), datasources=[config]) as engine:
        result = await import_osi(storage=engine.storage, path=path, data_source=DS, engine=engine)
        assert result.refused == []
        yield engine, result


def not_evaluable(result: OsiImportResult) -> list[str]:
    return [f"{e.entity}: {e.message}" for e in result.entries if e.kind == "not_evaluable"]


def keyed(resp: SlayerResponse, key: str, *values: str) -> dict[Any, tuple[Any, ...]]:
    return {r[key]: tuple(plain(r[v]) for v in values) for r in resp.data}


def keyed_oracle(found: list[tuple[Any, ...]]) -> dict[Any, tuple[Any, ...]]:
    return {r[0]: tuple(plain(v) for v in r[1:]) for r in found}


# --------------------------------------------------------------------------- #
# CASE and scalar functions around aggregates evaluate as written
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("typing", TYPINGS)
@pytest.mark.parametrize("backend", BACKENDS)
async def test_scalars_around_aggregates_match_sql_per_group(tmp_path: Path, backend: str, typing: str) -> None:
    """Spec: Scalars around aggregates match SQL per group."""
    seed_backend(backend, str(seed_path(tmp_path, backend)), TABLES)
    expected = keyed_oracle(oracle(backend, seed_path(tmp_path, backend),
                                   f"SELECT status, {', '.join(o for _, _, o in SCALARS)} FROM orders GROUP BY status"))
    text = fidelity_document(typing)
    async with imported(tmp_path, backend=backend, text=text) as (engine, result):
        assert not_evaluable(result) == []
        resp = await engine.execute({"population": "orders", "dimensions": [{"field": "orders.status", "alias": "g"}],
                                     "metrics": [{"metric": f"orders.{n}", "alias": n} for n, _, _ in SCALARS]})
        assert keyed(resp, "g", *(n for n, _, _ in SCALARS)) == expected
        assert await engine.storage.get_semantic_model_text("fidelity", DS) == text


@pytest.mark.parametrize("typing", TYPINGS)
@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_model_scoped_case_over_aggregates_evaluates(tmp_path: Path, backend: str, typing: str) -> None:
    """Spec: A model-scoped CASE over aggregates evaluates."""
    db = seed_path(tmp_path, backend)
    seed_backend(backend, str(db), TABLES)
    sql = ", ".join(o for _, _, o in MODEL_SCOPED)
    total = keyed_oracle([("all", *oracle(backend, db, f"SELECT {sql} FROM orders")[0])])
    grouped = keyed_oracle(oracle(backend, db, f"SELECT status, {sql} FROM orders GROUP BY status"))
    names = [n for n, _, _ in MODEL_SCOPED]
    metrics = [{"metric": n, "alias": n} for n in names]
    async with imported(tmp_path, backend=backend, text=fidelity_document(typing)) as (engine, result):
        assert not_evaluable(result) == []
        whole = await engine.execute({"metrics": metrics})
        assert {"all": tuple(plain(single(whole, n)) for n in names)} == total
        by_status = await engine.execute({"dimensions": [{"field": "orders.status", "alias": "g"}], "metrics": metrics})
        assert keyed(by_status, "g", *names) == grouped


@pytest.mark.parametrize("typing", TYPINGS)
@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(("name", "sql"), [(n, o) for n, _, o in ROW_FIELDS], ids=[n for n, _, _ in ROW_FIELDS])
async def test_a_case_field_evaluates_per_row(tmp_path: Path, backend: str, typing: str, name: str, sql: str) -> None:
    """Spec: A CASE field evaluates per row."""
    db = seed_path(tmp_path, backend)
    seed_backend(backend, str(db), TABLES)
    expected = keyed_oracle(oracle(backend, db, f"SELECT {sql}, COUNT(*) FROM orders GROUP BY 1"))
    async with imported(tmp_path, backend=backend, text=fidelity_document(typing)) as (engine, result):
        assert not_evaluable(result) == []
        resp = await engine.execute({"population": "orders", "dimensions": [{"field": f"orders.{name}", "alias": "t"}],
                                     "metrics": [m("count(orders.*)", "n")]})
        assert keyed(resp, "t", "n") == expected


# --------------------------------------------------------------------------- #
# ARRAY_AGG(DISTINCT x) keeps collect's NULL conventions
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("typing", TYPINGS)
async def test_array_agg_distinct_follows_collect_on_duckdb(tmp_path: Path, typing: str) -> None:
    """Spec: ARRAY_AGG DISTINCT follows collect's NULL conventions (DuckDB)."""
    seed_backend("duckdb", str(seed_path(tmp_path, "duckdb")), TABLES)
    async with imported(tmp_path, backend="duckdb", text=fidelity_document(typing)) as (engine, result):
        assert not_evaluable(result) == []
        whole = await engine.execute({"population": "tickets", "metrics": [m("tickets.statuses", "s")]})
        assert list(single(whole, "s")) == ["open", "paid"]
        grouped = await engine.execute({"population": "tickets", "dimensions": [{"field": "tickets.grp", "alias": "g"}],
                                        "metrics": [m("tickets.statuses", "s")]})
        assert {r["g"]: list(r["s"]) for r in grouped.data} == {"a": ["open", "paid"], "b": []}


async def test_array_agg_distinct_is_a_typed_list_error_on_sqlite(tmp_path: Path) -> None:
    """Spec: ARRAY_AGG DISTINCT follows collect's NULL conventions (SQLite has no lists)."""
    seed_backend("sqlite", str(seed_path(tmp_path, "sqlite")), TABLES)
    async with imported(tmp_path, backend="sqlite", text=fidelity_document("typed")) as (engine, _):
        with pytest.raises(ListTypeError):
            await engine.execute({"population": "tickets", "metrics": [m("tickets.statuses", "s")]})


# --------------------------------------------------------------------------- #
# Imported FIRST_VALUE / LAST_VALUE windows keep their window form
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("typing", TYPINGS)
@pytest.mark.parametrize("backend", BACKENDS)
async def test_imported_first_and_last_windows_keep_their_window_form(tmp_path: Path, backend: str,
                                                                      typing: str) -> None:
    """Spec: Imported first and last windows keep their window form."""
    db = seed_path(tmp_path, backend)
    seed_backend(backend, str(db), TABLES)
    names = [n for n, _, _ in WINDOWS]
    expected = keyed_oracle(oracle(backend, db, f"SELECT id, {', '.join(o for _, _, o in WINDOWS)} FROM events"))
    text = fidelity_document(typing)
    async with imported(tmp_path, backend=backend, text=text) as (engine, result):
        assert not_evaluable(result) == []
        resp = await engine.execute({
            "population": "events", "distinct_dimension_values": False,
            "dimensions": [{"field": "events.id", "alias": "id"}, *({"field": f"events.{n}", "alias": n} for n in names)],
        })
        got = {int(k): v for k, v in keyed(resp, "id", *names).items()}
        assert got == {int(k): v for k, v in expected.items()}
        agg = await engine.execute({"population": "events", "metrics": [m("first(events.id, events.ts)", "f"),
                                                                         m("last(events.id, events.ts)", "l")]})
        first, last = plain(single(agg, "f")), plain(single(agg, "l"))
        row = got[2]  # the NULL-keyed row
        assert row[names.index("lv_asc")] != last
        assert row[names.index("fv_nulls_first")] != first
        assert await engine.storage.get_semantic_model_text("fidelity", DS) == text


# --------------------------------------------------------------------------- #
# Quoted names: exact spelling, then Ossie's normalized form, then ambiguity
# --------------------------------------------------------------------------- #
@pytest.fixture(params=BACKENDS)
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    twins = SlayerDataset(name="twins", sql_table="ledger", data_source="test", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="AMOUNT", expression="rev_a", datatype=DataType.FLOAT),
        DatasetField(name="amount", expression="rev_b", datatype=DataType.FLOAT),
    ])
    spaced = SlayerDataset(name="spaced", sql_table="orders", data_source="test", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="order total", expression="amount", datatype=DataType.FLOAT),
    ])
    async with dev2215_engine(request.param, datasets=[*dev2215_datasets(), twins, spaced]) as eng:
        yield eng


class TestQuotedNames:
    @pytest.mark.parametrize("measure", ['sum("AMOUNT")', 'sum(orders."AMOUNT")', 'sum("ORDERS"."AMOUNT")'])
    async def test_a_quoted_name_in_ossie_normalized_form_matches(self, engine: SlayerQueryEngine,
                                                                  measure: str) -> None:
        """Spec: A quoted name in Ossie's normalized form matches."""
        plain_resp = await engine.execute(q(dimensions=["orders.status"], metrics=["sum(amount)"]))
        assert plain_resp.columns == ["orders.status", "orders.amount_sum"]
        quoted = await engine.execute(q(dimensions=['"ORDERS"."STATUS"'], metrics=[measure]))
        assert quoted.columns == plain_resp.columns
        assert sorted_rows(quoted) == sorted_rows(plain_resp)

    @pytest.mark.parametrize("dimension", ['"CUSTOMERS".region', 'customers."REGION"', '"CUSTOMERS"."REGION"'])
    async def test_normalized_form_per_segment(self, engine: SlayerQueryEngine, dimension: str) -> None:
        resp = await engine.execute(q(dimensions=[dimension], metrics=[m("count(orders.*)", "n")]))
        assert "customers.region" in resp.columns

    @pytest.mark.parametrize("ref", ['"monthly"."n"', '"MONTHLY"."N"'])
    async def test_stage_and_result_names_take_the_normalized_form(self, engine: SlayerQueryEngine, ref: str) -> None:
        stages: list[Any] = [
            {"name": "monthly", "population": "orders",
             "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
             "metrics": [m("count(orders.*)", "n")]},
            {"population": "monthly", "metrics": [m(f"sum({ref})", "t")]},
        ]
        resp = await engine.execute(stages)
        assert [num(v) for r in resp.data for k, v in r.items() if k == "t" or k.endswith(".t")] == [6.0]

    async def test_the_exact_spelling_beats_the_normalized_form(self, engine: SlayerQueryEngine) -> None:
        """Spec: The exact spelling beats the normalized form."""
        resp = await engine.execute(q(population="twins", metrics=[m('sum("AMOUNT")', "a"), m('sum("amount")', "b")]))
        assert (num(single(resp, "a")), num(single(resp, "b"))) == (3.0, 30.0)

    async def test_two_names_with_one_normalized_form_are_ambiguous(self, engine: SlayerQueryEngine) -> None:
        """Spec: Two names with one normalized form are ambiguous."""
        query = q(population="ledger", metrics=[m('sum(ledger."REVENUE")')])
        with pytest.raises(AmbiguousReferenceError) as err:
            await engine.execute(query)
        assert "Revenue" in str(err.value)
        assert "revenue" in str(err.value)

    async def test_a_name_that_must_be_quoted_has_no_other_form(self, engine: SlayerQueryEngine) -> None:
        """Spec: A name that must be quoted has no other form."""
        resp = await engine.execute(q(population="spaced", metrics=[m('sum("order total")')]))
        assert num(single(resp)) == 160.0
        query = q(population="spaced", metrics=[m('sum("ORDER TOTAL")')])
        with pytest.raises(UnknownReferenceError):
            await engine.execute(query)


async def test_strict_binding_uses_the_same_rule(tmp_path: Path) -> None:
    """Spec: Strict binding uses the same rule."""
    async with fixture_engine(tmp_path, "retail") as eng:
        response = await strict(eng, {"dimensions": ['"CUSTOMERS".region'], "metrics": ["Orders.total_revenue"]})
        assert rows(response) == [("EAST", 350.0), ("WEST", 300.0)]
        await strict_error(eng, {"dimensions": ['"Customers".region']}, "E_NAME_NOT_FOUND")


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_source_column_keeps_the_database_quoting(tmp_path: Path, backend: str) -> None:
    """Spec: A source column keeps the database's quoting."""
    db = seed_path(tmp_path, backend)
    seed_backend(backend, str(db), TABLES)
    expected = sorted(r[0] for r in oracle(backend, db, 'SELECT "ID" FROM orders'))
    text = document_text(name="fidelity", datasets=[
        dataset("orders", primary_key=["id"], fields=[field("id", datatype="Integer"),
                                                     field("big_id", '"ID"', datatype="Integer")]),
    ])
    async with imported(tmp_path, backend=backend, text=text) as (eng, _):
        resp = await eng.execute({"population": "orders", "distinct_dimension_values": False,
                                  "dimensions": [{"field": "orders.big_id", "alias": "b"}]})
        assert sorted(int(r["b"]) for r in resp.data) == expected
        assert '"ID"' in (resp.sql or "")
        assert await eng.storage.get_semantic_model_text("fidelity", DS) == text


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_quoted_reference_round_trips_with_its_quotes(tmp_path: Path, kind: str) -> None:
    """Spec: A quoted reference round-trips with its quotes."""
    seed_backend("duckdb", str(seed_path(tmp_path, "duckdb")), TABLES)
    text = document_text(name="fidelity", datasets=[
        dataset("orders", primary_key=["id"], fields=[field("id", datatype="Integer"),
                                                     field("amount", datatype="Float")],
                metrics=[metric("total", 'SUM("AMOUNT")')]),
    ])
    async with imported(tmp_path, backend="duckdb", text=text, kind=kind) as (eng, result):
        assert not_evaluable(result) == []
        for name in await eng.storage.list_datasets(data_source=DS):
            loaded = await eng.storage.get_dataset(name, data_source=DS)
            assert loaded is not None
            await eng.storage.save_dataset(loaded)
        assert await eng.storage.get_semantic_model_text("fidelity", DS) == text
        assert (await export_osi(engine=eng, datasource=DS)).text == text
        assert num(single(await eng.execute({"population": "orders", "metrics": [m("orders.total")]}))) == 60.75

        native = SlayerDataset(name="native_orders", sql_table="orders", data_source=DS, fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ], metrics=[DatasetMetric(name="total", expression='sum("AMOUNT")')])
        await eng.storage.save_dataset(native)
        read_back = await eng.storage.get_dataset("native_orders", data_source=DS)
        assert read_back is not None
        assert read_back.metrics[0].model_dump(mode="json")["expression"] == 'sum("AMOUNT")'
        resp = await eng.execute({"population": "native_orders", "metrics": [m("native_orders.total")]})
        assert num(single(resp)) == 60.75


# --------------------------------------------------------------------------- #
# Normalized forms follow Ossie's validator
# --------------------------------------------------------------------------- #
DECLARED = ["order_id", "été", "from", "alter", "select", "order total", " padded", "a-b", "a.b", 'say "hi"',
            "Revenue", "AMOUNT", "_a", "x1", "1x"]


@pytest.fixture(scope="module")
def upstream_validator(tmp_path_factory: pytest.TempPathFactory) -> ModuleType:
    """The vendored upstream ``validate.py``, loaded from a copy (it writes beside its loader)."""
    copy = tmp_path_factory.mktemp("ossie_validator")
    for part in ("validation", "core-spec", "python"):
        shutil.copytree(CORPUS / part, copy / part)
    spec = importlib.util.spec_from_file_location("ossie_upstream_validate", copy / "validation" / "validate.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def normalized_name() -> Callable[[str], str]:
    """The Ossie normalized form of a declared name (added by this change)."""
    return importlib.import_module("slayer.core.names").normalized_name


@pytest.mark.parametrize("name", DECLARED)
def test_normalized_forms_follow_ossie_validator(upstream_validator: ModuleType, name: str) -> None:
    """Spec: Normalized forms follow Ossie's validator."""
    assert normalized_name()(name) == upstream_validator.declared_name(name)


# --------------------------------------------------------------------------- #
# SQLite resolves an untyped field's type as ingestion does
# --------------------------------------------------------------------------- #
def sqlite_seed(db: Path, *statements: str) -> None:
    with contextlib.closing(sqlite3.connect(db)) as con:
        for statement in statements:
            con.execute(statement)
        con.commit()


@contextlib.asynccontextmanager
async def stored(tmp: Path, *, text: str) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over ``tmp/seed.db`` (SQLite) with ``text`` stored as a document."""
    config = DatasourceConfig(name=DS, type="sqlite", database=str(tmp / "seed.db"))
    async with owned_query_engine(storage=make_storage("yaml", tmp), datasources=[config]) as eng:
        await eng.storage.save_semantic_model_text(data_source=DS, name=yaml.safe_load(text)["name"], text=text)
        yield eng


async def resolved_type(eng: SlayerQueryEngine, *, dataset_name: str, field_name: str) -> DataType | None:
    loaded = await eng.storage.get_dataset(dataset_name, data_source=DS)
    assert loaded is not None
    typed = await UnknownTypes().resolve(loaded, storage=eng.storage, client_for=eng.client_for)
    return next(f.datatype for f in typed.fields if f.name == field_name)


async def test_an_untyped_sqlite_column_aggregates(tmp_path: Path) -> None:
    """Spec: An untyped SQLite column aggregates."""
    sqlite_seed(tmp_path / "seed.db", "CREATE TABLE orders (id INTEGER, amount REAL, qty INTEGER)",
                "INSERT INTO orders VALUES (1, 1.5, 2), (2, 2.25, 3), (3, NULL, 4)")
    text = document_text(name="untyped", datasets=[dataset("orders", primary_key=["id"], fields=[
        field("id", datatype=None), field("amount", datatype=None), field("qty", datatype=None),
        field("doubled", "amount * 2", datatype=None)])])
    async with stored(tmp_path, text=text) as eng:
        resp = await eng.execute({"population": "orders", "metrics": [
            m("sum(orders.amount)", "a"), m("sum(orders.qty)", "b"), m("sum(orders.doubled)", "c")]})
        assert (num(single(resp, "a")), num(single(resp, "b")), num(single(resp, "c"))) == (3.75, 9.0, 7.5)
        assert await eng.storage.get_semantic_model_text("untyped", DS) == text
        loaded = await eng.storage.get_dataset("orders", data_source=DS)
        assert loaded is not None
        assert [f.datatype for f in loaded.fields] == [None] * 4


#: More integer rows than the bounded sample holds.
SATURATED = (f"INSERT INTO probe (v) WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c "
             f"WHERE x <= {PROBE_SCAN_CAP}) SELECT x FROM c")
#: (case id, column declaration, insert statement) — one single-column table each.
INGESTION_CASES: list[tuple[str, str, str]] = [
    ("integer_values", "INTEGER", "INSERT INTO probe (v) VALUES (1), (2), (3)"),
    ("integer_holding_real", "INTEGER", "INSERT INTO probe (v) VALUES (1), (2.5)"),
    ("real_empty", "REAL", ""),
    ("integer_only_nulls", "INTEGER", "INSERT INTO probe (v) VALUES (NULL), (NULL)"),
    ("text_numeric", "TEXT", "INSERT INTO probe (v) VALUES ('1.5'), ('2')"),
    ("text_words", "TEXT", "INSERT INTO probe (v) VALUES ('a'), ('b')"),
    ("integer_holding_blob", "INTEGER", "INSERT INTO probe (v) VALUES (1), (x'00')"),
    ("integer_saturated", "INTEGER", SATURATED),
]


async def assert_parity(tmp: Path, *, declared: str, insert: str) -> None:
    """The untyped document field ``probe.v`` resolves to the type ingestion gives column ``v``."""
    sqlite_seed(tmp / "seed.db", f"CREATE TABLE probe (id INTEGER, v {declared})", *([insert] if insert else []))
    text = document_text(name="untyped", datasets=[dataset("probe", fields=[field("v", datatype=None)])])
    async with stored(tmp, text=text) as eng:
        config = await eng.storage.get_datasource(DS)
        assert config is not None
        ingested = await introspect_table_to_dataset(client=eng.client_for(config), table_name="probe", ref=None,
                                                     data_source=DS)
        expected = next(f.datatype for f in ingested.fields if f.name == "v")
        assert await resolved_type(eng, dataset_name="probe", field_name="v") == expected


@pytest.mark.parametrize(("declared", "insert"), [(d, i) for _, d, i in INGESTION_CASES],
                         ids=[c for c, _, _ in INGESTION_CASES])
async def test_sqlite_typing_agrees_with_ingestion(tmp_path: Path, declared: str, insert: str) -> None:
    """Spec: SQLite typing agrees with ingestion."""
    await assert_parity(tmp_path, declared=declared, insert=insert)


async def test_sqlite_typing_agrees_with_ingestion_when_the_probe_fails(tmp_path: Path,
                                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed storage-class census keeps the declared type, in ingestion and resolution alike."""
    execute = SlayerSQLClient.execute

    async def failing_census(self: SlayerSQLClient, sql: str, timeout_seconds: int = 120) -> Any:
        if "typeof" in sql.casefold():
            raise RuntimeError("census unavailable")
        return await execute(self, sql, timeout_seconds)

    monkeypatch.setattr(SlayerSQLClient, "execute", failing_census)
    await assert_parity(tmp_path, declared="INTEGER", insert="INSERT INTO probe (v) VALUES (1), (2.5)")


async def test_an_untyped_sqlite_expression_over_no_rows_is_string(tmp_path: Path) -> None:
    """Spec: An untyped SQLite expression over no rows is String."""
    sqlite_seed(tmp_path / "seed.db", "CREATE TABLE orders (id INTEGER, amount REAL)")
    text = document_text(name="untyped", datasets=[dataset("orders", fields=[
        field("amount", datatype=None), field("doubled", "amount * 2", datatype=None)])])
    async with stored(tmp_path, text=text) as eng:
        assert await resolved_type(eng, dataset_name="orders", field_name="doubled") is DataType.STRING
