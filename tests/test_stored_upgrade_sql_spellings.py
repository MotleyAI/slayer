"""Stored 1.x text in every spelling 1.9.1 accepted is SQL-string upgraded with no warning (spec: models/stored-upgrade)."""

from __future__ import annotations

import logging
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.database import engine_factory
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend
from slayer.storage.sql_string_migration import rewrite_query_strings
from tests._stored_upgrade_fixtures import BACKENDS, DS, memory_v2, metric_total, query_backed_v10, raw_store, run

_GRAMMAR = "not in the grammar its version claims"
_GATE_LOGGER = "slayer.storage.sql_string_migration"

# (id, name, status, region, amount, created_at)
_ROWS = [
    (1, "Ann", "ok", "EU", 10.0, "2024-01-05 00:00:00"),
    (2, "Alf", "paid", "US", 20.0, "2024-03-01 00:00:00"),
    (3, "Bob", "ok", "US", 30.0, "2023-06-01 00:00:00"),
    (4, "Cid", "new", None, 40.0, "2024-07-01 00:00:00"),
    (5, "Abe", "ok", "US", 50.0, "2024-02-01 00:00:00"),
    (6, "Dan", "paid", "EU", 60.0, "2023-12-31 00:00:00"),
]
_TICKETS = {"version": 10, "name": "tickets", "sql_table": "tickets", "data_source": DS, "columns": [
    {"name": "id", "type": "INT", "primary_key": True},
    {"name": "name", "type": "TEXT"},
    {"name": "status", "type": "TEXT"},
    {"name": "region", "type": "TEXT"},
    {"name": "amount", "type": "DOUBLE"},
    {"name": "created_at", "type": "TIMESTAMP"},
]}
#: sum(amount) where status = 'ok'.
OK_TOTAL = 90.0

#: (stored filter, text after the string upgrade, loaded qualified text, 1.x sum(amount)).
FILTER_SPELLINGS = [
    pytest.param('status <> "ok"', "status <> 'ok'", "tickets.status <> 'ok'", 120.0, id="not-equal"),
    pytest.param('status = "ok" AND NOT region = "EU"', "status = 'ok' AND NOT region = 'EU'",
                 "tickets.status = 'ok' AND NOT tickets.region = 'EU'", 80.0, id="and-not"),
    pytest.param('region IS NULL OR status = "ok"', "region IS NULL OR status = 'ok'",
                 "tickets.region IS NULL OR tickets.status = 'ok'", 130.0, id="is-null-or"),
    pytest.param('status IN ("ok", "paid")', "status IN ('ok', 'paid')", "tickets.status IN ('ok', 'paid')", 170.0,
                 id="in"),
    pytest.param("name LIKE 'A%' AND status = \"ok\"", "name LIKE 'A%' AND status = 'ok'",
                 "tickets.name LIKE 'A%' AND tickets.status = 'ok'", 60.0, id="like"),
    pytest.param('status || region = "okEU"', "status || region = 'okEU'",
                 "tickets.status || tickets.region = 'okEU'", 10.0, id="concat"),
]

#: (stored measure, text after the string upgrade, loaded qualified text, 1.x value).
MEASURE_SPELLINGS = [
    pytest.param('sum(CASE WHEN status = "ok" THEN amount ELSE 0 END)',
                 "sum(CASE WHEN status = 'ok' THEN amount ELSE 0 END)",
                 "sum(CASE WHEN tickets.status = 'ok' THEN tickets.amount ELSE 0 END)", 90.0, id="case"),
    pytest.param('count(distinct iif(status == "ok", id, None))', "count(distinct iif(status == 'ok', id, None))",
                 "count(distinct iif(tickets.status = 'ok', tickets.id, NULL))", 3.0, id="count-distinct"),
    pytest.param('sum(iif(extract(year from created_at) == 2024 and status == "ok", amount, 0))',
                 "sum(iif(extract(year from created_at) == 2024 and status == 'ok', amount, 0))",
                 "sum(iif(extract(year from tickets.created_at) = 2024 and tickets.status = 'ok', tickets.amount, 0))",
                 60.0, id="extract"),
]

REFUSED = [
    pytest.param('name LIKE "A%"', id="like-double-quoted"),
    pytest.param('status = "ok" {? AND region = {r} ?}', id="optional-block"),
]


def _v6(**fields: Any) -> dict:
    return {"version": 6, "source_model": "tickets", **fields}


def _rev(filters: list[str]) -> dict:
    return {"source_model": "tickets", "filters": filters, "measures": [{"formula": "sum(amount)", "name": "rev"}]}


def _gate_messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.name == _GATE_LOGGER and _GRAMMAR in r.getMessage()]


@pytest.fixture
def gate_log(caplog: pytest.LogCaptureFixture) -> Generator[pytest.LogCaptureFixture]:
    with caplog.at_level(logging.WARNING, logger=_GATE_LOGGER):
        yield caplog


class TestSingleEqualsMigration:
    def test_double_quoted_literal_is_single_quoted(self, gate_log: pytest.LogCaptureFixture) -> None:
        assert rewrite_query_strings(_v6(filters=['status = "ok"']))["filters"] == ["status = 'ok'"]
        assert _gate_messages(gate_log) == []

    def test_single_quoted_literal_is_byte_identical(self, gate_log: pytest.LogCaptureFixture) -> None:
        assert rewrite_query_strings(_v6(filters=["status = 'ok'"]))["filters"] == ["status = 'ok'"]
        assert _gate_messages(gate_log) == []

    def test_memory_query(self, gate_log: pytest.LogCaptureFixture) -> None:
        mem = Memory.model_validate(memory_v2(memory_id="m_eq", version=4, query=_v6(**_rev(['status = "ok"']))))
        assert mem.query is not None
        assert mem.query.filter == ["status = 'ok'"]
        assert _gate_messages(gate_log) == []

    def test_saved_query(self, gate_log: pytest.LogCaptureFixture) -> None:
        doc = query_backed_v10(name="eq_q", version=15, stages=[_rev(['status = "ok"'])])
        [stage] = SlayerDataset.model_validate(doc).source_queries or []
        assert isinstance(stage, SlayerQuery)
        assert stage.filter == ["status = 'ok'"]
        assert _gate_messages(gate_log) == []


class TestFilterSpellingsMigration:
    @pytest.mark.parametrize(("stored", "upgraded", "loaded", "total"), FILTER_SPELLINGS)
    def test_upgraded_with_no_warning(self, gate_log: pytest.LogCaptureFixture, stored: str, upgraded: str,
                                      loaded: str, total: float) -> None:
        assert rewrite_query_strings(_v6(filters=[stored]))["filters"] == [upgraded]
        assert _gate_messages(gate_log) == []


class TestExpressionSpellingsMigration:
    @pytest.mark.parametrize(("stored", "upgraded", "loaded", "value"), MEASURE_SPELLINGS)
    def test_upgraded_with_no_warning(self, gate_log: pytest.LogCaptureFixture, stored: str, upgraded: str,
                                      loaded: str, value: float) -> None:
        out = rewrite_query_strings(_v6(measures=[{"formula": stored, "name": "v"}]))
        assert out["measures"] == [{"formula": upgraded, "name": "v"}]
        assert _gate_messages(gate_log) == []


#: Each non-filter surface of a v6 query holding ``text``, keyed by the field path the warning names.
def _expression_surfaces(text: str) -> dict[str, dict]:
    return {
        "measures[0]": _v6(measures=[text]),
        "measures['m'].formula": _v6(measures=[{"formula": text, "name": "m"}]),
        "dimensions[0]": _v6(dimensions=[text]),
        "dimensions['d'].expression": _v6(dimensions=[{"expression": text, "name": "d"}]),
        "time_dimensions[0].dimension": _v6(time_dimensions=[{"dimension": text, "granularity": "month"}]),
        "time_dimensions[0].column": _v6(time_dimensions=[{"dimension": "created_at", "column": text,
                                                           "granularity": "month"}]),
        "order[0].column": _v6(order=[{"column": text, "direction": "desc"}]),
        "order[0].raw_formula": _v6(order=[{"column": {"name": "_funcstyle_pending"}, "direction": "asc",
                                            "raw_formula": text}]),
        "main_time_dimension": _v6(main_time_dimension=text),
    }


class TestSurfaceKind:
    @pytest.mark.parametrize("field", list(_expression_surfaces("x")))
    def test_filter_only_spelling_is_refused_on_an_expression_surface(
        self, gate_log: pytest.LogCaptureFixture, field: str,
    ) -> None:
        doc = _expression_surfaces('status = "ok"')[field]
        assert rewrite_query_strings({**doc, "name": "kind_q"}) == {**doc, "name": "kind_q"}
        assert [m for m in _gate_messages(gate_log) if "kind_q" in m and field in m], _gate_messages(gate_log)

    def test_measure_comparison_stays_but_the_same_filter_comparison_is_upgraded(
        self, gate_log: pytest.LogCaptureFixture,
    ) -> None:
        out = rewrite_query_strings(_v6(name="kind_q", measures=[{"formula": 'sum(iif(a = "x", 1, 0))', "name": "m"}],
                                        filters=['iif(a = "x", 1, 0) = 1']))
        assert out["measures"] == [{"formula": 'sum(iif(a = "x", 1, 0))', "name": "m"}]
        assert out["filters"] == ["iif(a = 'x', 1, 0) = 1"]
        messages = _gate_messages(gate_log)
        assert [m for m in messages if "measures['m']" in m], messages
        assert not [m for m in messages if "filters" in m], messages


class TestRefusedSpellingsMigration:
    @pytest.mark.parametrize("stored", REFUSED)
    def test_saved_query(self, gate_log: pytest.LogCaptureFixture, stored: str) -> None:
        query = SlayerQuery.model_validate(_v6(name="refused_q", filters=[stored]))
        assert query.filter == [stored]
        assert [m for m in _gate_messages(gate_log) if "refused_q" in m and "filters[0]" in m], gate_log.text

    @pytest.mark.parametrize("stored", REFUSED)
    def test_memory_query(self, gate_log: pytest.LogCaptureFixture, stored: str) -> None:
        mem = Memory.model_validate(memory_v2(memory_id="m_refused", version=4, query=_v6(**_rev([stored]))))
        assert mem.query is not None
        assert mem.query.filter == [stored]
        assert [m for m in _gate_messages(gate_log) if "m_refused" in m and "filters[0]" in m], gate_log.text


# --------------------------------------------------------------------------- #
# Loaded from a store and executed.
# --------------------------------------------------------------------------- #
@pytest.fixture
def tickets_ds(tmp_path: Path) -> Generator[DatasourceConfig]:
    duckdb = pytest.importorskip("duckdb")
    db = str(tmp_path / "tickets.duckdb")
    con = duckdb.connect(db)
    try:
        con.execute("CREATE TABLE tickets (id INTEGER, name TEXT, status TEXT, region TEXT, amount DOUBLE, "
                    "created_at TIMESTAMP)")
        con.executemany("INSERT INTO tickets VALUES (?,?,?,?,?,?)", _ROWS)
    finally:
        con.close()
    ds = DatasourceConfig(name=DS, type="duckdb", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


async def _store(*, tmp_path: Path, ds: DatasourceConfig, backend: str, saved: dict[str, dict] | None = None,
                 memories: dict[str, dict] | None = None) -> StorageBackend:
    models = [_TICKETS, *(query_backed_v10(name=n, version=15, stages=[s]) for n, s in (saved or {}).items())]
    mems = [memory_v2(memory_id=m, version=4, query=_v6(**q)) for m, q in (memories or {}).items()]
    return await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds, models=models, memories=mems)


async def _saved_stage(storage: StorageBackend, name: str) -> SlayerQuery:
    model = await storage.get_dataset(name, data_source=DS)
    assert model is not None
    [stage] = model.source_queries or []
    assert isinstance(stage, SlayerQuery)
    return stage


async def _saved_total(storage: StorageBackend, name: str, *, metric: str = "rev") -> float:
    resp = await run(storage, {"population": name, "metrics": [f"sum({name}.{metric})"]})
    return metric_total(resp, metric=f"{metric}_sum")


class TestSingleEqualsLoaded:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_memory_and_saved_queries(self, tmp_path: Path, tickets_ds: DatasourceConfig, backend: str,
                                            gate_log: pytest.LogCaptureFixture) -> None:
        storage = await _store(tmp_path=tmp_path, ds=tickets_ds, backend=backend,
                               saved={"dq_q": _rev(['status = "ok"']), "sq_q": _rev(["status = 'ok'"])},
                               memories={"m_dq": _rev(['status = "ok"'])})
        mem = await storage.get_memory("m_dq")
        assert mem.query is not None
        assert mem.query.filter == ["tickets.status = 'ok'"]
        assert metric_total(await run(storage, mem.query), metric="rev") == OK_TOTAL
        for name in ("dq_q", "sq_q"):
            assert (await _saved_stage(storage, name)).filter == ["tickets.status = 'ok'"]
            assert await _saved_total(storage, name) == OK_TOTAL
        assert _gate_messages(gate_log) == []


class TestFilterSpellingsLoaded:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize(("stored", "upgraded", "loaded", "total"), FILTER_SPELLINGS)
    async def test_saved_query(self, tmp_path: Path, tickets_ds: DatasourceConfig, backend: str,
                               gate_log: pytest.LogCaptureFixture, stored: str, upgraded: str, loaded: str,
                               total: float) -> None:
        storage = await _store(tmp_path=tmp_path, ds=tickets_ds, backend=backend, saved={"f_q": _rev([stored])})
        assert (await _saved_stage(storage, "f_q")).filter == [loaded]
        assert await _saved_total(storage, "f_q") == total
        assert _gate_messages(gate_log) == []

    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize(("stored", "upgraded", "loaded", "total"), FILTER_SPELLINGS)
    async def test_memory_query(self, tmp_path: Path, tickets_ds: DatasourceConfig, backend: str,
                                gate_log: pytest.LogCaptureFixture, stored: str, upgraded: str, loaded: str,
                                total: float) -> None:
        storage = await _store(tmp_path=tmp_path, ds=tickets_ds, backend=backend, memories={"m_f": _rev([stored])})
        mem = await storage.get_memory("m_f")
        assert mem.query is not None
        assert mem.query.filter == [loaded]
        assert metric_total(await run(storage, mem.query), metric="rev") == total
        assert _gate_messages(gate_log) == []


class TestExpressionSpellingsLoaded:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize(("stored", "upgraded", "loaded", "value"), MEASURE_SPELLINGS)
    async def test_memory_query(self, tmp_path: Path, tickets_ds: DatasourceConfig, backend: str,
                                gate_log: pytest.LogCaptureFixture, stored: str, upgraded: str, loaded: str,
                                value: float) -> None:
        storage = await _store(tmp_path=tmp_path, ds=tickets_ds, backend=backend,
                               memories={"m_x": {"source_model": "tickets",
                                                 "measures": [{"formula": stored, "name": "v"}]}})
        mem = await storage.get_memory("m_x")
        assert mem.query is not None
        assert [m.metric for m in mem.query.metrics or []] == [loaded]
        assert metric_total(await run(storage, mem.query), metric="v") == value
        assert _gate_messages(gate_log) == []

    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize(("stored", "upgraded", "loaded", "value"), MEASURE_SPELLINGS)
    async def test_saved_query(self, tmp_path: Path, tickets_ds: DatasourceConfig, backend: str,
                               gate_log: pytest.LogCaptureFixture, stored: str, upgraded: str, loaded: str,
                               value: float) -> None:
        stage = {"source_model": "tickets", "measures": [{"formula": stored, "name": "v"}]}
        storage = await _store(tmp_path=tmp_path, ds=tickets_ds, backend=backend, saved={"x_q": stage})
        assert [m.metric for m in (await _saved_stage(storage, "x_q")).metrics or []] == [loaded]
        assert await _saved_total(storage, "x_q", metric="v") == value
        assert _gate_messages(gate_log) == []


class TestRefusedSpellingsLoaded:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_double_quoted_like_pattern(self, tmp_path: Path, tickets_ds: DatasourceConfig, backend: str,
                                              gate_log: pytest.LogCaptureFixture) -> None:
        storage = await _store(tmp_path=tmp_path, ds=tickets_ds, backend=backend,
                               saved={"like_q": _rev(['name LIKE "A%"'])},
                               memories={"m_like": _rev(['name LIKE "A%"'])})
        assert (await _saved_stage(storage, "like_q")).filter == ['tickets.name LIKE tickets."A%"']
        mem = await storage.get_memory("m_like")
        assert mem.query is not None
        assert mem.query.filter == ['tickets.name LIKE tickets."A%"']
        messages = _gate_messages(gate_log)
        assert [m for m in messages if "like_q" in m and "filters[0]" in m], messages
        assert [m for m in messages if "m_like" in m and "filters[0]" in m], messages
        assert all(("like_q" in m or "m_like" in m) and "filters[0]" in m for m in messages), messages
