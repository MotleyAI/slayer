"""Stored date-only comparisons keep their whole-day meaning (spec: models/stored-upgrade)."""

from __future__ import annotations

import json
import logging
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.sqlite_conn import transaction
from tests._stored_upgrade_fixtures import BACKENDS, DS, memory_v2, raw_store, run

#: The versions this change introduces; a concurrent bump renumbers only this line.
UPGRADED_AT = {"SlayerModel": 24, "SlayerQuery": 15, "Memory": 11}
PRE = {entity: version - 1 for entity, version in UPGRADED_AT.items()}

# (id, created_at); order_date is its day, amount is id * 10.
ROWS = [
    (1, "2024-03-05 08:00:00"), (2, "2024-04-10 00:00:00"), (3, "2024-04-10 09:30:00"), (4, "2024-04-11 00:00:00"),
    (5, "2024-12-31 00:00:00"), (6, "2024-12-31 10:00:00"), (7, "2025-01-01 00:00:00"),
]
COLUMNS = [
    {"name": "id", "type": "INT", "primary_key": True},
    {"name": "created_at", "type": "TIMESTAMP"},
    {"name": "order_date", "type": "DATE"},
    {"name": "code", "type": "TEXT"},
    {"name": "raw"},
    {"name": "amount", "type": "DOUBLE"},
]
LATE_AMOUNT = {"name": "late_amount", "sql": "amount", "type": "DOUBLE", "filter": "created_at > '2024-04-10'"}
LATE_AMOUNT_UPGRADED = "created_at >= '2024-04-11'"
EARLY_COUNT = "count(*) FILTER (WHERE created_at <= '2024-04-10')"
EARLY_COUNT_UPGRADED = "count(*) FILTER (WHERE created_at < '2024-04-11')"

#: Stored filter → (upgraded text, the ids it matched before this change).
OPERATORS: dict[str, tuple[str, set[int]]] = {
    "created_at <= '2024-12-31'": ("created_at < '2025-01-01'", {1, 2, 3, 4, 5, 6}),
    "created_at > '2024-04-10'": ("created_at >= '2024-04-11'", {4, 5, 6, 7}),
    "created_at = '2024-04-10'": ("created_at in '2024-04-10'", {2, 3}),
    "created_at != '2024-04-10'": ("created_at not in '2024-04-10'", {1, 4, 5, 6, 7}),
    "'2024-04-10' >= created_at": ("'2024-04-11' > created_at", {1, 2, 3}),
    "'2024-04-10' < created_at": ("'2024-04-11' <= created_at", {4, 5, 6, 7}),
    "created_at BETWEEN '2024-03-05' AND '2024-04-10'": (
        "(created_at >= '2024-03-05' and created_at < '2024-04-11')", {1, 2, 3}),
    "created_at between '2024-03-05' and '2024-04-10'": (
        "(created_at >= '2024-03-05' and created_at < '2024-04-11')", {1, 2, 3}),
    "created_at NOT BETWEEN '2024-03-05' AND '2024-04-10'": (
        "(created_at < '2024-03-05' or created_at >= '2024-04-11')", {4, 5, 6, 7}),
    "code = 'zz' or created_at <= '2024-04-10'": ("code = 'zz' or created_at < '2024-04-11'", {1, 2, 3}),
    "amount > 0 and created_at BETWEEN '2024-03-05' AND '2024-04-10'": (
        "amount > 0 and (created_at >= '2024-03-05' and created_at < '2024-04-11')", {1, 2, 3}),
    "not (created_at NOT BETWEEN '2024-03-05' AND '2024-04-10')": (
        "not ((created_at < '2024-03-05' or created_at >= '2024-04-11'))", {1, 2, 3}),
    "code = 'zz' or created_at BETWEEN '2024-03-05' AND '2024-04-10'": (
        "code = 'zz' or (created_at >= '2024-03-05' and created_at < '2024-04-11')", {1, 2, 3}),
    "amount > 0 and created_at NOT BETWEEN '2024-03-05' AND '2024-04-10'": (
        "amount > 0 and (created_at < '2024-03-05' or created_at >= '2024-04-11')", {4, 5, 6, 7}),
    "code = 'zz' or created_at NOT BETWEEN '2024-03-05' AND '2024-04-10'": (
        "code = 'zz' or (created_at < '2024-03-05' or created_at >= '2024-04-11')", {4, 5, 6, 7}),
}
#: A BETWEEN with one instant bound → (the instant text it keeps, the ids it matched before this change).
MIXED_BOUNDS: dict[str, tuple[str, set[int]]] = {
    "created_at BETWEEN '2024-03-05 12:00:00' AND '2024-04-10'": ("'2024-03-05 12:00:00'", {2, 3}),
    "created_at BETWEEN '2024-03-05' AND '2024-04-10 12:00:00'": ("'2024-04-10 12:00:00'", {1, 2, 3}),
}
UNCHANGED = [
    "created_at >= '2024-01-01'", "created_at < '2025-01-01'", "code <= '2024-12-31'",
    "created_at <= '2024-12-31 00:00:00'", "created_at <= '{cutoff}'", "created_at <= '2024-12'",
    "created_at in '2024-04-10'",
]
LITERAL_TESTED = "'2024-04-10' BETWEEN created_at AND created_at"


def orders_doc(*, version: int = PRE["SlayerModel"], **extra: Any) -> dict:
    return {"version": version, "name": "orders", "sql_table": "orders", "data_source": DS,
            "columns": COLUMNS, **extra}


def saved_query_doc(name: str, *stages: dict, version: int = PRE["SlayerModel"],
                    stage_version: int = PRE["SlayerQuery"]) -> dict:
    return {"version": version, "name": name, "data_source": DS,
            "source_queries": [{"version": stage_version, **s} for s in stages]}


def query(*filters: str, version: int = PRE["SlayerQuery"], **extra: Any) -> dict:
    return {"version": version, "source_model": "orders", "dimensions": ["id"], "filters": list(filters), **extra}


def memory(memory_id: str, body: dict, *, version: int = PRE["Memory"]) -> dict:
    return memory_v2(memory_id=memory_id, version=version, query=body)


def dumped(obj: Any) -> str:
    return json.dumps(obj.model_dump(mode="json"), default=str)


@pytest.fixture
def shop_ds(tmp_path: Path) -> Generator[DatasourceConfig]:
    db = str(tmp_path / "shop.db")
    with transaction(db) as cur:
        cur.execute("CREATE TABLE orders (id INTEGER, created_at TIMESTAMP, order_date DATE, code TEXT, raw TEXT, "
                    "amount DOUBLE)")
        cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)",
                        [(i, ts, ts[:10], ts[:10], ts, i * 10.0) for i, ts in ROWS])
    ds = DatasourceConfig.model_validate({"name": DS, "type": "sqlite", "database": db})
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


async def store(tmp_path: Path, ds: DatasourceConfig, *, backend: str, models: tuple[dict, ...] = (),
                memories: tuple[dict, ...] = ()) -> StorageBackend:
    return await raw_store(backend=backend, base=str(tmp_path / f"store-{backend}"), datasource=ds,
                           models=[orders_doc(), *models], memories=memories)


@contextmanager
def logged(caplog: pytest.LogCaptureFixture) -> Generator[list[str]]:
    texts: list[str] = []
    with caplog.at_level(logging.WARNING):
        yield texts
    texts.extend(r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING)


def ids(resp: Any, *, model: str = "orders") -> set[int]:
    return {int(r[f"{model}.id"]) for r in resp.data}


def notices(resp: Any) -> list[str]:
    return [t for t in (json.dumps(w.model_dump(mode="json"), default=str) for w in resp.warnings or ())
            if "midnight" in t]


def test_versions_bumped() -> None:
    assert all(CURRENT_VERSIONS[entity] >= version for entity, version in UPGRADED_AT.items())


@pytest.mark.parametrize("backend", BACKENDS)
class TestRewrites:
    async def test_saved_query_with_an_inclusive_upper_bound(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                             backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend, models=(
            saved_query_doc("year_end", {"source_model": "orders", "dimensions": ["id"],
                                         "filters": ["created_at <= '2024-12-31'"]}),))
        model = await storage.get_model("year_end", data_source=DS)
        assert model is not None
        assert (model.source_queries or [])[0].filters == ["created_at < '2025-01-01'"]
        resp = await run(storage, {"source_model": "year_end", "dimensions": ["id"]})
        assert ids(resp, model="year_end") == {1, 2, 3, 4, 5, 6}
        assert notices(resp) == []

    async def test_each_operator_in_a_memory_query(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                   backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend,
                              memories=(memory("m_ops", query(*OPERATORS)),))
        mem = await storage.get_memory("m_ops")
        assert mem.query is not None
        assert mem.query.filters == [upgraded for upgraded, _ in OPERATORS.values()]
        for upgraded, before in OPERATORS.values():
            resp = await run(storage, mem.query.model_copy(update={"filters": [upgraded]}))
            assert ids(resp) == before, upgraded

    async def test_saved_measure_and_column_filter(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                   backend: str) -> None:
        doc = orders_doc(columns=[*COLUMNS, LATE_AMOUNT], measures=[{"name": "early", "formula": EARLY_COUNT}])
        storage = await raw_store(backend=backend, base=str(tmp_path / f"store-{backend}"), datasource=shop_ds,
                                  models=[doc])
        model = await storage.get_model("orders", data_source=DS)
        assert model is not None
        assert [m.formula for m in model.measures] == [EARLY_COUNT_UPGRADED]
        assert next(c for c in model.columns if c.name == "late_amount").filter == LATE_AMOUNT_UPGRADED
        assert next(c for c in model.columns if c.name == "late_amount").sql == "amount"
        resp = await run(storage, {"source_model": "orders", "measures": ["early", "sum(late_amount)"]})
        row = resp.data[0]
        assert int(row["orders.early"]) == 3
        assert float(next(v for k, v in row.items() if k != "orders.early")) == pytest.approx(220.0)

    async def test_every_walked_query_field(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str) -> None:
        td = "iif(created_at <= '2024-04-10', created_at, NULL)"
        body = query(
            "created_at <= '2024-04-10'",
            measures=[{"formula": EARLY_COUNT, "name": "n"}],
            dimensions=["id", {"expression": "iif(created_at > '2024-04-10', 'late', 'early')", "name": "phase"}],
            time_dimensions=[{"dimension": td, "granularity": "day"}], main_time_dimension=td,
            order=[{"column": "iif(created_at = '2024-04-10', 0, 1)", "direction": "asc"}],
        )
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(memory("m_fields", body),))
        mem = await storage.get_memory("m_fields")
        assert mem.query is not None
        text = dumped(mem.query)
        for stale in ("<= '2024-04-10'", "> '2024-04-10'", "= '2024-04-10'"):
            assert stale not in text, text
        assert mem.query.filters == ["created_at < '2024-04-11'"]
        assert [m.formula for m in mem.query.measures or []] == [EARLY_COUNT_UPGRADED]
        assert "iif(created_at >= '2024-04-11', 'late', 'early')" in text
        assert "iif(created_at < '2024-04-11', created_at, NULL)" in text
        assert "iif(created_at in '2024-04-10', 0, 1)" in text

    async def test_column_filter_in_an_extension(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                 backend: str) -> None:
        # Stored before extension columns were removed: the load-time conversion inlines it after this upgrade.
        extended_q = {**query(version=13), "extensions": [{"source_name": "orders", "columns": [LATE_AMOUNT]}],
                      "measures": [{"formula": "sum(late_amount)", "name": "s"}]}
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(memory("m_ext", extended_q, version=9),))
        mem_query = (await storage.get_memory("m_ext")).query
        assert mem_query is not None
        text = dumped(mem_query)
        assert LATE_AMOUNT_UPGRADED in text, text
        assert "created_at > '2024-04-10'" not in text, text
        resp = await run(storage, mem_query.model_copy(update={"dimensions": []}))
        assert float(resp.data[0][next(iter(resp.data[0]))]) == pytest.approx(220.0)

    async def test_between_with_one_instant_bound(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                  backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(memory("m_mixed", query(*MIXED_BOUNDS)),))
        mem = await storage.get_memory("m_mixed")
        assert mem.query is not None
        for upgraded, (instant, before) in zip(mem.query.filters or [], MIXED_BOUNDS.values(), strict=True):
            assert instant in upgraded, upgraded
            assert ids(await run(storage, mem.query.model_copy(update={"filters": [upgraded]}))) == before, upgraded

    async def test_an_interrupted_load_completes(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                 backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(
            memory("m_half", query("created_at < '2025-01-01'", "created_at <= '2024-04-10'")),))
        mem = await storage.get_memory("m_half")
        assert mem.query is not None
        assert mem.query.filters == ["created_at < '2025-01-01'", "created_at < '2024-04-11'"]
        assert ids(await run(storage, mem.query)) == {1, 2, 3}

    async def test_last_representable_day(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str) -> None:
        eod = "'9999-12-31 23:59:59.999999'"
        stored = {
            "created_at <= '9999-12-31'": f"created_at <= {eod}",
            "'9999-12-31' < created_at": f"{eod} < created_at",
            "created_at BETWEEN '2024-01-01' AND '9999-12-31'": f"(created_at >= '2024-01-01' and created_at <= {eod})",
            "created_at NOT BETWEEN '2024-01-01' AND '9999-12-31'": f"(created_at < '2024-01-01' or created_at > {eod})",
            "code <= '9999-12-31'": "code <= '9999-12-31'",
        }
        doc = orders_doc(name="orders_last", columns=[*COLUMNS, {**LATE_AMOUNT, "filter": "code > '9999-12-31'"}])
        storage = await store(tmp_path, shop_ds, backend=backend, models=(doc,),
                              memories=(memory("m_last", query(*stored)),))
        mem = await storage.get_memory("m_last")
        assert mem.query is not None
        assert mem.query.filters == list(stored.values())
        model = await storage.get_model("orders_last", data_source=DS)
        assert model is not None
        assert next(c for c in model.columns if c.name == "late_amount").filter == "code > '9999-12-31'"
        resp = await run(storage, query("created_at <= '9999-12-31'", version=CURRENT_VERSIONS["SlayerQuery"]))
        assert ids(resp) == {i for i, _ in ROWS}
        assert len(notices(resp)) == 1
        assert f"created_at <= {eod}" in notices(resp)[0]

    async def test_names_match_case_insensitively(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                  backend: str) -> None:
        body = {**query("Created_At <= '2024-04-10'", "ORDERS.created_at > '2024-04-10'"), "source_model": "ORDERS"}
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(memory("m_case", body),))
        mem = await storage.get_memory("m_case")
        assert mem.query is not None
        assert mem.query.filters == ["Created_At < '2024-04-11'", "ORDERS.created_at >= '2024-04-11'"]

    async def test_join_walked_backwards(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str) -> None:
        customers = orders_doc(name="customers", joins=[])
        orders = orders_doc(name="purchases", joins=[{"target_model": "customers", "join_pairs": [["code", "code"]]}])
        named = orders_doc(name="refunds", joins=[
            {"name": "refunded", "target_model": "customers", "join_pairs": [["code", "code"]]}])
        body = {**query("purchases.created_at <= '2024-04-10'", "refunded.created_at > '2024-04-10'"),
                "source_model": "customers"}
        storage = await store(tmp_path, shop_ds, backend=backend, models=(customers, orders, named),
                              memories=(memory("m_back", body),))
        mem = await storage.get_memory("m_back")
        assert mem.query is not None
        assert mem.query.filters == ["purchases.created_at < '2024-04-11'", "refunded.created_at >= '2024-04-11'"]

    async def test_date_column_keeps_its_rows(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend,
                              memories=(memory("m_date", query("order_date <= '2024-04-10'")),))
        mem = await storage.get_memory("m_date")
        assert mem.query is not None
        assert ids(await run(storage, mem.query)) == {1, 2, 3}


@pytest.mark.parametrize("backend", BACKENDS)
class TestLeftAlone:
    async def test_unchanged_comparisons(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(memory("m_same", query(*UNCHANGED)),))
        mem = await storage.get_memory("m_same")
        assert mem.query is not None
        assert mem.query.filters == UNCHANGED

    async def test_mode_a_text_and_date_range(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str) -> None:
        doc = orders_doc(name="orders_a", filters=["created_at <= '2024-12-31'"],
                         columns=[*COLUMNS, {"name": "flag", "type": "INT",
                                             "sql": "CASE WHEN created_at <= '2024-04-10' THEN 1 END"}])
        td = {"dimension": "created_at", "granularity": "day", "date_range": ["2024-03-05", "2024-04-10"]}
        storage = await store(tmp_path, shop_ds, backend=backend, models=(doc,),
                              memories=(memory("m_range", query(time_dimensions=[td])),))
        model = await storage.get_model("orders_a", data_source=DS)
        assert model is not None
        assert model.filters == ["created_at <= '2024-12-31'"]
        assert next(c for c in model.columns if c.name == "flag").sql == "CASE WHEN created_at <= '2024-04-10' THEN 1 END"
        mem = await storage.get_memory("m_range")
        assert mem.query is not None
        assert [list(t.date_range or []) for t in mem.query.time_dimensions or []] == [["2024-03-05", "2024-04-10"]]

    async def test_untyped_column_is_left_and_logged(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
                                                     caplog: pytest.LogCaptureFixture) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend,
                              memories=(memory("m_untyped", query("raw <= '2024-12-31'")),))
        with logged(caplog) as texts:
            mem = await storage.get_memory("m_untyped")
        assert mem.query is not None
        assert mem.query.filters == ["raw <= '2024-12-31'"]
        assert any("m_untyped" in t and "raw <= '2024-12-31'" in t for t in texts), texts

    async def test_literal_tested_between_is_left_and_logged(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                             backend: str, caplog: pytest.LogCaptureFixture) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend,
                              memories=(memory("m_lit", query(LITERAL_TESTED)),))
        with logged(caplog) as texts:
            mem = await storage.get_memory("m_lit")
        assert mem.query is not None
        assert mem.query.filters == [LITERAL_TESTED]
        assert any("m_lit" in t and LITERAL_TESTED in t for t in texts), texts

    async def test_missing_model_is_left_and_logged(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
                                                    caplog: pytest.LogCaptureFixture) -> None:
        body = {**query("created_at <= '2024-12-31'"), "source_model": "gone"}
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(memory("m_gone", body),))
        with logged(caplog) as texts:
            mem = await storage.get_memory("m_gone")
        assert mem.query is not None
        assert mem.query.filters == ["created_at <= '2024-12-31'"]
        assert any("m_gone" in t and "created_at <= '2024-12-31'" in t for t in texts), texts


@pytest.mark.parametrize("backend", BACKENDS)
class TestVersionGating:
    async def test_current_model_with_an_older_nested_query(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                            backend: str) -> None:
        doc = saved_query_doc("mixed", {"source_model": "orders", "dimensions": ["id"],
                                        "filters": ["created_at <= '2024-12-31'"]},
                              version=CURRENT_VERSIONS["SlayerModel"])
        storage = await store(tmp_path, shop_ds, backend=backend, models=(doc,))
        model = await storage.get_model("mixed", data_source=DS)
        assert model is not None
        assert (model.source_queries or [])[0].filters == ["created_at < '2025-01-01'"]

    async def test_current_memory_with_an_older_query(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                      backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(
            memory("m_cur", query("created_at <= '2024-12-31'"), version=CURRENT_VERSIONS["Memory"]),))
        mem = await storage.get_memory("m_cur")
        assert mem.query is not None
        assert mem.query.filters == ["created_at < '2025-01-01'"]

    async def test_current_memory_with_an_older_query_holding_an_extension(
        self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
    ) -> None:
        body = query(extensions=[{"source_name": "orders", "measures": [{"name": "early", "formula": EARLY_COUNT}]}])
        storage = await store(tmp_path, shop_ds, backend=backend,
                              memories=(memory("m_ext_old", body, version=CURRENT_VERSIONS["Memory"]),))
        mem = await storage.get_memory("m_ext_old")
        assert mem.query is not None
        assert [m.formula for e in mem.query.extensions or [] for m in e.measures or []] == [EARLY_COUNT_UPGRADED]

    async def test_datasource_models_read_only_for_a_date_literal(
        self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(
            memory("m_plain", query("code = 'zz'")), memory("m_dated", query("created_at <= '2024-12-31'"))))
        reads: list[str | None] = []
        stored_models = storage._stored_models

        async def counted(data_source: str | None) -> Any:
            reads.append(data_source)
            return await stored_models(data_source)

        monkeypatch.setattr(storage, "_stored_models", counted)
        await storage.get_memory("m_plain")
        assert reads == []
        await storage.get_memory("m_dated")
        assert reads == [DS]

    async def test_upgrade_is_idempotent(self, tmp_path: Path, shop_ds: DatasourceConfig, backend: str) -> None:
        storage = await store(tmp_path, shop_ds, backend=backend, memories=(
            memory("m_twice", query(*OPERATORS)),
            memory("m_fresh", query("created_at <= '2024-12-31'", version=CURRENT_VERSIONS["SlayerQuery"]),
                   version=CURRENT_VERSIONS["Memory"])))
        first = await storage.get_memory("m_twice")
        again = await storage.get_memory("m_twice")
        assert first.query is not None
        assert again.query is not None
        assert again.query.filters == first.query.filters == [upgraded for upgraded, _ in OPERATORS.values()]
        fresh = await storage.get_memory("m_fresh")
        assert fresh.query is not None
        assert fresh.query.filters == ["created_at <= '2024-12-31'"]

    async def test_a_model_saved_after_the_change_is_untouched(self, tmp_path: Path, shop_ds: DatasourceConfig,
                                                               backend: str) -> None:
        doc = orders_doc(version=CURRENT_VERSIONS["SlayerModel"], name="orders_new",
                         measures=[{"name": "early", "formula": EARLY_COUNT}])
        storage = await store(tmp_path, shop_ds, backend=backend, models=(doc,))
        model = await storage.get_model("orders_new", data_source=DS)
        assert model is not None
        assert [m.formula for m in model.measures] == [EARLY_COUNT]
