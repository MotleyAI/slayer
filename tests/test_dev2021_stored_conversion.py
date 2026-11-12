"""Stored extension columns are inlined on load; an unconvertible document fails to load, untouched.

Spec: models/stored-upgrade › Stored extension columns are inlined.
"""

from __future__ import annotations

import os
import warnings
from collections.abc import Iterator
from typing import Any

import pytest
import yaml

from slayer.core.errors import StoredDocumentLoadError
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import ComputedDimension, ModelMeasure, SlayerQuery
from slayer.database import engine_factory
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import owned_query_engine
from tests._stored_upgrade_fixtures import (
    BACKENDS,
    DS,
    customers_v10,
    memory_v2,
    orders_v10,
    query_backed_v10,
    raw_doc,
    raw_store,
    run,
    seed_shop,
    stored_bytes,
)

#: Stored versions written before this change.
_MODEL_V, _QUERY_V, _MEMORY_V = 21, 13, 9
_REV = {"formula": "sum(amount)", "name": "rev"}


def _stage(**fields: Any) -> dict:
    return {"version": _QUERY_V, "source_model": "orders", **fields}


def _qb(name: str, *stages: dict) -> dict:
    return query_backed_v10(name=name, stages=list(stages), version=_MODEL_V)


def _ext(source_name: str, *columns: dict, **fields: Any) -> dict:
    return {"source_name": source_name, "columns": list(columns), **fields}


@pytest.fixture
def shop_ds(tmp_path) -> Iterator[DatasourceConfig]:
    db = str(tmp_path / "shop.duckdb")
    seed_shop(db, dialect="duckdb")
    ds = DatasourceConfig(name=DS, type="duckdb", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


async def _store(tmp_path, ds: DatasourceConfig, backend: str, *docs: dict, orders: dict | None = None,
                 memories: tuple[dict, ...] = ()) -> StorageBackend:
    return await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds,
                           models=[orders or orders_v10(), customers_v10(), *docs], memories=memories)


async def _load(storage: StorageBackend, name: str) -> tuple[SlayerModel, list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        model = await storage.get_model(name, data_source=DS)
    assert model is not None
    return model, list(caught)


def _conversion_warnings(caught: list[warnings.WarningMessage], *, document: str, column: str) -> list[str]:
    return [str(w.message) for w in caught if document in str(w.message) and column in str(w.message)]


def _stage_of(model: SlayerModel) -> SlayerQuery:
    (stage,) = model.source_queries or []
    return stage


def _no_extension_columns(query: SlayerQuery) -> None:
    for extension in query.extensions or []:
        assert "columns" not in extension.model_dump(exclude_none=True)


def _rows(resp: Any, *keys: str) -> list[tuple]:
    return sorted(tuple(r[k] for k in keys) for r in resp.data)


# --------------------------------------------------------------------------- #
# Use sites.
# --------------------------------------------------------------------------- #
_TIER = _stage(extensions=[_ext("customers", {"name": "tier_label", "sql": "upper(region)"})],
               dimensions=["customers.tier_label"], measures=[_REV])


class TestDimension:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_joined_column_keeps_its_stage_column(self, tmp_path, shop_ds, backend: str) -> None:
        storage = await _store(tmp_path, shop_ds, backend, _qb("rev_by_tier", _TIER))
        model, caught = await _load(storage, "rev_by_tier")
        stage = _stage_of(model)
        _no_extension_columns(stage)
        assert ComputedDimension(expression="upper(customers.region)", name="customers__tier_label") in (
            stage.dimensions or [])
        assert len(_conversion_warnings(caught, document="rev_by_tier", column="tier_label")) == 1
        resp = await run(storage, {"source_model": "rev_by_tier", "dimensions": ["customers__tier_label"],
                                   "measures": [{"formula": "sum(rev)", "name": "t"}]})
        assert _rows(resp, "rev_by_tier.customers__tier_label", "rev_by_tier.t") == [("NORTH", 120.0), ("SOUTH", 90.0)]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_converted_model_is_written_back(self, tmp_path, shop_ds, backend: str) -> None:
        storage = await _store(tmp_path, shop_ds, backend, _qb("rev_by_tier", _TIER))
        model, _ = await _load(storage, "rev_by_tier")
        doc = await raw_doc(storage, "rev_by_tier")
        assert doc["version"] == CURRENT_VERSIONS["SlayerModel"]
        [stage] = doc["source_queries"]
        assert stage["version"] == CURRENT_VERSIONS["SlayerQuery"]
        assert all("columns" not in e for e in stage.get("extensions") or [])
        again, caught = await _load(storage, "rev_by_tier")
        assert again == model
        assert _conversion_warnings(caught, document="rev_by_tier", column="tier_label") == []
        async with owned_query_engine(storage=storage) as engine:
            by_name = await engine.execute("rev_by_tier")
        key = next(k for k in by_name.columns if k.endswith("tier_label"))
        assert {r[key]: r[next(k for k in by_name.columns if k.endswith("rev"))] for r in by_name.data} == {
            "NORTH": 120.0, "SOUTH": 90.0}

    async def test_a_root_column_keeps_its_name(self, tmp_path, shop_ds) -> None:
        stage = _stage(extensions=[_ext("orders", {"name": "status_label", "sql": "upper(status)", "type": "TEXT"})],
                       dimensions=["status_label"], measures=[_REV])
        storage = await _store(tmp_path, shop_ds, "yaml", _qb("by_status", stage))
        model, _ = await _load(storage, "by_status")
        assert ComputedDimension(expression="upper(status)", name="status_label") in (_stage_of(model).dimensions or [])
        resp = await run(storage, {"source_model": "by_status", "dimensions": ["status_label"],
                                   "measures": [{"formula": "sum(rev)", "name": "t"}]})
        assert _rows(resp, "by_status.status_label", "by_status.t") == [("2024/01/01", 60.0), ("NEW", 70.0), ("OK", 80.0)]


class TestMeasure:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_measure_keeps_its_generated_name(self, tmp_path, shop_ds, backend: str) -> None:
        stage = _stage(extensions=[_ext("orders", {"name": "double_amount", "sql": "amount * 2", "type": "DOUBLE"})],
                       dimensions=["status"], measures=["sum(double_amount)"])
        storage = await _store(tmp_path, shop_ds, backend, _qb("dbl", stage))
        model, caught = await _load(storage, "dbl")
        loaded = _stage_of(model)
        assert ModelMeasure(formula="sum(CAST(amount * 2 AS DOUBLE))", name="double_amount_sum") in (loaded.measures or [])
        assert len(_conversion_warnings(caught, document="dbl", column="double_amount")) == 1
        resp = await run(storage, {"source_model": "dbl", "measures": [{"formula": "sum(double_amount_sum)", "name": "t"}]})
        assert resp.data[0]["dbl.t"] == pytest.approx(420.0)

    async def test_a_joined_measure_takes_the_flattened_name(self, tmp_path, shop_ds) -> None:
        stage = _stage(extensions=[_ext("customers", {"name": "cid", "sql": "id * 10", "type": "INT"})],
                       dimensions=["status"], measures=["sum(customers.cid)"])
        storage = await _store(tmp_path, shop_ds, "yaml", _qb("cids", stage))
        model, _ = await _load(storage, "cids")
        assert ModelMeasure(formula="sum(CAST(customers.id * 10 AS INT))", name="customers__cid_sum") in (
            _stage_of(model).measures or [])
        resp = await run(storage, {"source_model": "cids", "dimensions": ["status"],
                                   "measures": [{"formula": "sum(customers__cid_sum)", "name": "t"}]})
        assert _rows(resp, "cids.status", "cids.t") == [("2024/01/01", 20.0), ("new", 40.0), ("ok", 60.0)]


class TestQualification:
    async def test_a_named_edge_qualifies_the_expression(self, tmp_path, shop_ds) -> None:
        buyer = {"target_model": "customers", "name": "buyer", "join_pairs": [["customer_id", "id"]],
                 "cardinality": "many_to_one"}
        stage = _stage(extensions=[_ext("customers", {"name": "region_label", "sql": "lower(region)"})],
                       dimensions=["buyer.region_label"], measures=[_REV])
        storage = await _store(tmp_path, shop_ds, "yaml", _qb("by_buyer", stage), orders=orders_v10(joins=[buyer]))
        model, _ = await _load(storage, "by_buyer")
        assert ComputedDimension(expression="lower(buyer.region)", name="buyer__region_label") in (
            _stage_of(model).dimensions or [])
        resp = await run(storage, {"source_model": "by_buyer", "dimensions": ["buyer__region_label"],
                                   "measures": [{"formula": "sum(rev)", "name": "t"}]})
        assert _rows(resp, "by_buyer.buyer__region_label", "by_buyer.t") == [("north", 120.0), ("south", 90.0)]

    async def test_an_inferred_population_qualifies_the_expression(self, tmp_path, shop_ds) -> None:
        stage = {"version": _QUERY_V, "extensions": [_ext("customers", {"name": "tier_label", "sql": "upper(region)"})],
                 "dimensions": ["status", "customers.tier_label"], "measures": [{"formula": "count(*)", "name": "n"}]}
        storage = await _store(tmp_path, shop_ds, "yaml", _qb("rootless", stage))
        model, _ = await _load(storage, "rootless")
        loaded = _stage_of(model)
        assert ComputedDimension(expression="upper(customers.region)", name="customers__tier_label") in (
            loaded.dimensions or [])
        resp = await run(storage, loaded)
        assert _rows(resp, "orders.status", "orders.customers__tier_label", "orders.n") == [
            ("2024/01/01", "SOUTH", 1), ("new", "NORTH", 2), ("ok", "NORTH", 2), ("ok", "SOUTH", 1)]

    async def test_an_alias_qualifies_the_expression(self, tmp_path, shop_ds) -> None:
        alias = {"target_model": "customers", "name": "cust", "join_pairs": [["customer_id", "id"]]}
        stage = _stage(extensions=[{"source_name": "orders", "joins": [alias]},
                                   _ext("customers", {"name": "region_label", "sql": "lower(region)"})],
                       dimensions=["cust.region_label"], measures=[_REV])
        storage = await _store(tmp_path, shop_ds, "yaml", _qb("by_alias", stage))
        model, _ = await _load(storage, "by_alias")
        assert ComputedDimension(expression="lower(cust.region)", name="cust__region_label") in (
            _stage_of(model).dimensions or [])
        resp = await run(storage, {"source_model": "by_alias", "dimensions": ["cust__region_label"],
                                   "measures": [{"formula": "sum(rev)", "name": "t"}]})
        assert _rows(resp, "by_alias.cust__region_label", "by_alias.t") == [("north", 120.0), ("south", 90.0)]


_Q_AMOUNT = "iif(status = 'ok', amount, NULL)"
_SHAPES = _stage(
    extensions=[_ext("orders",
                     {"name": "q_amount", "sql": "amount", "filter": "status = 'ok'"},
                     {"name": "q_double", "sql": "q_amount * 2"},
                     {"name": "status"})],
    dimensions=["order_id"],
    measures=["sum(q_amount)", "sum(q_double)", "count(status)"],
    filters=["q_double > 30", "status = 'ok'"],
    order=[{"column": {"name": "q_amount_sum"}, "direction": "desc", "raw_formula": "sum(q_amount)"}],
)
#: Hand-computed (order_id, q_amount_sum, q_double_sum, status_count), in result order.
_SHAPES_ROWS = [(4, 40.0, 80.0, 1), (3, 30.0, 60.0, 1)]


class TestColumnShapes:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_filtered_chained_and_sql_less_columns(self, tmp_path, shop_ds, backend: str) -> None:
        storage = await _store(tmp_path, shop_ds, backend, _qb("shapes", _SHAPES))
        model, caught = await _load(storage, "shapes")
        stage = _stage_of(model)
        _no_extension_columns(stage)
        formulas = {m.name: m.formula for m in stage.measures or []}
        assert formulas == {"q_amount_sum": f"sum({_Q_AMOUNT})", "q_double_sum": f"sum({_Q_AMOUNT} * 2)",
                            "status_count": "count(status)"}
        assert f"{_Q_AMOUNT} * 2" in (stage.filters or [])[0]
        assert (stage.filters or [])[1] == "status = 'ok'"
        assert _Q_AMOUNT in ((stage.order or [])[0].raw_formula or "")
        for column in ("q_amount", "q_double", "status"):
            assert len(_conversion_warnings(caught, document="shapes", column=column)) == 1
        resp = await run(storage, stage)
        keys = ("orders.order_id", "orders.q_amount_sum", "orders.q_double_sum", "orders.status_count")
        assert [tuple(r[k] for k in keys) for r in resp.data] == _SHAPES_ROWS

    @pytest.mark.parametrize(("type_", "sql", "expected"), [
        ("INT", "amount", "CAST(amount AS INT)"),
        ("DOUBLE", "amount", "CAST(amount AS DOUBLE)"),
        ("DATE", "ordered_at", "CAST(ordered_at AS DATE)"),
        ("TIMESTAMP", "ordered_at", "CAST(ordered_at AS TIMESTAMP)"),
        ("BOOLEAN", "amount > 25", "CAST(amount > 25 AS BOOLEAN)"),
        ("TEXT", "status", "status"),
        ("UNKNOWN", "status", "status"),
    ])
    async def test_a_declared_type_is_kept_as_a_cast(self, tmp_path, shop_ds, type_: str, sql: str, expected: str) -> None:
        stage = _stage(extensions=[_ext("orders", {"name": "c", "sql": sql, "type": type_})],
                       dimensions=["c"], measures=[_REV])
        storage = await _store(tmp_path, shop_ds, "yaml", _qb("typed", stage))
        model, _ = await _load(storage, "typed")
        assert ComputedDimension(expression=expected, name="c") in (_stage_of(model).dimensions or [])

    async def test_label_and_format_carry_over(self, tmp_path, shop_ds) -> None:
        column = {"name": "dbl", "sql": "amount * 2", "type": "DOUBLE", "label": "Double", "format": {"type": "currency"}}
        stage = _stage(extensions=[_ext("orders", column)], dimensions=["dbl"], measures=[_REV])
        storage = await _store(tmp_path, shop_ds, "yaml", _qb("labelled", stage))
        model, _ = await _load(storage, "labelled")
        [dim] = [d for d in _stage_of(model).dimensions or [] if isinstance(d, ComputedDimension)]
        assert dim.label == "Double"


_STAMP = {"name": "stamp", "sql": "ordered_at", "type": "TIMESTAMP"}
_BUCKET = {"name": "bucket", "sql": "iif(amount > 25, 'big', 'small')", "type": "TEXT"}


class TestEveryPosition:
    async def test_time_dimension_and_main_time_dimension(self, tmp_path, shop_ds) -> None:
        stage = _stage(extensions=[_ext("orders", _STAMP)], measures=[_REV], main_time_dimension="stamp",
                       time_dimensions=[{"dimension": {"name": "stamp"}, "granularity": "year"}])
        storage = await _store(tmp_path, shop_ds, "yaml", _qb("by_year", stage))
        model, _ = await _load(storage, "by_year")
        loaded = _stage_of(model)
        _no_extension_columns(loaded)
        assert "CAST(ordered_at AS TIMESTAMP)" in str(loaded.main_time_dimension)
        assert "CAST(ordered_at AS TIMESTAMP)" in str((loaded.time_dimensions or [])[0].dimension)
        resp = await run(storage, loaded)
        assert sorted(float(r["orders.rev"]) for r in resp.data) == [10.0, 50.0, 150.0]

    async def test_partition_and_ranking_keys_and_extension_measures(self, tmp_path, shop_ds) -> None:
        stage = _stage(
            extensions=[_ext("orders", _STAMP, _BUCKET,
                             measures=[{"name": "big_total", "formula": "sum(iif(bucket = 'big', amount, 0))"}])],
            dimensions=["status", "bucket"],
            measures=[{"formula": "sum(amount, partition_by=bucket)", "name": "per_bucket"},
                      {"formula": "last(amount, stamp)", "name": "latest"}, "big_total"],
        )
        storage = await _store(tmp_path, shop_ds, "yaml", _qb("keys", stage))
        model, _ = await _load(storage, "keys")
        loaded = _stage_of(model)
        _no_extension_columns(loaded)
        formulas = {m.name: m.formula for m in loaded.measures or []}
        assert "iif(amount > 25, 'big', 'small')" in formulas["per_bucket"]
        assert "CAST(ordered_at AS TIMESTAMP)" in formulas["latest"]
        [big_total] = [m.formula for e in loaded.extensions or [] for m in e.measures or [] if m.name == "big_total"]
        assert "iif(amount > 25, 'big', 'small')" in big_total
        resp = await run(storage, loaded)
        keys = ("orders.status", "orders.bucket", "orders.per_bucket", "orders.latest", "orders.big_total")
        assert _rows(resp, *keys) == [
            ("2024/01/01", "big", 180.0, 60.0, 60.0), ("new", "big", 180.0, 50.0, 50.0),
            ("new", "small", 30.0, 20.0, 0.0), ("ok", "big", 180.0, 40.0, 70.0), ("ok", "small", 30.0, 10.0, 0.0),
        ]


# --------------------------------------------------------------------------- #
# Unconvertible documents.
# --------------------------------------------------------------------------- #
_UNCONVERTIBLE = {
    "inline-source": _stage(source_model={"name": "x", "sql_table": "orders", "data_source": DS,
                                          "columns": [{"name": "amount", "type": "DOUBLE"}]}, measures=[_REV]),
    "not-mode-b": _stage(extensions=[_ext("orders", {"name": "yr", "sql": "strftime('%Y', ordered_at)"})],
                         dimensions=["yr"], measures=[_REV]),
    "array": _stage(extensions=[_ext("orders", {"name": "arr", "sql": "amount", "type": "ARRAY"})],
                    dimensions=["status"], measures=[_REV]),
    "hidden-sql-less": _stage(extensions=[_ext("orders", {"name": "nope"})], dimensions=["nope"], measures=[_REV]),
    "shadowing": _stage(extensions=[_ext("orders", {"name": "status", "sql": "upper(status)"})],
                        dimensions=["status"], measures=[_REV]),
    "cycle": _stage(extensions=[_ext("orders", {"name": "a", "sql": "b + 1"}, {"name": "b", "sql": "a + 1"})],
                    dimensions=["a"], measures=[_REV]),
    "join-key-source": _stage(
        extensions=[_ext("orders", {"name": "cid", "sql": "customer_id"},
                         joins=[{"target_model": "customers", "name": "c2", "join_pairs": [["cid", "id"]]}])],
        dimensions=["c2.region"], measures=[_REV]),
    "join-key-target": _stage(
        extensions=[{"source_name": "orders",
                     "joins": [{"target_model": "customers", "name": "c3", "join_pairs": [["customer_id", "kid"]]}]},
                    _ext("customers", {"name": "kid", "sql": "id"})],
        dimensions=["c3.region"], measures=[_REV]),
    "unattributable": _stage(extensions=[_ext("ghosts", {"name": "g", "sql": "id"})], measures=[_REV]),
}


class TestUnconvertible:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("case", sorted(_UNCONVERTIBLE))
    async def test_fails_to_load_untouched_and_others_load(self, tmp_path, shop_ds, backend: str, case: str) -> None:
        good = _qb("good", _TIER)
        storage = await _store(tmp_path, shop_ds, backend, good, _qb("bad", _UNCONVERTIBLE[case]))
        base = str(tmp_path / "store")
        before = stored_bytes(backend=backend, base=base, name="bad")
        with pytest.raises(StoredDocumentLoadError) as exc:
            await storage.get_model("bad", data_source=DS)
        assert f"{DS}.bad" in str(exc.value)
        assert stored_bytes(backend=backend, base=base, name="bad") == before
        loaded, failed = await storage.load_models(data_source=DS)
        assert {m.name for m in loaded} == {"orders", "customers", "good"}
        assert [e.name for e in failed] == ["bad"]


# --------------------------------------------------------------------------- #
# Memories.
# --------------------------------------------------------------------------- #
def _memory_bytes(*, backend: str, base: str, storage: StorageBackend, memory_id: str) -> str:
    if backend == "yaml":
        with open(storage._memory_md_path(memory_id), encoding="utf-8") as f:  # type: ignore[attr-defined]  # NOSONAR(S7493) — test read
            return f.read()
    with transaction(os.path.join(base, "store.db")) as conn:
        return conn.execute("SELECT data FROM memories WHERE id = ?", (memory_id,)).fetchone()[0]


class TestMemories:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_memory_converts_on_every_load_without_write_back(self, tmp_path, shop_ds, backend: str) -> None:
        memory = memory_v2(memory_id="m_tier", query=_TIER, version=_MEMORY_V)
        storage = await _store(tmp_path, shop_ds, backend, memories=(memory,))
        base = str(tmp_path / "store")
        before = _memory_bytes(backend=backend, base=base, storage=storage, memory_id="m_tier")
        for _ in range(2):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                loaded = await storage.get_memory("m_tier")
            assert loaded.query is not None
            _no_extension_columns(loaded.query)
            assert ComputedDimension(expression="upper(customers.region)", name="customers__tier_label") in (
                loaded.query.dimensions or [])
            assert len(_conversion_warnings(list(caught), document="m_tier", column="tier_label")) == 1
        assert _memory_bytes(backend=backend, base=base, storage=storage, memory_id="m_tier") == before
        query = (await storage.get_memory("m_tier")).query
        assert query is not None
        resp = await run(storage, query)
        assert _rows(resp, "orders.customers__tier_label", "orders.rev") == [("NORTH", 120.0), ("SOUTH", 90.0)]

    async def test_an_unconvertible_memory_fails_to_load(self, tmp_path, shop_ds) -> None:
        memory = memory_v2(memory_id="m_bad", query=_UNCONVERTIBLE["not-mode-b"], version=_MEMORY_V)
        storage = await _store(tmp_path, shop_ds, "yaml", memories=(memory,))
        with pytest.raises(StoredDocumentLoadError) as exc:
            await storage.get_memory("m_bad")
        assert "m_bad" in str(exc.value)


class TestFreshInputIsNeverConverted:
    def test_a_versioned_payload_outside_storage_is_refused(self) -> None:
        with pytest.raises(ValueError, match="columns"):
            SlayerQuery.model_validate(_TIER)

    def test_a_versioned_model_document_outside_storage_is_refused(self) -> None:
        with pytest.raises(ValueError, match="columns"):
            SlayerModel.model_validate(yaml.safe_load(yaml.safe_dump(_qb("rev_by_tier", _TIER))))
