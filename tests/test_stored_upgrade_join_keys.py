"""Undeclared join keys become hidden base columns of their own document on load (spec: models/join-keys)."""

from __future__ import annotations

import warnings

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.models import DatasourceConfig
from slayer.sql import engine_factory
from slayer.storage import migrations as mig
from tests._stored_upgrade_fixtures import (
    AMOUNT_BY_REGION,
    BACKENDS,
    DS,
    by_dimension,
    column_of,
    customers_v10,
    orders_v10,
    raw_doc,
    raw_store,
    run,
    seed_shop,
    stored_bytes,
)

REGION_QUERY = {"source_model": "orders", "dimensions": ["customers.region"], "measures": ["sum(amount)"]}


@pytest.fixture(params=["duckdb", "sqlite"])
def shop_ds(request, tmp_path):
    dialect = request.param
    db = str(tmp_path / f"shop.{dialect}")
    seed_shop(db, dialect=dialect)
    ds = DatasourceConfig(name=DS, type=dialect, database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


def _hidden(doc: dict, name: str) -> dict:
    [col] = [c for c in doc["columns"] if c.get("name") == name]
    return col


def _join_on(sql: str, *, dialect: str) -> Expression:
    [join] = list(sqlglot.parse_one(sql, read=dialect).find_all(exp.Join))
    on = join.args.get("on")
    assert on is not None
    return on


def _assert_cast_free_key_equality(sql: str, *, dialect: str) -> None:
    on = _join_on(sql, dialect=dialect)
    assert not list(on.find_all(exp.Cast, exp.TryCast)), on.sql(dialect=dialect)
    assert isinstance(on, exp.EQ), on.sql(dialect=dialect)
    assert {c.name for c in on.find_all(exp.Column)} == {"customer_id", "id"}


class TestSourceSide:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_cube_import_shape_loads_with_hidden_key(self, tmp_path, shop_ds, backend):
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                                  models=[orders_v10(declare_fk=False), customers_v10()])
        orders = await storage.get_model("orders", data_source=DS)
        assert orders is not None
        col = column_of(orders, "customer_id")
        assert col.hidden is True
        doc = await raw_doc(storage, "orders")
        assert doc["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
        assert _hidden(doc, "customer_id")["hidden"] is True
        assert _hidden(doc, "customer_id")["type"] == "INT"

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_query_returns_the_0_10_2_rows_with_cast_free_on(self, tmp_path, shop_ds, backend):
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                                  models=[orders_v10(declare_fk=False), customers_v10()])
        resp = await run(storage, REGION_QUERY)
        assert by_dimension(resp, dim_suffix="region") == AMOUNT_BY_REGION
        dry = await run(storage, REGION_QUERY, dry_run=True)
        assert dry.sql is not None
        _assert_cast_free_key_equality(dry.sql, dialect=shop_ds.type)

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_second_load_is_a_noop_and_resave_passes(self, tmp_path, shop_ds, backend):
        base = str(tmp_path / "store")
        storage = await raw_store(backend=backend, base=base, datasource=shop_ds,
                                  models=[orders_v10(declare_fk=False), customers_v10()])
        assert await storage.get_model("orders", data_source=DS) is not None
        after_first = stored_bytes(backend=backend, base=base, name="orders")
        orders = await storage.get_model("orders", data_source=DS)
        assert orders is not None
        assert stored_bytes(backend=backend, base=base, name="orders") == after_first
        await storage.save_model(orders)

    async def test_hidden_key_type_defaults_when_peer_type_is_unknown(self, tmp_path, shop_ds):
        customers = customers_v10()
        customers["columns"][0]["type"] = "not_a_type"
        storage = await raw_store(backend="yaml", base=str(tmp_path / "store"), datasource=shop_ds,
                                  models=[orders_v10(declare_fk=False), customers])
        await storage.get_model("orders", data_source=DS)
        doc = await raw_doc(storage, "orders")
        assert _hidden(doc, "customer_id").get("type", "TEXT") == "TEXT"


class TestTargetSide:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("target_version", [10, 13])
    async def test_undeclared_target_key_becomes_hidden(self, tmp_path, shop_ds, backend, target_version):
        storage = await raw_store(
            backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
            models=[orders_v10(), customers_v10(declare_pk=False, version=target_version)])
        customers = await storage.get_model("customers", data_source=DS)
        assert customers is not None
        col = column_of(customers, "id")
        assert col.hidden is True
        assert _hidden(await raw_doc(storage, "customers"), "id")["type"] == "INT"
        resp = await run(storage, REGION_QUERY)
        assert by_dimension(resp, dim_suffix="region") == AMOUNT_BY_REGION

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_query_through_undeclared_target_key(self, tmp_path, shop_ds, backend):
        storage = await raw_store(
            backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
            models=[orders_v10(), customers_v10(declare_pk=False)])
        resp = await run(storage, REGION_QUERY)
        assert by_dimension(resp, dim_suffix="region") == AMOUNT_BY_REGION
        orders = await raw_doc(storage, "orders")
        assert [c["name"] for c in orders["columns"]].count("id") == 0

    async def test_query_backed_target_is_untouched(self, tmp_path, shop_ds):
        qb = {"version": 10, "name": "customers", "data_source": DS,
              "source_queries": [{"source_model": "people", "dimensions": ["region"],
                                  "measures": [{"formula": "count(*)", "name": "n"}]}]}
        people = {**customers_v10(), "name": "people"}
        storage = await raw_store(backend="yaml", base=str(tmp_path / "store"), datasource=shop_ds,
                                  models=[orders_v10(), qb, people])
        model = await storage.get_model("customers", data_source=DS)
        assert model is not None
        assert "id" not in {c.name for c in model.columns}


class TestLeftForValidation:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_dotted_key_fails_while_siblings_load(self, tmp_path, shop_ds, backend):
        orders = orders_v10(declare_fk=False, joins=[
            {"target_model": "customers", "join_pairs": [["orders.customer_id", "id"]]}])
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                                  models=[orders, customers_v10()])
        with pytest.raises(ValueError, match=r"orders\.customer_id"):
            await storage.get_model("orders", data_source=DS)
        assert await storage.get_model("customers", data_source=DS) is not None
        assert "orders.customer_id" not in {c["name"] for c in (await raw_doc(storage, "orders"))["columns"]}

    async def test_corrupt_sibling_does_not_block_the_target_scan(self, tmp_path, shop_ds):
        storage = await raw_store(
            backend="sqlite", base=str(tmp_path / "store"), datasource=shop_ds,
            models=[orders_v10(), customers_v10(declare_pk=False)],
            raw_rows=[("broken", "{not json")])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            customers = await storage.get_model("customers", data_source=DS)
            orders = await storage.get_model("orders", data_source=DS)
        assert orders is not None
        assert customers is not None
        assert "id" in {c.name for c in customers.columns}
        assert not [w for w in caught if "broken" in str(w.message)]
        with pytest.raises(ValueError, match="broken"):
            await storage.get_model("broken", data_source=DS)

    async def test_corrupt_sibling_warns_once_per_enumeration(self, tmp_path, shop_ds):
        storage = await raw_store(
            backend="sqlite", base=str(tmp_path / "store"), datasource=shop_ds,
            models=[orders_v10(), customers_v10(declare_pk=False)],
            raw_rows=[("broken", "{not json")])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            [spine] = await storage.builtin_models(DS, detailed=True)
        assert spine is not None
        unloadable = [w for w in caught
                      if getattr(getattr(w.message, "payload", None), "kind", None) == "unloadable_document"]
        assert len(unloadable) == 1
        assert "broken" in str(unloadable[0].message)
