"""A stored join target names its model's exact spelling: refused at save, repaired on load, fail-closed at query time
(spec: queries/identifier-case)."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import (
    AmbiguousReferenceError,
    HiddenContentConflictError,
    JoinTargetCaseError,
    SlayerError,
    UnknownJoinTargetError,
)
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.database import engine_factory
from slayer.storage import migrations as mig
from slayer.storage.access_filtered import AccessFilteredStorage
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2215_fixtures import messages, recorded_warnings
from tests._stored_upgrade_fixtures import (
    AMOUNT_BY_REGION,
    BACKENDS,
    DS,
    by_dimension,
    customers_v10,
    orders_v10,
    raw_doc,
    raw_store,
    run,
    seed_shop,
    stored_bytes,
)

STORAGES = ("yaml", "sqlite")
#: Last ``SlayerModel`` version before the stored-target repair.
PRE_REPAIR = 22
#: Orders per customer name in the shop seed.
ORDERS_BY_NAME = {"Ann": 2, "Bob": 2, "Cid": 2}
NAME_QUERY = {"source_model": "orders", "dimensions": ["customers.name"],
              "measures": [{"formula": "count(*)", "name": "n"}]}
REGION_QUERY = {"source_model": "orders", "dimensions": ["customers.region"], "measures": ["sum(amount)"]}


# --------------------------------------------------------------------------- #
# Fixtures and models
# --------------------------------------------------------------------------- #
@pytest.fixture(params=STORAGES)
async def storage(request, tmp_path) -> StorageBackend:
    store: StorageBackend = (YAMLStorage(base_dir=str(tmp_path)) if request.param == "yaml"
                             else SQLiteStorage(db_path=os.path.join(tmp_path, "store.db")))
    for name in ("test", "other"):
        await store.save_datasource(DatasourceConfig(name=name, type="sqlite", database=":memory:"))
    return store


@pytest.fixture(params=["duckdb", "sqlite"])
def shop_ds(request, tmp_path) -> Iterator[DatasourceConfig]:
    db = str(tmp_path / f"shop.{request.param}")
    seed_shop(db, dialect=request.param)
    ds = DatasourceConfig(name=DS, type=request.param, database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


def customers(name: str = "Customers", *, data_source: str = "test", tags: tuple[str, ...] = ()) -> SlayerModel:
    return SlayerModel(
        name=name, sql_table="customers", data_source=data_source, access_tags=list(tags),
        columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="name", type=DataType.TEXT)],
    )


def orders(target: str = "customers", *, tags: tuple[str, ...] = ()) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test", access_tags=list(tags),
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="customer_id", type=DataType.INT)],
        joins=[ModelJoin(target_model=target, name="buyer", join_pairs=[["customer_id", "id"]])],
    )


def _text(err: pytest.ExceptionInfo[BaseException]) -> str:
    return str(err.value)


def test_the_error_is_typed() -> None:
    assert issubclass(JoinTargetCaseError, SlayerError)
    assert issubclass(JoinTargetCaseError, ValueError)


# --------------------------------------------------------------------------- #
# Save refusals
# --------------------------------------------------------------------------- #
class TestSaveRefusals:
    async def test_a_case_only_target_is_refused_naming_the_declared_spelling(self, storage: StorageBackend) -> None:
        await storage.save_model(customers())
        model = orders()
        with pytest.raises(JoinTargetCaseError) as err:
            await storage.save_model(model)
        for needle in ("orders", "buyer", "'customers'", "Customers"):
            assert needle in _text(err)
        assert await storage.get_model("orders", data_source="test") is None

    async def test_the_target_saved_second_is_refused(self, storage: StorageBackend) -> None:
        await storage.save_model(orders())
        model = customers()
        with pytest.raises(JoinTargetCaseError) as err:
            await storage.save_model(model)
        for needle in ("orders", "buyer", "'customers'"):
            assert needle in _text(err)
        assert await storage.get_model("Customers", data_source="test") is None

    async def test_respelling_the_join_first_lets_the_target_save(self, storage: StorageBackend) -> None:
        await storage.save_model(orders())
        await storage.save_model(orders("Customers"))
        await storage.save_model(customers())
        stored = await storage.get_model("orders", data_source="test")
        assert stored is not None
        assert [j.target_model for j in stored.joins] == ["Customers"]

    @pytest.mark.parametrize("already_stored", [False, True], ids=["new-join", "unchanged-stored-join"])
    async def test_an_ambiguous_target_is_refused(self, storage: StorageBackend, already_stored: bool) -> None:
        await storage.save_model(customers("Customers"), _validate=False)
        await storage.save_model(customers("CUSTOMERS"), _validate=False)
        if already_stored:
            await storage.save_model(orders(), _validate=False)
        model = orders()
        with pytest.raises(AmbiguousReferenceError) as err:
            await storage.save_model(model)
        assert {"Customers", "CUSTOMERS"} <= set(err.value.candidates)

    async def test_a_unicode_case_fold_is_a_case_only_target(self, storage: StorageBackend) -> None:
        await storage.save_model(customers("straße"))
        model = orders("STRASSE")
        with pytest.raises(JoinTargetCaseError) as err:
            await storage.save_model(model)
        assert "STRASSE" in _text(err)
        assert "straße" in _text(err)

    async def test_a_dangling_target_is_accepted(self, storage: StorageBackend) -> None:
        await storage.save_model(orders("suppliers"))
        stored = await storage.get_model("orders", data_source="test")
        assert stored is not None
        assert [j.target_model for j in stored.joins] == ["suppliers"]

    async def test_a_model_of_another_datasource_is_not_a_target(self, storage: StorageBackend) -> None:
        await storage.save_model(customers(data_source="other"))
        await storage.save_model(orders())
        stored = await storage.get_model("orders", data_source="test")
        assert stored is not None
        assert [j.target_model for j in stored.joins] == ["customers"]


# --------------------------------------------------------------------------- #
# Access-filtered saves
# --------------------------------------------------------------------------- #
def _as_fin(storage: StorageBackend, *, bypass: bool = False) -> AccessFilteredStorage:
    return AccessFilteredStorage(storage, tags={"fin"}, bypass=bypass)


class TestAccessFilteredSaves:
    async def test_the_case_only_error_precedes_the_unknown_target_error(self, storage: StorageBackend) -> None:
        await storage.save_model(customers())
        view, model = _as_fin(storage), orders()
        with pytest.raises(JoinTargetCaseError) as err:
            await view.save_model(model)
        assert "Customers" in _text(err)

    async def test_a_hidden_peers_case_only_target_is_not_revealed(self, storage: StorageBackend) -> None:
        await storage.save_model(orders(tags=("hr",)))
        view, model = _as_fin(storage), customers()
        with pytest.raises(HiddenContentConflictError) as err:
            await view.save_model(model)
        assert "orders" not in _text(err)
        assert "buyer" not in _text(err)
        assert await storage.get_model("Customers", data_source="test") is None

    async def test_a_hidden_target_is_unknown_to_the_caller(self, storage: StorageBackend) -> None:
        await storage.save_model(customers(tags=("hr",)))
        view, model = _as_fin(storage), orders()
        with pytest.raises(UnknownJoinTargetError) as err:
            await view.save_model(model)
        assert "Customers" not in _text(err)

    async def test_bypass_behaves_as_the_inner_store(self, storage: StorageBackend) -> None:
        await storage.save_model(customers(tags=("hr",)))
        view, model = _as_fin(storage, bypass=True), orders()
        with pytest.raises(JoinTargetCaseError) as err:
            await view.save_model(model)
        assert "Customers" in _text(err)


# --------------------------------------------------------------------------- #
# The version-22 upgrade of stored documents
# --------------------------------------------------------------------------- #
def _customers_doc(name: str = "Customers", **kw) -> dict:
    return {**customers_v10(version=PRE_REPAIR, **kw), "name": name}


def _orders_doc(**kw) -> dict:
    return orders_v10(version=PRE_REPAIR, **kw)


async def _store(tmp_path, *, backend: str, ds: DatasourceConfig | None, docs: list[dict],
                 base: str = "store") -> StorageBackend:
    return await raw_store(backend=backend, base=str(tmp_path / base), datasource=ds, models=docs)


async def _targets(storage: StorageBackend, name: str) -> list[str]:
    model = await storage.get_model(name, data_source=DS)
    assert model is not None
    return [j.target_model for j in model.joins]


class TestStoredUpgrade:
    def test_the_upgrade_is_a_version_step(self) -> None:
        assert mig.CURRENT_VERSIONS["SlayerModel"] > PRE_REPAIR

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_stored_case_only_target_is_repaired_on_load(self, tmp_path, shop_ds, backend) -> None:
        storage = await _store(tmp_path, backend=backend, ds=shop_ds, docs=[_orders_doc(), _customers_doc()])
        assert await _targets(storage, "orders") == ["Customers"]
        doc = await raw_doc(storage, "orders")
        assert doc["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
        assert [j["target_model"] for j in doc["joins"]] == ["Customers"]
        resp = await run(storage, NAME_QUERY)
        assert {k: int(v) for k, v in by_dimension(resp, dim_suffix="name").items()} == ORDERS_BY_NAME
        written = stored_bytes(backend=backend, base=str(tmp_path / "store"), name="orders")
        assert await _targets(storage, "orders") == ["Customers"]
        assert stored_bytes(backend=backend, base=str(tmp_path / "store"), name="orders") == written

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_ambiguous_and_dangling_targets_are_unchanged(self, tmp_path, backend) -> None:
        ambiguous = await _store(tmp_path, backend=backend, ds=None, base="ambiguous",
                                 docs=[_orders_doc(), _customers_doc("Customers"), _customers_doc("CUSTOMERS")])
        assert await _targets(ambiguous, "orders") == ["customers"]
        dangling = await _store(tmp_path, backend=backend, ds=None, base="dangling", docs=[_orders_doc()])
        assert await _targets(dangling, "orders") == ["customers"]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_repair_precedes_join_key_repair(self, tmp_path, shop_ds, backend) -> None:
        target = _customers_doc()
        target["columns"][0] = {"name": "cid", "sql": "id", "type": "INT", "primary_key": True}
        storage = await _store(tmp_path, backend=backend, ds=shop_ds, docs=[_orders_doc(declare_fk=False), target])
        model = await storage.get_model("orders", data_source=DS)
        assert model is not None
        [join] = model.joins
        assert (join.target_model, join.join_pairs) == ("Customers", [["customer_id", "cid"]])
        assert {c.name: c.hidden for c in model.columns}.get("customer_id") is True
        resp = await run(storage, REGION_QUERY)
        assert by_dimension(resp, dim_suffix="region") == AMOUNT_BY_REGION

    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("first", ["orders", "Customers"])
    async def test_repair_precedes_exact_inverse_dedup(self, tmp_path, shop_ds, backend, first) -> None:
        mirror = {**_customers_doc(), "joins": [
            {"target_model": "orders", "join_pairs": [["id", "customer_id"]], "cardinality": "one_to_many"}]}
        storage = await _store(tmp_path, backend=backend, ds=shop_ds, docs=[_orders_doc(), mirror])
        second = "Customers" if first == "orders" else "orders"
        loaded = {name: await _targets(storage, name) for name in (first, second)}
        assert loaded["orders"].count("Customers") + loaded["Customers"].count("orders") == 1
        resp = await run(storage, REGION_QUERY)
        assert by_dimension(resp, dim_suffix="region") == AMOUNT_BY_REGION


# --------------------------------------------------------------------------- #
# Query-time backstop
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("backend", BACKENDS)
async def test_an_ambiguous_stored_target_fails_closed_at_query_time(tmp_path, shop_ds, backend) -> None:
    storage = await _store(tmp_path, backend=backend, ds=shop_ds,
                           docs=[_orders_doc(), _customers_doc("Customers"), _customers_doc("CUSTOMERS")])
    assert await _targets(storage, "orders") == ["customers"]
    with pytest.raises(AmbiguousReferenceError) as err:
        await run(storage, NAME_QUERY)
    assert {"Customers", "CUSTOMERS"} <= set(err.value.candidates)


# --------------------------------------------------------------------------- #
# A stored join to the spine is dropped on load
# --------------------------------------------------------------------------- #
def _ev_doc(*, joins: list[dict]) -> dict:
    """No time axis: two temporal columns and no declared default."""
    return {
        "version": PRE_REPAIR, "name": "ev", "sql_table": "orders", "data_source": DS,
        "columns": [
            {"name": "order_id", "type": "INT", "primary_key": True},
            {"name": "amount", "type": "DOUBLE"},
            {"name": "ordered_at", "type": "TIMESTAMP"},
            {"name": "ordered_day", "sql": "ordered_at", "type": "TIMESTAMP"},
        ],
        "joins": joins,
    }


SPINE_QUERY = {
    "time_dimensions": [{"dimension": "time_spine.timestamp", "granularity": "month",
                         "date_range": ["2024-01-01", "2024-03-31"]}],
    "measures": [{"formula": "sum(ev.amount)", "name": "s"}],
}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("target", ["time_spine", "TIME_SPINE"])
async def test_a_stored_join_to_the_spine_is_dropped_on_load(tmp_path, shop_ds, backend, target) -> None:
    spine_join = {"target_model": target, "name": "spine_link", "join_pairs": [["ordered_at", "timestamp"]]}
    storage = await _store(tmp_path, backend=backend, ds=shop_ds, docs=[_ev_doc(joins=[spine_join])])
    with recorded_warnings() as caught:
        model = await storage.get_model("ev", data_source=DS)
    assert model is not None
    assert model.joins == []
    assert [w for w in messages(caught, "spine_link") if "ev" in w]
    doc = await raw_doc(storage, "ev")
    assert (doc["version"], doc.get("joins") or []) == (mig.CURRENT_VERSIONS["SlayerModel"], [])
    resp = await run(storage, SPINE_QUERY)
    plain = await _store(tmp_path, backend=backend, ds=shop_ds, base="plain", docs=[_ev_doc(joins=[])])
    expected = await run(plain, SPINE_QUERY)
    assert {str(r["time_spine.timestamp"])[:7]: float(r["time_spine.s"]) for r in resp.data} == {
        "2024-01": 210.0, "2024-02": 210.0, "2024-03": 210.0}
    assert sorted(map(str, resp.data)) == sorted(map(str, expected.data))
