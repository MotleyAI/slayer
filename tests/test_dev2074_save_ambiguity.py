"""Save-time ambiguity: a Mode-A definition crossing an ambiguous hop is rejected at
both save doors, and an edge-set change warns about the definitions it breaks."""

from __future__ import annotations

import warnings

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.errors import (
    AmbiguousJoinPathError,
    CircularJoinPathError,
    DerivedColumnAmbiguousError,
    DerivedColumnCircularError,
    DerivedColumnFanningError,
)
from slayer.core.join_walker import observe_traversals
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

DS = "ds"
M2O = JoinCardinality.MANY_TO_ONE


def _pk() -> Column:
    return Column(name="id", type=DataType.INT, primary_key=True)


def _int(name: str) -> Column:
    return Column(name=name, type=DataType.INT)


def _m(name: str, *cols: Column, joins: list[ModelJoin] | None = None,
       filters: list[str] | None = None) -> SlayerModel:
    return SlayerModel(name=name, data_source=DS, sql_table=name, columns=[_pk(), *cols],
                       joins=joins or [], filters=filters or [])


def _j(target: str, src: str, *, tgt: str = "id", name: str | None = None,
       cardinality: JoinCardinality | None = M2O) -> ModelJoin:
    return ModelJoin(target_model=target, join_pairs=[[src, tgt]], name=name, cardinality=cardinality)


def _addresses() -> SlayerModel:
    return _m("addresses", Column(name="city", type=DataType.TEXT))


def _orders(*cols: Column, joins: list[ModelJoin] | None = None,
            filters: list[str] | None = None) -> SlayerModel:
    return _m("orders", _int("address_id"), _int("ship_address_id"),
              Column(name="amount", type=DataType.DOUBLE), *cols,
              joins=joins if joins is not None else [_j("addresses", "address_id")], filters=filters)


def _parallel(*, named: bool = False) -> list[ModelJoin]:
    return [_j("addresses", "address_id", name="billing_address" if named else None),
            _j("addresses", "ship_address_id", name="shipping_address" if named else None)]


async def _storage(tmp_path) -> YAMLStorage:
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite"))
    return storage


async def _save(door: str, storage: YAMLStorage, model: SlayerModel) -> None:
    if door == "engine":
        await SlayerQueryEngine(storage=storage).save_model(model)
    else:
        await storage.save_model(model)


async def _both_doors(tmp_path, model: SlayerModel, *peers: SlayerModel,
                      expected: type[BaseException]) -> tuple[BaseException, BaseException]:
    storage = await _storage(tmp_path)
    for peer in peers:
        await storage.save_model(peer)
    with pytest.raises(expected) as storage_ei:
        await _save("storage", storage, model)
    with pytest.raises(expected) as engine_ei:
        await _save("engine", storage, model)
    assert str(storage_ei.value) == str(engine_ei.value)
    return storage_ei.value, engine_ei.value


def _assert_names_both_edges(exc: AmbiguousJoinPathError) -> None:
    assert exc.source_model == "orders"
    assert exc.target_model == "addresses"
    assert sorted(tuple(map(tuple, c.join_pairs)) for c in exc.candidates) == [
        (("address_id", "id"),), (("ship_address_id", "id"),),
    ]
    msg = str(exc)
    assert "address_id=id" in msg
    assert "ship_address_id=id" in msg
    assert "edge name" in msg.lower()


@pytest.fixture(params=["storage", "engine"])
def door(request) -> str:
    return request.param


class TestAmbiguousDefinitionRejected:
    async def test_column_sql_rejected_at_both_doors(self, tmp_path) -> None:
        model = _orders(Column(name="city", type=DataType.TEXT, sql="addresses.city"), joins=_parallel())
        for exc in await _both_doors(tmp_path, model, _addresses(), expected=DerivedColumnAmbiguousError):
            assert isinstance(exc, DerivedColumnAmbiguousError)
            assert isinstance(exc, AmbiguousJoinPathError)
            assert exc.column == "city"
            assert exc.model == "orders"
            assert exc.reference == "addresses.city"
            _assert_names_both_edges(exc)
            msg = str(exc)
            assert "'city'" in msg
            assert "'orders'" in msg
            assert "'addresses.city'" in msg

    async def test_column_filter_rejected_at_both_doors(self, tmp_path) -> None:
        model = _orders(
            Column(name="paris_amount", type=DataType.DOUBLE, sql="amount",
                   filter="addresses.city = 'Paris'"),
            joins=_parallel(),
        )
        for exc in await _both_doors(tmp_path, model, _addresses(), expected=DerivedColumnAmbiguousError):
            assert isinstance(exc, DerivedColumnAmbiguousError)
            assert exc.column == "paris_amount"
            assert exc.reference == "addresses.city"
            _assert_names_both_edges(exc)

    async def test_model_filter_rejected_with_the_base_error(self, tmp_path) -> None:
        model = _orders(joins=_parallel(), filters=["addresses.city = 'Paris'"])
        for exc in await _both_doors(tmp_path, model, _addresses(), expected=AmbiguousJoinPathError):
            assert not isinstance(exc, DerivedColumnAmbiguousError)
            assert "addresses.city = 'Paris'" in str(exc)
            _assert_names_both_edges(exc)

    async def test_transitive_reference_names_the_inner_column(self, tmp_path) -> None:
        model = _orders(
            Column(name="loud_city", type=DataType.TEXT, sql="upper(city)"),
            Column(name="city", type=DataType.TEXT, sql="addresses.city"),
            joins=_parallel(),
        )
        for exc in await _both_doors(tmp_path, model, _addresses(), expected=DerivedColumnAmbiguousError):
            assert isinstance(exc, DerivedColumnAmbiguousError)
            assert exc.column == "city"
            assert exc.reference == "addresses.city"

    async def test_revisit_after_an_ambiguous_hop_is_rejected(self, tmp_path) -> None:
        regions = _m("regions", Column(name="name", type=DataType.TEXT))
        customers = _m(
            "customers", _int("region_id"), _int("hq_region_id"),
            Column(name="spend", type=DataType.DOUBLE),
            Column(name="revisit_spend", type=DataType.DOUBLE, sql="regions.customers.spend"),
            joins=[_j("regions", "region_id", name="r1"), _j("regions", "hq_region_id", name="r2")],
        )
        for exc in await _both_doors(tmp_path, customers, regions, expected=DerivedColumnAmbiguousError):
            assert isinstance(exc, DerivedColumnAmbiguousError)
            assert not isinstance(exc, CircularJoinPathError)
            assert exc.column == "revisit_spend"
            assert exc.reference == "regions.customers.spend"
            assert {c.name for c in exc.candidates} == {"r1", "r2"}

    async def test_ambiguity_precedes_a_later_fanning_hop(self, tmp_path) -> None:
        visits = _m("visits", _int("address_id"), Column(name="n", type=DataType.INT),
                    joins=[_j("addresses", "address_id")])
        model = _orders(Column(name="visit_n", type=DataType.INT, sql="addresses.visits.n"),
                        joins=_parallel())
        storage = await _storage(tmp_path)
        await storage.save_model(_addresses())
        await storage.save_model(visits)
        for d in ("storage", "engine"):
            with pytest.raises(DerivedColumnAmbiguousError) as ei:
                await _save(d, storage, model)
            assert not isinstance(ei.value, DerivedColumnFanningError)

    async def test_revisit_before_an_ambiguous_hop_stays_circular(self, tmp_path) -> None:
        regions = _m("regions", Column(name="name", type=DataType.TEXT))
        customers = _m("customers", _int("region_id"), _int("hq_region_id"),
                       joins=[_j("regions", "region_id"), _j("regions", "hq_region_id")])
        model = _m("orders", _int("customer_id"),
                   Column(name="rn", type=DataType.TEXT, sql="customers.orders.customers.regions.name"),
                   joins=[_j("customers", "customer_id")])
        for exc in await _both_doors(tmp_path, model, regions, customers, expected=DerivedColumnCircularError):
            assert exc.revisited == "orders"

    async def test_named_hop_through_the_same_pair_saves(self, tmp_path, door) -> None:
        storage = await _storage(tmp_path)
        await storage.save_model(_addresses())
        model = _orders(Column(name="city", type=DataType.TEXT, sql="billing_address.city"),
                        joins=_parallel(named=True))
        await _save(door, storage, model)
        assert await storage.get_model("orders", data_source=DS) is not None


# --------------------------------------------------------------------------- #
# Edge-set changes warn about the dependents they break.
# --------------------------------------------------------------------------- #
def _shipments() -> SlayerModel:
    return _m("shipments", _int("order_id"),
              Column(name="city", type=DataType.TEXT, sql="orders.addresses.city"),
              joins=[_j("orders", "order_id")])


async def _world(tmp_path, *dependents: SlayerModel, orders: SlayerModel | None = None) -> YAMLStorage:
    storage = await _storage(tmp_path)
    await storage.save_model(_addresses())
    await storage.save_model(orders or _orders())
    for dep in dependents:
        await storage.save_model(dep)
    return storage


async def _save_warnings(door: str, storage: YAMLStorage, model: SlayerModel) -> list[str]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await _save(door, storage, model)
    return [str(w.message) for w in caught if issubclass(w.category, UserWarning)]


def _about(messages: list[str], model: str) -> list[str]:
    return [m for m in messages if f"'{model}'" in m]


def _assert_dependent_warning(msg: str, *, model: str, definition: str, reference: str) -> None:
    assert f"'{model}'" in msg
    assert definition in msg
    assert reference in msg
    assert "orders" in msg and "addresses" in msg
    assert "edge name" in msg.lower()


class TestEdgeSetChangeWarnings:
    async def test_new_parallel_edge_warns_about_a_dependent(self, tmp_path, door) -> None:
        storage = await _world(tmp_path, _shipments())
        msgs = await _save_warnings(door, storage, _orders(joins=_parallel()))
        about = _about(msgs, "shipments")
        assert len(about) == 1, msgs
        _assert_dependent_warning(about[0], model="shipments", definition="city",
                                  reference="orders.addresses.city")
        assert await storage.get_model("orders", data_source=DS) is not None

    async def test_removed_edge_warns_no_longer_resolvable(self, tmp_path, door) -> None:
        storage = await _world(tmp_path, _shipments())
        msgs = await _save_warnings(door, storage, _orders(joins=[]))
        about = _about(msgs, "shipments")
        assert len(about) == 1, msgs
        assert "city" in about[0]
        assert "orders.addresses.city" in about[0]
        assert "no longer resolv" in about[0].lower()

    async def test_dependent_several_hops_away(self, tmp_path, door) -> None:
        parcels = _m("parcels", _int("shipment_id"),
                     Column(name="city", type=DataType.TEXT, sql="shipments.orders.addresses.city"),
                     joins=[_j("shipments", "shipment_id")])
        storage = await _world(tmp_path, _shipments(), parcels)
        msgs = await _save_warnings(door, storage, _orders(joins=_parallel()))
        about = _about(msgs, "parcels")
        assert len(about) == 1, msgs
        _assert_dependent_warning(about[0], model="parcels", definition="city",
                                  reference="shipments.orders.addresses.city")

    async def test_dependent_across_a_reverse_declared_edge(self, tmp_path, door) -> None:
        invoices = _m("invoices", _int("order_id"),
                      Column(name="city", type=DataType.TEXT, sql="orders.addresses.city"))
        to_invoice = _j("invoices", "id", tgt="order_id", cardinality=JoinCardinality.ONE_TO_ONE)
        storage = await _world(tmp_path, invoices,
                               orders=_orders(joins=[_j("addresses", "address_id"), to_invoice]))
        msgs = await _save_warnings(door, storage, _orders(joins=[*_parallel(), to_invoice]))
        about = _about(msgs, "invoices")
        assert len(about) == 1, msgs
        _assert_dependent_warning(about[0], model="invoices", definition="city",
                                  reference="orders.addresses.city")

    async def test_dependent_column_filter(self, tmp_path, door) -> None:
        crates = _m("crates", _int("order_id"), Column(name="weight", type=DataType.DOUBLE),
                    Column(name="paris_weight", type=DataType.DOUBLE, sql="weight",
                           filter="orders.addresses.city = 'Paris'"),
                    joins=[_j("orders", "order_id")])
        storage = await _world(tmp_path, crates)
        msgs = await _save_warnings(door, storage, _orders(joins=_parallel()))
        about = _about(msgs, "crates")
        assert len(about) == 1, msgs
        _assert_dependent_warning(about[0], model="crates", definition="paris_weight",
                                  reference="orders.addresses.city")

    async def test_dependent_model_filter(self, tmp_path, door) -> None:
        audits = _m("audits", _int("order_id"), joins=[_j("orders", "order_id")],
                    filters=["orders.addresses.city = 'Paris'"])
        storage = await _world(tmp_path, audits)
        msgs = await _save_warnings(door, storage, _orders(joins=_parallel()))
        about = _about(msgs, "audits")
        assert len(about) == 1, msgs
        _assert_dependent_warning(about[0], model="audits", definition="orders.addresses.city = 'Paris'",
                                  reference="orders.addresses.city")


class TestUnchangedEdgeSetReExaminesNothing:
    """``audits`` is reachable only through its model filter, so a strict hop onto
    ``orders`` during the save can only come from re-examining it."""

    @staticmethod
    def _audits() -> SlayerModel:
        return _m("audits", _int("order_id"), joins=[_j("orders", "order_id")],
                  filters=["orders.addresses.city = 'Paris'"])

    async def test_column_only_change_neither_warns_nor_re_examines(self, tmp_path, door) -> None:
        storage = await _world(tmp_path, self._audits())
        with observe_traversals() as seen:
            msgs = await _save_warnings(door, storage, _orders(Column(name="note", type=DataType.TEXT)))
        assert _about(msgs, "audits") == []
        assert seen.get("orders") is not True

    async def test_edge_change_re_examines_the_dependent(self, tmp_path, door) -> None:
        storage = await _world(tmp_path, self._audits())
        with observe_traversals() as seen:
            await _save_warnings(door, storage, _orders(joins=_parallel()))
        assert seen.get("orders") is True
