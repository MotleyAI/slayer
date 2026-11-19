"""Save-time ambiguity: a Mode-A definition crossing an ambiguous hop is rejected at
both save doors, and an edge-set change warns about the definitions it breaks."""

from __future__ import annotations

import inspect
import warnings

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import (
    AmbiguousRelationshipPathError,
    CircularRelationshipPathError,
    DerivedFieldAmbiguousError,
    DerivedFieldCircularError,
    DerivedFieldFanningError,
)
from slayer.core.relationship_walker import observe_traversals
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

DS = "ds"
M2O = RelationshipCardinality.MANY_TO_ONE


def _pk() -> DatasetField:
    return DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)


def _int(name: str) -> DatasetField:
    return DatasetField(name=name, datatype=DataType.INTEGER)


def _m(name: str, *cols: DatasetField, joins: list[DatasetRelationship] | None = None,
       filters: list[str] | None = None) -> SlayerDataset:
    return SlayerDataset(name=name, data_source=DS, sql_table=name, fields=[_pk(), *cols],
                       relationships=joins or [], filters=filters or [])


def _j(target: str, src: str, *, tgt: str = "id", name: str | None = None,
       cardinality: RelationshipCardinality | None = M2O) -> DatasetRelationship:
    return DatasetRelationship(to=target, from_columns=[src], to_columns=[tgt], name=name, cardinality=cardinality)


def _addresses() -> SlayerDataset:
    return _m("addresses", DatasetField(name="city", datatype=DataType.STRING))


def _orders(*cols: DatasetField, joins: list[DatasetRelationship] | None = None,
            filters: list[str] | None = None) -> SlayerDataset:
    return _m("orders", _int("address_id"), _int("ship_address_id"),
              DatasetField(name="amount", datatype=DataType.FLOAT), *cols,
              joins=joins if joins is not None else [_j("addresses", "address_id")], filters=filters)


def _parallel(*, named: bool = False) -> list[DatasetRelationship]:
    return [_j("addresses", "address_id", name="billing_address" if named else None),
            _j("addresses", "ship_address_id", name="shipping_address" if named else None)]


async def _storage(tmp_path) -> YAMLStorage:
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite"))
    return storage


async def _save(door: str, storage: YAMLStorage, dataset: SlayerDataset) -> None:
    if door == "engine":
        await SlayerQueryEngine(storage=storage).save_dataset(dataset)
    else:
        await storage.save_dataset(dataset)


async def _both_doors(tmp_path, dataset: SlayerDataset, *peers: SlayerDataset,
                      expected: type[BaseException]) -> tuple[BaseException, BaseException]:
    storage = await _storage(tmp_path)
    for peer in peers:
        await storage.save_dataset(peer)
    with pytest.raises(expected) as storage_ei:
        await _save("storage", storage, dataset)
    with pytest.raises(expected) as engine_ei:
        await _save("engine", storage, dataset)
    assert str(storage_ei.value) == str(engine_ei.value)
    return storage_ei.value, engine_ei.value


def _assert_names_both_edges(exc: AmbiguousRelationshipPathError) -> None:
    assert exc.source_dataset == "orders"
    assert exc.target_dataset == "addresses"
    assert sorted(tuple(map(tuple, c.column_pairs)) for c in exc.candidates) == [
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
        model = _orders(DatasetField(name="city", datatype=DataType.STRING, sql="addresses.city"), joins=_parallel())
        for exc in await _both_doors(tmp_path, model, _addresses(), expected=DerivedFieldAmbiguousError):
            assert isinstance(exc, DerivedFieldAmbiguousError)
            assert isinstance(exc, AmbiguousRelationshipPathError)
            assert exc.field == "city"
            assert exc.dataset == "orders"
            assert exc.reference == "addresses.city"
            _assert_names_both_edges(exc)
            msg = str(exc)
            assert "'city'" in msg
            assert "'orders'" in msg
            assert "'addresses.city'" in msg

    async def test_column_filter_rejected_at_both_doors(self, tmp_path) -> None:
        model = _orders(
            DatasetField(name="paris_amount", datatype=DataType.FLOAT, sql="amount",
                   filter="addresses.city = 'Paris'"),
            joins=_parallel(),
        )
        for exc in await _both_doors(tmp_path, model, _addresses(), expected=DerivedFieldAmbiguousError):
            assert isinstance(exc, DerivedFieldAmbiguousError)
            assert exc.field == "paris_amount"
            assert exc.reference == "addresses.city"
            _assert_names_both_edges(exc)

    async def test_model_filter_rejected_with_the_base_error(self, tmp_path) -> None:
        model = _orders(joins=_parallel(), filters=["addresses.city = 'Paris'"])
        for exc in await _both_doors(tmp_path, model, _addresses(), expected=AmbiguousRelationshipPathError):
            assert not isinstance(exc, DerivedFieldAmbiguousError)
            assert "addresses.city = 'Paris'" in str(exc)
            assert isinstance(exc, AmbiguousRelationshipPathError)
            _assert_names_both_edges(exc)

    async def test_transitive_reference_names_the_inner_column(self, tmp_path) -> None:
        model = _orders(
            DatasetField(name="loud_city", datatype=DataType.STRING, sql="upper(city)"),
            DatasetField(name="city", datatype=DataType.STRING, sql="addresses.city"),
            joins=_parallel(),
        )
        for exc in await _both_doors(tmp_path, model, _addresses(), expected=DerivedFieldAmbiguousError):
            assert isinstance(exc, DerivedFieldAmbiguousError)
            assert exc.field == "city"
            assert exc.reference == "addresses.city"

    async def test_revisit_after_an_ambiguous_hop_is_rejected(self, tmp_path) -> None:
        regions = _m("regions", DatasetField(name="name", datatype=DataType.STRING))
        customers = _m(
            "customers", _int("region_id"), _int("hq_region_id"),
            DatasetField(name="spend", datatype=DataType.FLOAT),
            DatasetField(name="revisit_spend", datatype=DataType.FLOAT, sql="regions.customers.spend"),
            joins=[_j("regions", "region_id", name="r1"), _j("regions", "hq_region_id", name="r2")],
        )
        for exc in await _both_doors(tmp_path, customers, regions, expected=DerivedFieldAmbiguousError):
            assert isinstance(exc, DerivedFieldAmbiguousError)
            assert not isinstance(exc, CircularRelationshipPathError)
            assert exc.field == "revisit_spend"
            assert exc.reference == "regions.customers.spend"
            assert {c.name for c in exc.candidates} == {"r1", "r2"}

    async def test_ambiguity_precedes_a_later_fanning_hop(self, tmp_path) -> None:
        visits = _m("visits", _int("address_id"), DatasetField(name="n", datatype=DataType.INTEGER),
                    joins=[_j("addresses", "address_id")])
        model = _orders(DatasetField(name="visit_n", datatype=DataType.INTEGER, sql="addresses.visits.n"),
                        joins=_parallel())
        storage = await _storage(tmp_path)
        await storage.save_dataset(_addresses())
        await storage.save_dataset(visits)
        for d in ("storage", "engine"):
            with pytest.raises(DerivedFieldAmbiguousError) as ei:
                await _save(d, storage, model)
            assert not isinstance(ei.value, DerivedFieldFanningError)

    async def test_revisit_before_an_ambiguous_hop_stays_circular(self, tmp_path) -> None:
        regions = _m("regions", DatasetField(name="name", datatype=DataType.STRING))
        customers = _m("customers", _int("region_id"), _int("hq_region_id"),
                       joins=[_j("regions", "region_id"), _j("regions", "hq_region_id")])
        model = _m("orders", _int("customer_id"),
                   DatasetField(name="rn", datatype=DataType.STRING, sql="customers.orders.customers.regions.name"),
                   joins=[_j("customers", "customer_id")])
        for exc in await _both_doors(tmp_path, model, regions, customers, expected=DerivedFieldCircularError):
            assert isinstance(exc, DerivedFieldCircularError)
            assert exc.revisited == "orders"

    async def test_named_hop_through_the_same_pair_saves(self, tmp_path, door) -> None:
        storage = await _storage(tmp_path)
        await storage.save_dataset(_addresses())
        model = _orders(DatasetField(name="city", datatype=DataType.STRING, sql="billing_address.city"),
                        joins=_parallel(named=True))
        await _save(door, storage, model)
        assert await storage.get_dataset("orders", data_source=DS) is not None


# --------------------------------------------------------------------------- #
# Edge-set changes warn about the dependents they break.
# --------------------------------------------------------------------------- #
def _shipments() -> SlayerDataset:
    return _m("shipments", _int("order_id"),
              DatasetField(name="city", datatype=DataType.STRING, sql="orders.addresses.city"),
              joins=[_j("orders", "order_id")])


async def _world(tmp_path, *dependents: SlayerDataset, orders: SlayerDataset | None = None) -> YAMLStorage:
    storage = await _storage(tmp_path)
    await storage.save_dataset(_addresses())
    await storage.save_dataset(orders or _orders())
    for dep in dependents:
        await storage.save_dataset(dep)
    return storage


async def _save_warnings(door: str, storage: YAMLStorage, dataset: SlayerDataset) -> list[str]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await _save(door, storage, dataset)
    return [str(w.message) for w in caught if issubclass(w.category, UserWarning)]


def _about(messages: list[str], dataset: str) -> list[str]:
    return [m for m in messages if f"'{dataset}'" in m]


def _assert_dependent_warning(msg: str, *, dataset: str, definition: str, reference: str) -> None:
    assert f"'{dataset}'" in msg
    assert definition in msg
    assert reference in msg
    assert "orders" in msg
    assert "addresses" in msg
    assert "edge name" in msg.lower()


class TestEdgeSetChangeWarnings:
    async def test_new_parallel_edge_warns_about_a_dependent(self, tmp_path, door) -> None:
        storage = await _world(tmp_path, _shipments())
        msgs = await _save_warnings(door, storage, _orders(joins=_parallel()))
        about = _about(msgs, "shipments")
        assert len(about) == 1, msgs
        _assert_dependent_warning(about[0], dataset="shipments", definition="city",
                                  reference="orders.addresses.city")
        assert await storage.get_dataset("orders", data_source=DS) is not None

    async def test_removed_edge_warns_no_longer_resolvable(self, tmp_path, door) -> None:
        storage = await _world(tmp_path, _shipments())
        msgs = await _save_warnings(door, storage, _orders(joins=[]))
        about = _about(msgs, "shipments")
        assert len(about) == 1, msgs
        assert "city" in about[0]
        assert "orders.addresses.city" in about[0]
        assert "no longer resolv" in about[0].lower()

    async def test_warning_names_the_callers_line(self, tmp_path, door, monkeypatch) -> None:
        # The reparse-law wrapper is a frame of its own outside slayer/; production has none.
        monkeypatch.setattr(SlayerQueryEngine, "save_dataset", inspect.unwrap(SlayerQueryEngine.save_dataset))
        storage = await _world(tmp_path, _shipments())
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await _save(door, storage, _orders(joins=[]))
        about = [w for w in caught if issubclass(w.category, UserWarning) and "'shipments'" in str(w.message)]
        assert [w.filename for w in about] == [__file__]

    async def test_dependent_several_hops_away(self, tmp_path, door) -> None:
        parcels = _m("parcels", _int("shipment_id"),
                     DatasetField(name="city", datatype=DataType.STRING, sql="shipments.orders.addresses.city"),
                     joins=[_j("shipments", "shipment_id")])
        storage = await _world(tmp_path, _shipments(), parcels)
        msgs = await _save_warnings(door, storage, _orders(joins=_parallel()))
        about = _about(msgs, "parcels")
        assert len(about) == 1, msgs
        _assert_dependent_warning(about[0], dataset="parcels", definition="city",
                                  reference="shipments.orders.addresses.city")

    async def test_dependent_across_a_reverse_declared_edge(self, tmp_path, door) -> None:
        invoices = _m("invoices", _int("order_id"),
                      DatasetField(name="city", datatype=DataType.STRING, sql="orders.addresses.city"))
        to_invoice = _j("invoices", "id", tgt="order_id", cardinality=RelationshipCardinality.ONE_TO_ONE)
        storage = await _world(tmp_path, invoices,
                               orders=_orders(joins=[_j("addresses", "address_id"), to_invoice]))
        msgs = await _save_warnings(door, storage, _orders(joins=[*_parallel(), to_invoice]))
        about = _about(msgs, "invoices")
        assert len(about) == 1, msgs
        _assert_dependent_warning(about[0], dataset="invoices", definition="city",
                                  reference="orders.addresses.city")

    async def test_dependent_column_filter(self, tmp_path, door) -> None:
        crates = _m("crates", _int("order_id"), DatasetField(name="weight", datatype=DataType.FLOAT),
                    DatasetField(name="paris_weight", datatype=DataType.FLOAT, sql="weight",
                           filter="orders.addresses.city = 'Paris'"),
                    joins=[_j("orders", "order_id")])
        storage = await _world(tmp_path, crates)
        msgs = await _save_warnings(door, storage, _orders(joins=_parallel()))
        about = _about(msgs, "crates")
        assert len(about) == 1, msgs
        _assert_dependent_warning(about[0], dataset="crates", definition="paris_weight",
                                  reference="orders.addresses.city")

    async def test_dependent_model_filter(self, tmp_path, door) -> None:
        audits = _m("audits", _int("order_id"), joins=[_j("orders", "order_id")],
                    filters=["orders.addresses.city = 'Paris'"])
        storage = await _world(tmp_path, audits)
        msgs = await _save_warnings(door, storage, _orders(joins=_parallel()))
        about = _about(msgs, "audits")
        assert len(about) == 1, msgs
        _assert_dependent_warning(about[0], dataset="audits", definition="orders.addresses.city = 'Paris'",
                                  reference="orders.addresses.city")


class TestUnchangedEdgeSetReExaminesNothing:
    """``audits`` is reachable only through its dataset filter, so a strict hop onto
    ``orders`` during the save can only come from re-examining it."""

    @staticmethod
    def _audits() -> SlayerDataset:
        return _m("audits", _int("order_id"), joins=[_j("orders", "order_id")],
                  filters=["orders.addresses.city = 'Paris'"])

    async def test_column_only_change_neither_warns_nor_re_examines(self, tmp_path, door) -> None:
        storage = await _world(tmp_path, self._audits())
        with observe_traversals() as seen:
            msgs = await _save_warnings(door, storage, _orders(DatasetField(name="note", datatype=DataType.STRING)))
        assert _about(msgs, "audits") == []
        assert seen.get("orders") is not True

    async def test_edge_change_re_examines_the_dependent(self, tmp_path, door) -> None:
        storage = await _world(tmp_path, self._audits())
        with observe_traversals() as seen:
            await _save_warnings(door, storage, _orders(joins=_parallel()))
        assert seen.get("orders") is True
