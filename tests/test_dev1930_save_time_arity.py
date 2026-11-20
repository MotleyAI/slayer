"""Save-time arity of derived fields / field filters.

A derived ``DatasetField.sql`` / ``DatasetField.filter`` reference that provably crosses a
fanning hop from its declaring dataset is rejected at save time
(``DerivedFieldFanningError``); an unproven hop warns and defers to the
query-time backstop; a provably to-one hop is silent; an unloaded
target is skipped (best-effort); an ambiguous one is rejected.

The query-time backstop for an aggregated unproven-hop field is pinned by
``tests/integration/test_integration_duckdb.py::TestDev1709SiblingProtection``
(unchanged by this change) — the remaining ADDED-requirement scenario.
"""
import warnings

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import DerivedFieldAmbiguousError, DerivedFieldFanningError, SlayerError
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.cardinality import relationship_safety
from slayer.engine.field_dependency import validate_derived_fields
from slayer.engine.cardinality.relationship_safety import provably_fans
from slayer.storage.yaml_storage import YAMLStorage

DS = "ds"


def _storage(tmp_path) -> YAMLStorage:
    return YAMLStorage(base_dir=str(tmp_path))


def _line_items(*, extra: tuple[DatasetField, ...] = ()) -> SlayerDataset:
    """Target dataset: PK ``id``, non-unique ``order_id``, base ``qty``."""
    return SlayerDataset(
        name="line_items", data_source=DS, sql_table="line_items",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="order_id", sql="order_id", datatype=DataType.INTEGER),
            DatasetField(name="qty", sql="qty", datatype=DataType.FLOAT),
            *extra,
        ],
    )


def _orders(*, li_field: DatasetField, relationships: list[DatasetRelationship]) -> SlayerDataset:
    """Declaring dataset with a per-test derived field and relationship set."""
    return SlayerDataset(
        name="orders", data_source=DS, sql_table="orders",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", sql="status", datatype=DataType.STRING),
            DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            li_field,
        ],
        relationships=relationships,
    )


def _fanning_warnings(rec, *, field: str) -> list:
    """Recorded warnings whose message names ``field`` (the arity channel)."""
    return [w for w in rec if field in str(w.message)]


def _hop_warnings(rec, *, field: str, hop: str) -> list:
    """Recorded warnings naming both the field and the hop (the contract)."""
    return [w for w in rec if field in str(w.message) and hop in str(w.message)]


# --------------------------------------------------------------------------- #
# 1. provably_fans predicate (join_safety) — task 1.1.
# --------------------------------------------------------------------------- #


class TestProvablyFans:
    def test_declared_one_to_many_on_non_unique_target_fans(self) -> None:
        target = SlayerDataset(
            name="b", data_source=DS, sql_table="b",
            fields=[DatasetField(name="grp", sql="grp", datatype=DataType.INTEGER)],
        )
        edge = DatasetRelationship(
            to="b", from_columns=["b_grp"], to_columns=["grp"],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        )
        assert provably_fans(edge=edge, target_dataset=target) is True

    def test_declared_many_to_many_fans(self) -> None:
        target = SlayerDataset(
            name="b", data_source=DS, sql_table="b",
            fields=[DatasetField(name="grp", sql="grp", datatype=DataType.INTEGER)],
        )
        edge = DatasetRelationship(
            to="b", from_columns=["b_grp"], to_columns=["grp"],
            cardinality=RelationshipCardinality.MANY_TO_MANY,
        )
        assert provably_fans(edge=edge, target_dataset=target) is True

    def test_undeclared_hop_does_not_fan(self) -> None:
        # cardinality None (reverse-PK-covered forward hop): unproven, not fanning.
        target = _line_items()
        edge = DatasetRelationship(to="line_items", from_columns=["id"], to_columns=["order_id"])
        assert provably_fans(edge=edge, target_dataset=target) is False

    def test_declared_to_many_but_covers_unique_key_does_not_fan(self) -> None:
        # Proof beats a contradictory to-many declaration.
        target = _line_items()
        edge = DatasetRelationship(
            to="line_items", from_columns=["li_id"], to_columns=["id"],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        )
        assert provably_fans(edge=edge, target_dataset=target) is False

    def test_many_to_one_does_not_fan(self) -> None:
        target = _line_items()
        edge = DatasetRelationship(
            to="line_items", from_columns=["li_id"], to_columns=["order_id"],
            cardinality=RelationshipCardinality.MANY_TO_ONE,
        )
        assert provably_fans(edge=edge, target_dataset=target) is False

    def test_exported_in_all(self) -> None:
        assert "provably_fans" in relationship_safety.__all__


# --------------------------------------------------------------------------- #
# 2. Provably fanning -> save-time rejection (ADDED requirement 1).
# --------------------------------------------------------------------------- #


async def test_sql_across_declared_one_to_many_rejected(tmp_path) -> None:
    storage = _storage(tmp_path)
    await storage.save_dataset(_line_items())
    orders = _orders(
        li_field=DatasetField(name="li_qty", sql="line_items.qty", datatype=DataType.FLOAT),
        relationships=[DatasetRelationship(
            to="line_items", from_columns=["id"], to_columns=["order_id"],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        )],
    )
    with pytest.raises(DerivedFieldFanningError) as ei:
        await storage.save_dataset(orders)
    exc = ei.value
    assert isinstance(exc, ValueError)
    assert isinstance(exc, SlayerError)
    assert exc.field == "li_qty"
    assert exc.dataset == "orders"
    assert exc.hop == "line_items"
    assert exc.reference == "line_items.qty"
    msg = str(exc)
    assert "li_qty" in msg
    assert "line_items" in msg
    assert "line_items.qty" in msg  # the cross-dataset aggregate remedy spelling
    # The three remedy components the spec requires: aggregate, filter, or
    # declare a to-one cardinality / covering unique key.
    assert "aggregat" in msg.lower()
    assert "filter" in msg.lower()
    assert any(w in msg.lower() for w in ("cardinality", "unique"))


async def test_filter_across_declared_one_to_many_rejected(tmp_path) -> None:
    storage = _storage(tmp_path)
    await storage.save_dataset(_line_items())
    # Value SQL is host-local; only the filter crosses the fanning hop.
    orders = _orders(
        li_field=DatasetField(
            name="big_item", sql="amount", datatype=DataType.FLOAT,
            filter="line_items.qty >= 2",
        ),
        relationships=[DatasetRelationship(
            to="line_items", from_columns=["id"], to_columns=["order_id"],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        )],
    )
    with pytest.raises(DerivedFieldFanningError) as ei:
        await storage.save_dataset(orders)
    exc = ei.value
    assert exc.field == "big_item"
    assert exc.hop == "line_items"
    assert exc.reference == "line_items.qty"
    msg = str(exc)
    assert "big_item" in msg
    assert "line_items" in msg
    assert "aggregat" in msg.lower()


async def test_host_prefixed_reference_rejected(tmp_path) -> None:
    storage = _storage(tmp_path)
    await storage.save_dataset(_line_items())
    orders = _orders(
        li_field=DatasetField(
            name="li_qty", sql="orders.line_items.qty", datatype=DataType.FLOAT,
        ),
        relationships=[DatasetRelationship(
            to="line_items", from_columns=["id"], to_columns=["order_id"],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        )],
    )
    with pytest.raises(DerivedFieldFanningError) as ei:
        await storage.save_dataset(orders)
    assert ei.value.hop == "line_items"
    assert ei.value.reference == "orders.line_items.qty"


async def test_join_declared_on_peer_side_rejected(tmp_path) -> None:
    """The fanning edge is declared on the target as ``many_to_one``; from
    ``orders`` its reverse orientation is ``one_to_many``."""
    storage = _storage(tmp_path)
    line_items = SlayerDataset(
        name="line_items", data_source=DS, sql_table="line_items",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="order_id", sql="order_id", datatype=DataType.INTEGER),
            DatasetField(name="qty", sql="qty", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(
            to="orders", from_columns=["order_id"], to_columns=["id"],
            cardinality=RelationshipCardinality.MANY_TO_ONE,
        )],
    )
    await storage.save_dataset(line_items)
    orders = _orders(
        li_field=DatasetField(name="li_qty", sql="line_items.qty", datatype=DataType.FLOAT),
        relationships=[],  # no relationship declared on orders — it resolves over the reverse edge
    )
    with pytest.raises(DerivedFieldFanningError) as ei:
        await storage.save_dataset(orders)
    assert ei.value.hop == "line_items"


async def test_multihop_fanning_then_unproven_rejected(tmp_path) -> None:
    storage = _storage(tmp_path)
    parts = SlayerDataset(
        name="parts", data_source=DS, sql_table="parts",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="code", sql="code", datatype=DataType.STRING),  # non-unique
            DatasetField(name="pval", sql="pval", datatype=DataType.FLOAT),
        ],
    )
    await storage.save_dataset(parts)
    # orders -> line_items fans; line_items -> parts is undeclared on a
    # non-unique target field (unproven). The fanning hop is first.
    line_items = _line_items(
        extra=(DatasetField(name="part_code", sql="part_code", datatype=DataType.STRING),),
    )
    line_items = line_items.model_copy(update={
        "relationships": [DatasetRelationship(
            to="parts", from_columns=["part_code"], to_columns=["code"],
        )],
    })
    await storage.save_dataset(line_items)
    orders = _orders(
        li_field=DatasetField(name="pv", sql="line_items.parts.pval", datatype=DataType.FLOAT),
        relationships=[DatasetRelationship(
            to="line_items", from_columns=["id"], to_columns=["order_id"],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        )],
    )
    with pytest.raises(DerivedFieldFanningError) as ei:
        await storage.save_dataset(orders)
    assert ei.value.hop == "line_items"  # first fanning hop on the path


async def test_multihop_unproven_then_fanning_rejected(tmp_path) -> None:
    storage = _storage(tmp_path)
    addresses = SlayerDataset(
        name="addresses", data_source=DS, sql_table="addresses",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="street", sql="street", datatype=DataType.STRING),
        ],
    )
    await storage.save_dataset(addresses)
    # customers -> addresses fans; orders -> customers is unproven (region=region).
    customers = SlayerDataset(
        name="customers", data_source=DS, sql_table="customers",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region", sql="region", datatype=DataType.STRING),
        ],
        relationships=[DatasetRelationship(
            to="addresses", from_columns=["id"], to_columns=["customer_id"],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        )],
    )
    await storage.save_dataset(customers)
    orders = SlayerDataset(
        name="orders", data_source=DS, sql_table="orders",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region", sql="region", datatype=DataType.STRING),
            DatasetField(name="astreet", sql="customers.addresses.street", datatype=DataType.STRING),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["region"], to_columns=["region"])],
    )
    with pytest.raises(DerivedFieldFanningError) as ei:
        await storage.save_dataset(orders)
    assert ei.value.hop == "addresses"  # first fanning hop on the path


# --------------------------------------------------------------------------- #
# 3. Provably to-one -> accepted silently (ADDED requirement 1).
# --------------------------------------------------------------------------- #


async def test_declared_many_to_one_accepted_silently(tmp_path) -> None:
    storage = _storage(tmp_path)
    customers = SlayerDataset(
        name="customers", data_source=DS, sql_table="customers",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region", sql="region", datatype=DataType.STRING),
        ],
    )
    await storage.save_dataset(customers)
    orders = SlayerDataset(
        name="orders", data_source=DS, sql_table="orders",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="cust_id", sql="cust_id", datatype=DataType.INTEGER),
            DatasetField(name="cust_region", sql="customers.region", datatype=DataType.STRING),
        ],
        relationships=[DatasetRelationship(
            to="customers", from_columns=["cust_id"], to_columns=["id"],
            cardinality=RelationshipCardinality.MANY_TO_ONE,
        )],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_dataset(orders)
    assert _fanning_warnings(rec, field="cust_region") == []
    assert await storage.get_dataset("orders", data_source=DS) is not None


async def test_proof_beats_contradictory_to_many_declaration(tmp_path) -> None:
    """Declared ``one_to_many`` but the target-side field covers the PK →
    provably to-one → accepted silently."""
    storage = _storage(tmp_path)
    await storage.save_dataset(_line_items())
    orders = SlayerDataset(
        name="orders", data_source=DS, sql_table="orders",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="li_id", sql="li_id", datatype=DataType.INTEGER),
            DatasetField(name="item_qty", sql="line_items.qty", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(
            to="line_items", from_columns=["li_id"], to_columns=["id"],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        )],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_dataset(orders)  # must not raise
    assert _fanning_warnings(rec, field="item_qty") == []


# --------------------------------------------------------------------------- #
# 4. Unproven -> warn + save (ADDED requirement 2).
# --------------------------------------------------------------------------- #


async def test_reverse_pk_undeclared_hop_warns_and_saves(tmp_path) -> None:
    """Undeclared ``orders -> line_items`` whose reverse
    orientation covers ``orders``'s PK. Forward is unproven → warn."""
    storage = _storage(tmp_path)
    await storage.save_dataset(_line_items())
    orders = _orders(
        li_field=DatasetField(name="li_qty", sql="line_items.qty", datatype=DataType.FLOAT),
        relationships=[DatasetRelationship(to="line_items", from_columns=["id"], to_columns=["order_id"])],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_dataset(orders)
    assert _hop_warnings(rec, field="li_qty", hop="line_items")
    assert await storage.get_dataset("orders", data_source=DS) is not None


async def test_fully_undeclared_hop_warns_and_saves(tmp_path) -> None:
    storage = _storage(tmp_path)
    widgets = SlayerDataset(
        name="widgets", data_source=DS, sql_table="widgets",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region", sql="region", datatype=DataType.STRING),
            DatasetField(name="w", sql="w", datatype=DataType.FLOAT),
        ],
    )
    await storage.save_dataset(widgets)
    orders = SlayerDataset(
        name="orders", data_source=DS, sql_table="orders",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region", sql="region", datatype=DataType.STRING),
            DatasetField(name="wsum", sql="widgets.w", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="widgets", from_columns=["region"], to_columns=["region"])],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_dataset(orders)
    assert _hop_warnings(rec, field="wsum", hop="widgets")
    assert await storage.get_dataset("orders", data_source=DS) is not None


async def test_filter_across_unproven_hop_warns_and_saves(tmp_path) -> None:
    """A ``DatasetField.filter`` across an unproven hop warns like an ``sql`` ref."""
    storage = _storage(tmp_path)
    await storage.save_dataset(_line_items())
    orders = _orders(
        li_field=DatasetField(
            name="big_item", sql="amount", datatype=DataType.FLOAT,
            filter="line_items.qty >= 2",
        ),
        relationships=[DatasetRelationship(to="line_items", from_columns=["id"], to_columns=["order_id"])],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_dataset(orders)
    assert _hop_warnings(rec, field="big_item", hop="line_items")
    assert await storage.get_dataset("orders", data_source=DS) is not None


async def test_unproven_warning_deduplicated_per_hop(tmp_path) -> None:
    """One field referencing the same unproven hop twice warns exactly once."""
    storage = _storage(tmp_path)
    await storage.save_dataset(_line_items())
    orders = _orders(
        li_field=DatasetField(
            name="qtysum", sql="line_items.qty + line_items.order_id",
            datatype=DataType.FLOAT,
        ),
        relationships=[DatasetRelationship(to="line_items", from_columns=["id"], to_columns=["order_id"])],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_dataset(orders)
    assert len(_hop_warnings(rec, field="qtysum", hop="line_items")) == 1


# --------------------------------------------------------------------------- #
# 5. Skipped (unloaded / ambiguous) — best-effort (ADDED requirement 2).
# --------------------------------------------------------------------------- #


async def test_unloaded_target_skipped(tmp_path) -> None:
    """Same fanning-declared orders dataset, but ``line_items`` is not saved:
    the hop's target is unloaded → skipped (no error, no warning)."""
    storage = _storage(tmp_path)
    orders = _orders(
        li_field=DatasetField(name="li_qty", sql="line_items.qty", datatype=DataType.FLOAT),
        relationships=[DatasetRelationship(
            to="line_items", from_columns=["id"], to_columns=["order_id"],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        )],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_dataset(orders)  # must not raise
    assert _fanning_warnings(rec, field="li_qty") == []
    assert await storage.get_dataset("orders", data_source=DS) is not None


async def test_unresolvable_reference_skipped(tmp_path) -> None:
    """A qualifier that names no relationship hop from the host resolves to nothing →
    skipped at save time (no error, no warning), like the cycle walk."""
    storage = _storage(tmp_path)
    orders = SlayerDataset(
        name="orders", data_source=DS, sql_table="orders",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="ghost", sql="nowhere.value", datatype=DataType.FLOAT),
        ],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_dataset(orders)  # must not raise
    assert _fanning_warnings(rec, field="ghost") == []
    assert await storage.get_dataset("orders", data_source=DS) is not None


async def test_ambiguous_parallel_edge_rejected(tmp_path) -> None:
    """A bare dataset-name token spanning two parallel named edges is ambiguous
    → rejected as ambiguous, never as fanning, even when one edge is fanning-declared."""
    storage = _storage(tmp_path)
    widgets = SlayerDataset(
        name="widgets", data_source=DS, sql_table="widgets",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="w", sql="w", datatype=DataType.FLOAT),
        ],
    )
    await storage.save_dataset(widgets)
    orders = SlayerDataset(
        name="orders", data_source=DS, sql_table="orders",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="w1", sql="w1", datatype=DataType.INTEGER),
            DatasetField(name="w2", sql="w2", datatype=DataType.INTEGER),
            DatasetField(name="wsum", sql="widgets.w", datatype=DataType.FLOAT),
        ],
        relationships=[
            DatasetRelationship(
                to="widgets", from_columns=["w1"], to_columns=["id"], name="e1",
                cardinality=RelationshipCardinality.ONE_TO_MANY,
            ),
            DatasetRelationship(to="widgets", from_columns=["w2"], to_columns=["id"], name="e2"),
        ],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        with pytest.raises(DerivedFieldAmbiguousError) as ei:
            await storage.save_dataset(orders)
    assert not isinstance(ei.value, DerivedFieldFanningError)
    assert ei.value.field == "wsum"
    assert _fanning_warnings(rec, field="wsum") == []
    assert await storage.get_dataset("orders", data_source=DS) is None


async def test_peer_save_does_not_reclassify_stored_column(tmp_path) -> None:
    """Best-effort scope: arity is classified for the dataset being saved only.
    A stored fanning field (persisted via ``_validate=False``) is not
    re-rejected or re-warned when an unrelated peer is later saved."""
    storage = _storage(tmp_path)
    await storage.save_dataset(_line_items())
    fanning = _orders(
        li_field=DatasetField(name="li_qty", sql="line_items.qty", datatype=DataType.FLOAT),
        relationships=[DatasetRelationship(
            to="line_items", from_columns=["id"], to_columns=["order_id"],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        )],
    )
    await storage.save_dataset(fanning, _validate=False)  # persist the ill-formed dataset
    peer = SlayerDataset(
        name="widgets", data_source=DS, sql_table="widgets",
        fields=[DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True)],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_dataset(peer)  # must not raise about orders.li_qty
    assert _fanning_warnings(rec, field="li_qty") == []


# --------------------------------------------------------------------------- #
# 6. Entry-point rename (task 3.3).
# --------------------------------------------------------------------------- #


def test_validate_derived_columns_entry_point_exists() -> None:
    assert callable(validate_derived_fields)
