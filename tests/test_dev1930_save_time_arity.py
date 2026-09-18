"""DEV-1930: save-time arity of derived columns / column filters.

A derived ``Column.sql`` / ``Column.filter`` reference that provably crosses a
fanning hop from its declaring model is rejected at save time
(``DerivedColumnFanningError``); an unproven hop warns and defers to the
DEV-1832 query-time backstop; a provably to-one hop is silent; an unloaded /
ambiguous target is skipped (best-effort).

The query-time backstop for an aggregated unproven-hop column is pinned by
``tests/integration/test_integration_duckdb.py::TestDev1709SiblingProtection``
(unchanged by this change) — the remaining ADDED-requirement scenario.
"""
import warnings

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.errors import DerivedColumnFanningError, SlayerError
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine import join_safety
from slayer.engine.join_safety import provably_fans
from slayer.storage.yaml_storage import YAMLStorage

DS = "ds"


def _storage(tmp_path) -> YAMLStorage:
    return YAMLStorage(base_dir=str(tmp_path))


def _line_items(*, extra: tuple[Column, ...] = ()) -> SlayerModel:
    """Target model: PK ``id``, non-unique ``order_id``, base ``qty``."""
    return SlayerModel(
        name="line_items", data_source=DS, sql_table="line_items",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="order_id", sql="order_id", type=DataType.INT),
            Column(name="qty", sql="qty", type=DataType.DOUBLE),
            *extra,
        ],
    )


def _orders(*, li_column: Column, joins: list[ModelJoin]) -> SlayerModel:
    """Declaring model with a per-test derived column and join set."""
    return SlayerModel(
        name="orders", data_source=DS, sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            li_column,
        ],
        joins=joins,
    )


def _fanning_warnings(rec, *, column: str) -> list:
    """Recorded warnings whose message names ``column`` (the arity channel)."""
    return [w for w in rec if column in str(w.message)]


def _hop_warnings(rec, *, column: str, hop: str) -> list:
    """Recorded warnings naming both the column and the hop (the contract)."""
    return [w for w in rec if column in str(w.message) and hop in str(w.message)]


# --------------------------------------------------------------------------- #
# 1. provably_fans predicate (join_safety) — task 1.1.
# --------------------------------------------------------------------------- #


class TestProvablyFans:
    def test_declared_one_to_many_on_non_unique_target_fans(self) -> None:
        target = SlayerModel(
            name="b", data_source=DS, sql_table="b",
            columns=[Column(name="grp", sql="grp", type=DataType.INT)],
        )
        edge = ModelJoin(
            target_model="b", join_pairs=[["b_grp", "grp"]],
            cardinality=JoinCardinality.ONE_TO_MANY,
        )
        assert provably_fans(edge=edge, target_model=target) is True

    def test_declared_many_to_many_fans(self) -> None:
        target = SlayerModel(
            name="b", data_source=DS, sql_table="b",
            columns=[Column(name="grp", sql="grp", type=DataType.INT)],
        )
        edge = ModelJoin(
            target_model="b", join_pairs=[["b_grp", "grp"]],
            cardinality=JoinCardinality.MANY_TO_MANY,
        )
        assert provably_fans(edge=edge, target_model=target) is True

    def test_undeclared_hop_does_not_fan(self) -> None:
        # cardinality None (reverse-PK-covered forward hop): unproven, not fanning.
        target = _line_items()
        edge = ModelJoin(target_model="line_items", join_pairs=[["id", "order_id"]])
        assert provably_fans(edge=edge, target_model=target) is False

    def test_declared_to_many_but_covers_unique_key_does_not_fan(self) -> None:
        # Proof beats a contradictory to-many declaration.
        target = _line_items()
        edge = ModelJoin(
            target_model="line_items", join_pairs=[["li_id", "id"]],
            cardinality=JoinCardinality.ONE_TO_MANY,
        )
        assert provably_fans(edge=edge, target_model=target) is False

    def test_many_to_one_does_not_fan(self) -> None:
        target = _line_items()
        edge = ModelJoin(
            target_model="line_items", join_pairs=[["li_id", "order_id"]],
            cardinality=JoinCardinality.MANY_TO_ONE,
        )
        assert provably_fans(edge=edge, target_model=target) is False

    def test_exported_in_all(self) -> None:
        assert "provably_fans" in join_safety.__all__


# --------------------------------------------------------------------------- #
# 2. Provably fanning -> save-time rejection (ADDED requirement 1).
# --------------------------------------------------------------------------- #


async def test_sql_across_declared_one_to_many_rejected(tmp_path) -> None:
    storage = _storage(tmp_path)
    await storage.save_model(_line_items())
    orders = _orders(
        li_column=Column(name="li_qty", sql="line_items.qty", type=DataType.DOUBLE),
        joins=[ModelJoin(
            target_model="line_items", join_pairs=[["id", "order_id"]],
            cardinality=JoinCardinality.ONE_TO_MANY,
        )],
    )
    with pytest.raises(DerivedColumnFanningError) as ei:
        await storage.save_model(orders)
    exc = ei.value
    assert isinstance(exc, ValueError) and isinstance(exc, SlayerError)
    assert exc.column == "li_qty"
    assert exc.model == "orders"
    assert exc.hop == "line_items"
    assert exc.kind == "sql"
    msg = str(exc)
    assert "li_qty" in msg and "line_items" in msg
    assert "line_items.qty" in msg  # the cross-model aggregate remedy spelling
    # The three remedy components the spec requires: aggregate, filter, or
    # declare a to-one cardinality / covering unique key.
    assert "aggregat" in msg.lower()
    assert "filter" in msg.lower()
    assert any(w in msg.lower() for w in ("cardinality", "unique"))


async def test_filter_across_declared_one_to_many_rejected(tmp_path) -> None:
    storage = _storage(tmp_path)
    await storage.save_model(_line_items())
    # Value SQL is host-local; only the filter crosses the fanning hop.
    orders = _orders(
        li_column=Column(
            name="big_item", sql="amount", type=DataType.DOUBLE,
            filter="line_items.qty >= 2",
        ),
        joins=[ModelJoin(
            target_model="line_items", join_pairs=[["id", "order_id"]],
            cardinality=JoinCardinality.ONE_TO_MANY,
        )],
    )
    with pytest.raises(DerivedColumnFanningError) as ei:
        await storage.save_model(orders)
    exc = ei.value
    assert exc.column == "big_item"
    assert exc.hop == "line_items"
    assert exc.kind == "filter"
    msg = str(exc)
    assert "big_item" in msg and "line_items" in msg
    assert "aggregat" in msg.lower()


async def test_host_prefixed_reference_rejected(tmp_path) -> None:
    storage = _storage(tmp_path)
    await storage.save_model(_line_items())
    orders = _orders(
        li_column=Column(
            name="li_qty", sql="orders.line_items.qty", type=DataType.DOUBLE,
        ),
        joins=[ModelJoin(
            target_model="line_items", join_pairs=[["id", "order_id"]],
            cardinality=JoinCardinality.ONE_TO_MANY,
        )],
    )
    with pytest.raises(DerivedColumnFanningError) as ei:
        await storage.save_model(orders)
    assert ei.value.hop == "line_items"


async def test_join_declared_on_peer_side_rejected(tmp_path) -> None:
    """The fanning edge is declared on the target as ``many_to_one``; from
    ``orders`` its reverse orientation is ``one_to_many`` (DEV-1853)."""
    storage = _storage(tmp_path)
    line_items = SlayerModel(
        name="line_items", data_source=DS, sql_table="line_items",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="order_id", sql="order_id", type=DataType.INT),
            Column(name="qty", sql="qty", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(
            target_model="orders", join_pairs=[["order_id", "id"]],
            cardinality=JoinCardinality.MANY_TO_ONE,
        )],
    )
    await storage.save_model(line_items)
    orders = _orders(
        li_column=Column(name="li_qty", sql="line_items.qty", type=DataType.DOUBLE),
        joins=[],  # no join declared on orders — it resolves over the reverse edge
    )
    with pytest.raises(DerivedColumnFanningError) as ei:
        await storage.save_model(orders)
    assert ei.value.hop == "line_items"


async def test_multihop_fanning_then_unproven_rejected(tmp_path) -> None:
    storage = _storage(tmp_path)
    parts = SlayerModel(
        name="parts", data_source=DS, sql_table="parts",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="code", sql="code", type=DataType.TEXT),  # non-unique
            Column(name="pval", sql="pval", type=DataType.DOUBLE),
        ],
    )
    await storage.save_model(parts)
    # orders -> line_items fans; line_items -> parts is undeclared on a
    # non-unique target column (unproven). The fanning hop is first.
    line_items = _line_items(
        extra=(Column(name="part_code", sql="part_code", type=DataType.TEXT),),
    )
    line_items = line_items.model_copy(update={
        "joins": [ModelJoin(
            target_model="parts", join_pairs=[["part_code", "code"]],
        )],
    })
    await storage.save_model(line_items)
    orders = _orders(
        li_column=Column(name="pv", sql="line_items.parts.pval", type=DataType.DOUBLE),
        joins=[ModelJoin(
            target_model="line_items", join_pairs=[["id", "order_id"]],
            cardinality=JoinCardinality.ONE_TO_MANY,
        )],
    )
    with pytest.raises(DerivedColumnFanningError) as ei:
        await storage.save_model(orders)
    assert ei.value.hop == "line_items"  # first fanning hop on the path


async def test_multihop_unproven_then_fanning_rejected(tmp_path) -> None:
    storage = _storage(tmp_path)
    addresses = SlayerModel(
        name="addresses", data_source=DS, sql_table="addresses",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
            Column(name="street", sql="street", type=DataType.TEXT),
        ],
    )
    await storage.save_model(addresses)
    # customers -> addresses fans; orders -> customers is unproven (region=region).
    customers = SlayerModel(
        name="customers", data_source=DS, sql_table="customers",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
        ],
        joins=[ModelJoin(
            target_model="addresses", join_pairs=[["id", "customer_id"]],
            cardinality=JoinCardinality.ONE_TO_MANY,
        )],
    )
    await storage.save_model(customers)
    orders = SlayerModel(
        name="orders", data_source=DS, sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="astreet", sql="customers.addresses.street", type=DataType.TEXT),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["region", "region"]])],
    )
    with pytest.raises(DerivedColumnFanningError) as ei:
        await storage.save_model(orders)
    assert ei.value.hop == "addresses"  # first fanning hop on the path


# --------------------------------------------------------------------------- #
# 3. Provably to-one -> accepted silently (ADDED requirement 1).
# --------------------------------------------------------------------------- #


async def test_declared_many_to_one_accepted_silently(tmp_path) -> None:
    storage = _storage(tmp_path)
    customers = SlayerModel(
        name="customers", data_source=DS, sql_table="customers",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
        ],
    )
    await storage.save_model(customers)
    orders = SlayerModel(
        name="orders", data_source=DS, sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="cust_id", sql="cust_id", type=DataType.INT),
            Column(name="cust_region", sql="customers.region", type=DataType.TEXT),
        ],
        joins=[ModelJoin(
            target_model="customers", join_pairs=[["cust_id", "id"]],
            cardinality=JoinCardinality.MANY_TO_ONE,
        )],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_model(orders)
    assert _fanning_warnings(rec, column="cust_region") == []
    assert await storage.get_model("orders", data_source=DS) is not None


async def test_proof_beats_contradictory_to_many_declaration(tmp_path) -> None:
    """Declared ``one_to_many`` but the target-side column covers the PK →
    provably to-one → accepted silently."""
    storage = _storage(tmp_path)
    await storage.save_model(_line_items())
    orders = SlayerModel(
        name="orders", data_source=DS, sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="li_id", sql="li_id", type=DataType.INT),
            Column(name="item_qty", sql="line_items.qty", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(
            target_model="line_items", join_pairs=[["li_id", "id"]],
            cardinality=JoinCardinality.ONE_TO_MANY,
        )],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_model(orders)  # must not raise
    assert _fanning_warnings(rec, column="item_qty") == []


# --------------------------------------------------------------------------- #
# 4. Unproven -> warn + save (ADDED requirement 2).
# --------------------------------------------------------------------------- #


async def test_reverse_pk_undeclared_hop_warns_and_saves(tmp_path) -> None:
    """The DEV-1709 shape: undeclared ``orders -> line_items`` whose reverse
    orientation covers ``orders``'s PK. Forward is unproven → warn."""
    storage = _storage(tmp_path)
    await storage.save_model(_line_items())
    orders = _orders(
        li_column=Column(name="li_qty", sql="line_items.qty", type=DataType.DOUBLE),
        joins=[ModelJoin(target_model="line_items", join_pairs=[["id", "order_id"]])],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_model(orders)
    assert _hop_warnings(rec, column="li_qty", hop="line_items")
    assert await storage.get_model("orders", data_source=DS) is not None


async def test_fully_undeclared_hop_warns_and_saves(tmp_path) -> None:
    storage = _storage(tmp_path)
    widgets = SlayerModel(
        name="widgets", data_source=DS, sql_table="widgets",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="w", sql="w", type=DataType.DOUBLE),
        ],
    )
    await storage.save_model(widgets)
    orders = SlayerModel(
        name="orders", data_source=DS, sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="wsum", sql="widgets.w", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="widgets", join_pairs=[["region", "region"]])],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_model(orders)
    assert _hop_warnings(rec, column="wsum", hop="widgets")
    assert await storage.get_model("orders", data_source=DS) is not None


async def test_filter_across_unproven_hop_warns_and_saves(tmp_path) -> None:
    """A ``Column.filter`` across an unproven hop warns like an ``sql`` ref."""
    storage = _storage(tmp_path)
    await storage.save_model(_line_items())
    orders = _orders(
        li_column=Column(
            name="big_item", sql="amount", type=DataType.DOUBLE,
            filter="line_items.qty >= 2",
        ),
        joins=[ModelJoin(target_model="line_items", join_pairs=[["id", "order_id"]])],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_model(orders)
    assert _hop_warnings(rec, column="big_item", hop="line_items")
    assert await storage.get_model("orders", data_source=DS) is not None


async def test_unproven_warning_deduplicated_per_hop(tmp_path) -> None:
    """One column referencing the same unproven hop twice warns exactly once."""
    storage = _storage(tmp_path)
    await storage.save_model(_line_items())
    orders = _orders(
        li_column=Column(
            name="qtysum", sql="line_items.qty + line_items.order_id",
            type=DataType.DOUBLE,
        ),
        joins=[ModelJoin(target_model="line_items", join_pairs=[["id", "order_id"]])],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_model(orders)
    assert len(_hop_warnings(rec, column="qtysum", hop="line_items")) == 1


# --------------------------------------------------------------------------- #
# 5. Skipped (unloaded / ambiguous) — best-effort (ADDED requirement 2).
# --------------------------------------------------------------------------- #


async def test_unloaded_target_skipped(tmp_path) -> None:
    """Same fanning-declared orders model, but ``line_items`` is not saved:
    the hop's target is unloaded → skipped (no error, no warning)."""
    storage = _storage(tmp_path)
    orders = _orders(
        li_column=Column(name="li_qty", sql="line_items.qty", type=DataType.DOUBLE),
        joins=[ModelJoin(
            target_model="line_items", join_pairs=[["id", "order_id"]],
            cardinality=JoinCardinality.ONE_TO_MANY,
        )],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_model(orders)  # must not raise
    assert _fanning_warnings(rec, column="li_qty") == []
    assert await storage.get_model("orders", data_source=DS) is not None


async def test_unresolvable_reference_skipped(tmp_path) -> None:
    """A qualifier that names no join hop from the host resolves to nothing →
    skipped at save time (no error, no warning), like the cycle walk."""
    storage = _storage(tmp_path)
    orders = SlayerModel(
        name="orders", data_source=DS, sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="ghost", sql="nowhere.value", type=DataType.DOUBLE),
        ],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_model(orders)  # must not raise
    assert _fanning_warnings(rec, column="ghost") == []
    assert await storage.get_model("orders", data_source=DS) is not None


async def test_ambiguous_parallel_edge_skipped(tmp_path) -> None:
    """A bare model-name token spanning two parallel named edges is ambiguous
    → skipped, even when one edge is fanning-declared."""
    storage = _storage(tmp_path)
    widgets = SlayerModel(
        name="widgets", data_source=DS, sql_table="widgets",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="w", sql="w", type=DataType.DOUBLE),
        ],
    )
    await storage.save_model(widgets)
    orders = SlayerModel(
        name="orders", data_source=DS, sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="w1", sql="w1", type=DataType.INT),
            Column(name="w2", sql="w2", type=DataType.INT),
            Column(name="wsum", sql="widgets.w", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(
                target_model="widgets", join_pairs=[["w1", "id"]], name="e1",
                cardinality=JoinCardinality.ONE_TO_MANY,
            ),
            ModelJoin(target_model="widgets", join_pairs=[["w2", "id"]], name="e2"),
        ],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_model(orders)  # ambiguous hop → skipped, no raise
    assert _fanning_warnings(rec, column="wsum") == []


async def test_peer_save_does_not_reclassify_stored_column(tmp_path) -> None:
    """Best-effort scope: arity is classified for the model being saved only.
    A stored fanning column (persisted via ``_validate=False``) is not
    re-rejected or re-warned when an unrelated peer is later saved."""
    storage = _storage(tmp_path)
    await storage.save_model(_line_items())
    fanning = _orders(
        li_column=Column(name="li_qty", sql="line_items.qty", type=DataType.DOUBLE),
        joins=[ModelJoin(
            target_model="line_items", join_pairs=[["id", "order_id"]],
            cardinality=JoinCardinality.ONE_TO_MANY,
        )],
    )
    await storage.save_model(fanning, _validate=False)  # persist the ill-formed model
    peer = SlayerModel(
        name="widgets", data_source=DS, sql_table="widgets",
        columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
    )
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        await storage.save_model(peer)  # must not raise about orders.li_qty
    assert _fanning_warnings(rec, column="li_qty") == []


# --------------------------------------------------------------------------- #
# 6. Entry-point rename (task 3.3).
# --------------------------------------------------------------------------- #


def test_validate_derived_columns_entry_point_exists() -> None:
    from slayer.engine.column_dependency import validate_derived_columns

    assert callable(validate_derived_columns)
