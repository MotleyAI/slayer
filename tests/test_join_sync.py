"""Tests for the JoinSyncStorage wrapper (inner-join symmetry)."""

import tempfile

import pytest

from slayer.core.enums import DataType, JoinType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.storage.join_sync import JoinSyncStorage, _mirror_inner_joins
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _model(name: str, *, joins: list[ModelJoin] | None = None) -> SlayerModel:
    """Minimal model with one PK column and one numeric column."""
    return SlayerModel(
        name=name,
        sql_table=name,
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.NUMBER),
        ],
        joins=joins or [],
    )


def _inner_join(target: str, pairs: list[list[str]] | None = None) -> ModelJoin:
    return ModelJoin(
        target_model=target,
        join_pairs=pairs or [["fk_id", "id"]],
        join_type=JoinType.INNER,
    )


def _left_join(target: str, pairs: list[list[str]] | None = None) -> ModelJoin:
    return ModelJoin(
        target_model=target,
        join_pairs=pairs or [["fk_id", "id"]],
        join_type=JoinType.LEFT,
    )


@pytest.fixture
def raw_storage():
    """Bare YAMLStorage (no wrapper) for setting up test state."""
    with tempfile.TemporaryDirectory() as d:
        yield YAMLStorage(base_dir=d)


@pytest.fixture
def synced_storage(raw_storage):
    """JoinSyncStorage wrapping the raw_storage fixture."""
    return JoinSyncStorage(inner=raw_storage)


# ---------------------------------------------------------------------------
# _mirror_inner_joins (standalone helper)
# ---------------------------------------------------------------------------


class TestMirrorInnerJoins:
    async def test_basic_mirror(self, raw_storage) -> None:
        a = _model("a", joins=[_inner_join("b")])
        b = _model("b")
        await raw_storage.save_model(a)
        await raw_storage.save_model(b)

        await _mirror_inner_joins(a, raw_storage)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 1
        assert b_reloaded.joins[0].target_model == "a"
        assert b_reloaded.joins[0].join_type == JoinType.INNER
        assert b_reloaded.joins[0].join_pairs == [["id", "fk_id"]]

    async def test_self_join_not_mirrored(self, raw_storage) -> None:
        model = SlayerModel(
            name="employees",
            sql_table="employees",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="manager_id", sql="manager_id", type=DataType.NUMBER),
            ],
            joins=[ModelJoin(target_model="employees", join_pairs=[["manager_id", "id"]], join_type=JoinType.INNER)],
        )
        await raw_storage.save_model(model)
        await _mirror_inner_joins(model, raw_storage)

        reloaded = await raw_storage.get_model("employees")
        assert len(reloaded.joins) == 1

    async def test_left_join_not_mirrored(self, raw_storage) -> None:
        a = _model("a", joins=[_left_join("b")])
        b = _model("b")
        await raw_storage.save_model(a)
        await raw_storage.save_model(b)

        await _mirror_inner_joins(a, raw_storage)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 0

    async def test_target_missing_no_crash(self, raw_storage) -> None:
        a = _model("a", joins=[_inner_join("nonexistent")])
        await raw_storage.save_model(a)
        await _mirror_inner_joins(a, raw_storage)  # should not raise

    async def test_join_pair_update(self, raw_storage) -> None:
        """When join_pairs change, the reverse is updated (not duplicated)."""
        a = _model("a", joins=[_inner_join("b", [["fk_id", "id"]])])
        b = _model("b", joins=[_inner_join("a", [["id", "fk_id"]])])
        await raw_storage.save_model(a)
        await raw_storage.save_model(b)

        # Change A's join pairs
        a.joins[0].join_pairs = [["new_fk", "id"]]
        await raw_storage.save_model(a)
        await _mirror_inner_joins(a, raw_storage)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 1
        assert b_reloaded.joins[0].join_pairs == [["id", "new_fk"]]


# ---------------------------------------------------------------------------
# JoinSyncStorage — save_model
# ---------------------------------------------------------------------------


class TestJoinSyncSave:
    async def test_save_creates_reverse(self, raw_storage, synced_storage) -> None:
        b = _model("b")
        await raw_storage.save_model(b)

        a = _model("a", joins=[_inner_join("b")])
        await synced_storage.save_model(a)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 1
        assert b_reloaded.joins[0].target_model == "a"
        assert b_reloaded.joins[0].join_type == JoinType.INNER

    async def test_save_left_join_no_mirror(self, raw_storage, synced_storage) -> None:
        b = _model("b")
        await raw_storage.save_model(b)

        a = _model("a", joins=[_left_join("b")])
        await synced_storage.save_model(a)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 0

    async def test_remove_inner_join_removes_reverse(self, raw_storage, synced_storage) -> None:
        b = _model("b")
        await raw_storage.save_model(b)

        # Create with inner join
        a = _model("a", joins=[_inner_join("b")])
        await synced_storage.save_model(a)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 1  # reverse exists

        # Save again without the join
        a_no_join = _model("a")
        await synced_storage.save_model(a_no_join)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 0  # reverse removed

    async def test_change_inner_to_left_removes_reverse(self, raw_storage, synced_storage) -> None:
        b = _model("b")
        await raw_storage.save_model(b)

        a = _model("a", joins=[_inner_join("b")])
        await synced_storage.save_model(a)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 1

        # Change to left join
        a_left = _model("a", joins=[_left_join("b")])
        await synced_storage.save_model(a_left)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 0

    async def test_change_left_to_inner_creates_reverse(self, raw_storage, synced_storage) -> None:
        b = _model("b")
        await raw_storage.save_model(b)

        a = _model("a", joins=[_left_join("b")])
        await synced_storage.save_model(a)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 0

        # Change to inner join
        a_inner = _model("a", joins=[_inner_join("b")])
        await synced_storage.save_model(a_inner)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 1
        assert b_reloaded.joins[0].join_type == JoinType.INNER

    async def test_join_pair_update_propagates(self, raw_storage, synced_storage) -> None:
        b = _model("b")
        await raw_storage.save_model(b)

        a = _model("a", joins=[_inner_join("b", [["fk_id", "id"]])])
        await synced_storage.save_model(a)

        b_reloaded = await raw_storage.get_model("b")
        assert b_reloaded.joins[0].join_pairs == [["id", "fk_id"]]

        # Update pairs
        a_updated = _model("a", joins=[_inner_join("b", [["new_fk", "id"]])])
        await synced_storage.save_model(a_updated)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 1
        assert b_reloaded.joins[0].join_pairs == [["id", "new_fk"]]


# ---------------------------------------------------------------------------
# JoinSyncStorage — delete_model
# ---------------------------------------------------------------------------


class TestJoinSyncDelete:
    async def test_delete_removes_reverse_joins(self, raw_storage, synced_storage) -> None:
        b = _model("b")
        await raw_storage.save_model(b)

        a = _model("a", joins=[_inner_join("b")])
        await synced_storage.save_model(a)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 1

        await synced_storage.delete_model("a")

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 0

    async def test_delete_nonexistent_returns_false(self, synced_storage) -> None:
        result = await synced_storage.delete_model("no_such_model")
        assert result is False

    async def test_delete_preserves_left_joins(self, raw_storage, synced_storage) -> None:
        """Deleting a model should only remove inner reverse joins, not unrelated left joins."""
        b = _model("b", joins=[_left_join("c")])
        c = _model("c")
        await raw_storage.save_model(b)
        await raw_storage.save_model(c)

        a = _model("a", joins=[_inner_join("b")])
        await synced_storage.save_model(a)

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 2  # left→c and inner→a

        await synced_storage.delete_model("a")

        b_reloaded = await raw_storage.get_model("b")
        assert len(b_reloaded.joins) == 1
        assert b_reloaded.joins[0].target_model == "c"
        assert b_reloaded.joins[0].join_type == JoinType.LEFT


# ---------------------------------------------------------------------------
# Startup reconciliation
# ---------------------------------------------------------------------------


class TestReconciliation:
    async def test_asymmetric_inner_join_healed_on_access(self, raw_storage) -> None:
        """If A→B inner exists but B→A does not, accessing via the wrapper heals it."""
        a = _model("a", joins=[_inner_join("b")])
        b = _model("b")
        await raw_storage.save_model(a)
        await raw_storage.save_model(b)

        synced = JoinSyncStorage(inner=raw_storage)
        # First access triggers reconciliation
        b_reloaded = await synced.get_model("b")
        assert len(b_reloaded.joins) == 1
        assert b_reloaded.joins[0].target_model == "a"
        assert b_reloaded.joins[0].join_type == JoinType.INNER

    async def test_already_symmetric_no_change(self, raw_storage) -> None:
        """If joins are already symmetric, reconciliation is a no-op."""
        a = _model("a", joins=[_inner_join("b")])
        b = _model("b", joins=[_inner_join("a", [["id", "fk_id"]])])
        await raw_storage.save_model(a)
        await raw_storage.save_model(b)

        synced = JoinSyncStorage(inner=raw_storage)
        b_reloaded = await synced.get_model("b")
        assert len(b_reloaded.joins) == 1

    async def test_reconciliation_runs_only_once(self, raw_storage) -> None:
        """After first access, adding asymmetric data directly to raw storage
        is NOT auto-healed (reconciliation only runs once)."""
        synced = JoinSyncStorage(inner=raw_storage)
        await synced.list_models()  # triggers reconciliation (empty, no-op)

        # Now add asymmetric data directly to raw storage (bypassing wrapper)
        a = _model("a", joins=[_inner_join("b")])
        b = _model("b")
        await raw_storage.save_model(a)
        await raw_storage.save_model(b)

        # Subsequent reads through wrapper don't re-reconcile
        b_reloaded = await synced.get_model("b")
        assert len(b_reloaded.joins) == 0  # NOT healed, as expected


# ---------------------------------------------------------------------------
# v4: datasource-aware reverse-join mirroring
#
# After v4 (DEV-1330) two models can share a name across datasources. A model
# saved in ``db_a`` that joins ``customers`` must mirror to ``customers@db_a``
# — reverse joins never cross a datasource boundary, because cross-datasource
# joins aren't executable in SLayer's query engine anyway.
# ---------------------------------------------------------------------------


from slayer.core.models import DatasourceConfig  # noqa: E402


def _model_in(name: str, data_source: str, *, joins: list[ModelJoin] | None = None) -> SlayerModel:
    return SlayerModel(
        name=name,
        sql_table=name,
        data_source=data_source,
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.NUMBER),
        ],
        joins=joins or [],
    )


class TestDatasourceAwareJoinSync:
    async def test_inner_join_mirrors_within_same_datasource_only(self, raw_storage, synced_storage) -> None:
        """``orders@db_a`` joins ``customers``; both ``customers@db_a`` and
        ``customers@db_b`` exist. Only the same-datasource target gets the
        reverse join."""
        await raw_storage.save_datasource(DatasourceConfig(name="db_a", type="postgres", host="h"))
        await raw_storage.save_datasource(DatasourceConfig(name="db_b", type="postgres", host="h"))

        cust_a = _model_in("customers", "db_a")
        cust_b = _model_in("customers", "db_b")
        await raw_storage.save_model(cust_a)
        await raw_storage.save_model(cust_b)

        orders_a = _model_in("orders", "db_a", joins=[_inner_join("customers")])
        await synced_storage.save_model(orders_a)

        cust_a_reloaded = await raw_storage.get_model("customers", data_source="db_a")
        cust_b_reloaded = await raw_storage.get_model("customers", data_source="db_b")

        assert cust_a_reloaded is not None
        assert cust_b_reloaded is not None

        # Reverse join landed on db_a's customers only.
        assert any(j.target_model == "orders" for j in cust_a_reloaded.joins)
        assert all(j.target_model != "orders" for j in cust_b_reloaded.joins)

    async def test_inner_join_target_only_in_other_datasource_no_op(self, raw_storage, synced_storage) -> None:
        """``orders@db_a`` joins ``customers``; the only ``customers`` lives
        in ``db_b``. Cross-datasource joins are not auto-mirrored — the
        reverse join is silently skipped (matches existing behavior for an
        unresolvable target name)."""
        await raw_storage.save_datasource(DatasourceConfig(name="db_a", type="postgres", host="h"))
        await raw_storage.save_datasource(DatasourceConfig(name="db_b", type="postgres", host="h"))

        cust_b = _model_in("customers", "db_b")
        await raw_storage.save_model(cust_b)

        orders_a = _model_in("orders", "db_a", joins=[_inner_join("customers")])
        await synced_storage.save_model(orders_a)

        cust_b_reloaded = await raw_storage.get_model("customers", data_source="db_b")
        assert cust_b_reloaded is not None
        # No reverse join was added on the other-datasource target.
        assert all(j.target_model != "orders" for j in cust_b_reloaded.joins)

    async def test_delete_clears_reverse_in_same_datasource_only(self, raw_storage, synced_storage) -> None:
        """Deleting ``orders@db_a`` removes the reverse join on
        ``customers@db_a`` — and nothing happens to ``customers@db_b``."""
        await raw_storage.save_datasource(DatasourceConfig(name="db_a", type="postgres", host="h"))
        await raw_storage.save_datasource(DatasourceConfig(name="db_b", type="postgres", host="h"))

        cust_a = _model_in("customers", "db_a")
        cust_b = _model_in("customers", "db_b", joins=[_inner_join("orders")])
        await raw_storage.save_model(cust_a)
        await raw_storage.save_model(cust_b)

        orders_a = _model_in("orders", "db_a", joins=[_inner_join("customers")])
        await synced_storage.save_model(orders_a)

        await synced_storage.delete_model("orders", data_source="db_a")

        cust_a_reloaded = await raw_storage.get_model("customers", data_source="db_a")
        cust_b_reloaded = await raw_storage.get_model("customers", data_source="db_b")
        assert cust_a_reloaded is not None
        assert cust_b_reloaded is not None
        # db_a's customers no longer references orders.
        assert all(j.target_model != "orders" for j in cust_a_reloaded.joins)
        # db_b's customers is untouched (the manually-added join survives).
        assert any(j.target_model == "orders" for j in cust_b_reloaded.joins)
