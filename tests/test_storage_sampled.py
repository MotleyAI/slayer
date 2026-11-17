"""Per-column sample-value persistence (DEV-1375, extended in DEV-1480).

Pins ``StorageBackend.update_field_sampled`` semantics across the ABC
contract, the YAML and SQLite implementations, and the ``resolve_storage``
factory path.

DEV-1480 extends the signature with two new required kwargs alongside
``sampled``: ``sampled_values: Optional[List[str]]`` (the structured top-N
list paired with the text summary) and ``distinct_count: Optional[int]``
(the true total cardinality at profile time). All three round-trip through
every backend.
"""

from __future__ import annotations

import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.storage.base import resolve_storage
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


def _make_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="orders",
        data_source="ds",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="amount", datatype=DataType.DOUBLE,
                   description="Total amount."),
            DatasetField(name="status", datatype=DataType.TEXT),
        ],
    )


# ---------------------------------------------------------------------------
# YAML
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_yaml_update_column_sampled_persists() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_dataset(_make_dataset())
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="amount", sampled="0.0 .. 9999.99",
            sampled_values=None, distinct_count=None,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        assert loaded is not None
        assert loaded.get_field("amount").sampled == "0.0 .. 9999.99"
        # Other columns untouched.
        assert loaded.get_field("id").sampled is None
        assert loaded.get_field("status").sampled is None


@pytest.mark.asyncio
async def test_yaml_update_column_sampled_to_none_clears() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        m = _make_dataset()
        m.fields[1].sampled = "stale value"
        await storage.save_dataset(m)
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="amount", sampled=None,
            sampled_values=None, distinct_count=None,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        assert loaded.get_field("amount").sampled is None


@pytest.mark.asyncio
async def test_yaml_update_column_sampled_preserves_other_fields() -> None:
    """Read-modify-write must not lose adjacent fields."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        m = _make_dataset()
        m.fields[1].description = "preserve me"
        m.fields[1].label = "Order Amount"
        await storage.save_dataset(m)
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="amount", sampled="0 .. 100",
            sampled_values=None, distinct_count=None,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        col = loaded.get_field("amount")
        assert col.description == "preserve me"
        assert col.label == "Order Amount"
        assert col.sampled == "0 .. 100"


@pytest.mark.asyncio
async def test_yaml_update_column_sampled_unknown_model_errors() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        with pytest.raises((KeyError, ValueError, FileNotFoundError)):
            await storage.update_field_sampled(
                data_source="ds", dataset_name="nope",
                field_name="amount", sampled="x",
                sampled_values=None, distinct_count=None,
            )


@pytest.mark.asyncio
async def test_yaml_update_column_sampled_unknown_column_errors() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_dataset(_make_dataset())
        with pytest.raises((KeyError, ValueError)):
            await storage.update_field_sampled(
                data_source="ds", dataset_name="orders",
                field_name="nope", sampled="x",
                sampled_values=None, distinct_count=None,
            )


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sqlite_update_column_sampled_persists() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/storage.db"
        storage = SQLiteStorage(db_path=db_path)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_dataset(_make_dataset())
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="amount", sampled="0.0 .. 9999.99",
            sampled_values=None, distinct_count=None,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        assert loaded.get_field("amount").sampled == "0.0 .. 9999.99"


@pytest.mark.asyncio
async def test_sqlite_update_column_sampled_preserves_other_fields() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/storage.db"
        storage = SQLiteStorage(db_path=db_path)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        m = _make_dataset()
        m.fields[1].description = "preserve me"
        await storage.save_dataset(m)
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="amount", sampled="0 .. 100",
            sampled_values=None, distinct_count=None,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        col = loaded.get_field("amount")
        assert col.description == "preserve me"
        assert col.sampled == "0 .. 100"


# ---------------------------------------------------------------------------
# resolve_storage factory path (DEV-1853: join_sync wrapper retired — the
# factory returns the bare backend)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_factory_storage_update_column_sampled() -> None:
    """update_column_sampled works through the factory-returned backend."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wrapped = resolve_storage(tmpdir)
        assert isinstance(wrapped, YAMLStorage)
        await wrapped.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await wrapped.save_dataset(_make_dataset())
        await wrapped.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="amount", sampled="0 .. 100",
            sampled_values=None, distinct_count=None,
        )
        loaded = await wrapped.get_dataset("orders", data_source="ds")
        assert loaded.get_field("amount").sampled == "0 .. 100"


@pytest.mark.asyncio
async def test_factory_storage_sqlite_update_column_sampled() -> None:
    """Same factory path, SQLite-backed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/storage.db"
        wrapped = resolve_storage(db_path)
        assert isinstance(wrapped, SQLiteStorage)
        await wrapped.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await wrapped.save_dataset(_make_dataset())
        await wrapped.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="amount", sampled="0 .. 100",
            sampled_values=None, distinct_count=None,
        )
        loaded = await wrapped.get_dataset("orders", data_source="ds")
        assert loaded.get_field("amount").sampled == "0 .. 100"


# ---------------------------------------------------------------------------
# Two-update independence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_sampled_updates_to_different_columns_dont_clobber_each_other() -> None:
    """Sequential updates to different columns must accumulate, not overwrite."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_dataset(_make_dataset())
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="amount", sampled="0 .. 100",
            sampled_values=None, distinct_count=None,
        )
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="status", sampled="paid, refunded",
            sampled_values=["paid", "refunded"], distinct_count=2,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        assert loaded.get_field("amount").sampled == "0 .. 100"
        assert loaded.get_field("amount").sampled_values is None
        assert loaded.get_field("amount").distinct_count is None
        assert loaded.get_field("status").sampled == "paid, refunded"
        assert loaded.get_field("status").sampled_values == ["paid", "refunded"]
        assert loaded.get_field("status").distinct_count == 2


# ---------------------------------------------------------------------------
# DEV-1480: sampled_values + distinct_count round-trip alongside sampled
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_yaml_round_trips_sampled_values_list() -> None:
    """A populated ``sampled_values`` list survives YAML write+read."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_dataset(_make_dataset())
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="status",
            sampled="paid, refunded, cancelled",
            sampled_values=["paid", "refunded", "cancelled"],
            distinct_count=3,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        col = loaded.get_field("status")
        assert col.sampled_values == ["paid", "refunded", "cancelled"]
        assert col.distinct_count == 3
        assert col.sampled == "paid, refunded, cancelled"


@pytest.mark.asyncio
async def test_yaml_round_trips_values_with_commas() -> None:
    """Values containing commas survive intact via the structured list
    even though the text ``sampled`` ambiguates them at split time."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_dataset(_make_dataset())
        comma_values = ["R$ 1,000–3,000", "R$ 3,000–5,000", "R$ 5,000–10,000"]
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="status",
            sampled=", ".join(comma_values),
            sampled_values=comma_values,
            distinct_count=3,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        col = loaded.get_field("status")
        # The structured list preserves the exact strings — ambiguous-text
        # split would have produced 6 fragments instead of 3 values.
        assert col.sampled_values == comma_values


@pytest.mark.asyncio
async def test_yaml_round_trips_empty_list_for_all_null_column() -> None:
    """All-NULL profiled column: empty list + text="" + distinct_count=0."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_dataset(_make_dataset())
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="status",
            sampled="", sampled_values=[], distinct_count=0,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        col = loaded.get_field("status")
        assert col.sampled == ""
        assert col.sampled_values == []
        assert col.distinct_count == 0


@pytest.mark.asyncio
async def test_yaml_round_trips_overflow_with_top_50() -> None:
    """Overflow case: top 50 in sampled_values, full count in distinct_count."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_dataset(_make_dataset())
        top_50 = [f"v{i:03d}" for i in range(50)]
        top_20_joined = ", ".join(top_50[:20])
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="status",
            sampled=f"{top_20_joined} ... (1234 distinct)",
            sampled_values=top_50,
            distinct_count=1234,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        col = loaded.get_field("status")
        assert col.distinct_count == 1234
        assert col.sampled_values == top_50
        assert col.sampled.endswith("(1234 distinct)")


@pytest.mark.asyncio
async def test_yaml_clear_sampled_values_independently_of_sampled() -> None:
    """``sampled_values=None`` clears the list even if ``sampled`` is set."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        m = _make_dataset()
        m.fields[2].sampled = "paid, refunded"
        m.fields[2].sampled_values = ["paid", "refunded"]
        m.fields[2].distinct_count = 2
        await storage.save_dataset(m)
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="status",
            sampled="paid, refunded, cancelled",
            sampled_values=None,
            distinct_count=None,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        col = loaded.get_field("status")
        assert col.sampled == "paid, refunded, cancelled"
        assert col.sampled_values is None
        assert col.distinct_count is None


@pytest.mark.asyncio
async def test_yaml_preserves_other_columns_when_updating_one() -> None:
    """Updating column A's new fields must not touch column B's fields."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        m = _make_dataset()
        m.fields[1].sampled = "0 .. 100"      # amount
        m.fields[1].distinct_count = None
        m.fields[2].sampled = "paid"           # status
        m.fields[2].sampled_values = ["paid"]
        m.fields[2].distinct_count = 1
        await storage.save_dataset(m)
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="status",
            sampled="paid, refunded",
            sampled_values=["paid", "refunded"],
            distinct_count=2,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        # status updated.
        assert loaded.get_field("status").sampled_values == ["paid", "refunded"]
        assert loaded.get_field("status").distinct_count == 2
        # amount untouched.
        assert loaded.get_field("amount").sampled == "0 .. 100"
        assert loaded.get_field("amount").sampled_values is None
        assert loaded.get_field("amount").distinct_count is None


@pytest.mark.asyncio
async def test_sqlite_round_trips_sampled_values_and_distinct_count() -> None:
    """SQLite backend stores the new fields inside the JSON ``data`` blob."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/storage.db"
        storage = SQLiteStorage(db_path=db_path)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_dataset(_make_dataset())
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="status",
            sampled="paid, refunded",
            sampled_values=["paid", "refunded"],
            distinct_count=2,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        col = loaded.get_field("status")
        assert col.sampled_values == ["paid", "refunded"]
        assert col.distinct_count == 2


@pytest.mark.asyncio
async def test_sqlite_clear_distinct_count_via_none() -> None:
    """Setting ``distinct_count=None`` must drop the persisted key."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/storage.db"
        storage = SQLiteStorage(db_path=db_path)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        m = _make_dataset()
        m.fields[2].distinct_count = 5
        await storage.save_dataset(m)
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="status",
            sampled="paid",
            sampled_values=["paid"],
            distinct_count=None,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        assert loaded.get_field("status").distinct_count is None


@pytest.mark.asyncio
async def test_factory_storage_sampled_values_and_distinct_count() -> None:
    """Both DEV-1480 kwargs round-trip through the factory-returned backend."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wrapped = resolve_storage(tmpdir)
        assert isinstance(wrapped, YAMLStorage)
        await wrapped.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await wrapped.save_dataset(_make_dataset())
        await wrapped.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="status",
            sampled="paid",
            sampled_values=["paid"],
            distinct_count=1,
        )
        loaded = await wrapped.get_dataset("orders", data_source="ds")
        col = loaded.get_field("status")
        assert col.sampled_values == ["paid"]
        assert col.distinct_count == 1


@pytest.mark.asyncio
async def test_update_does_not_clobber_distinct_count_for_other_columns() -> None:
    """Sequential per-column updates: each column's distinct_count is independent."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_dataset(_make_dataset())
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="status", sampled="paid",
            sampled_values=["paid"], distinct_count=1,
        )
        await storage.update_field_sampled(
            data_source="ds", dataset_name="orders",
            field_name="amount", sampled="0 .. 9999.99",
            sampled_values=None, distinct_count=None,
        )
        loaded = await storage.get_dataset("orders", data_source="ds")
        assert loaded.get_field("status").distinct_count == 1
        assert loaded.get_field("amount").distinct_count is None
