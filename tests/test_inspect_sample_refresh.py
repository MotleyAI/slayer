"""DEV-1615: ``inspect`` (single-entity point-lookup) lazily back-fills
column sample values on read, exactly like ``inspect_model`` / ``search``.

Contract pinned here (settled in the spec interview + Codex passes):

* ``inspect(reference, entity_type="column", compact=False)`` on a column
  whose persisted sample is missing/stale triggers a live profile via the
  shared ``ensure_column_sample_fresh`` helper, renders the fresh
  ``Sample values:`` line, AND persists the refreshed sample for later reads.
* Coverage is full non-regression vs ``inspect_model``: BOTH categorical
  (distinct-value lists) AND numeric/temporal (min/max ranges) columns.
* ``compact=True`` stays cheap/DB-free: description-only, NO refresh, NO
  profile query — the broad-exploration mode never shows ``Sample values:``.
* Only ``entity_type="column"`` refreshes; measures / aggregations have no
  sample concept and never trigger a profile.
* Engine-guarded: with ``engine=None`` the refresh is a silent no-op.
* Hidden columns are rendered but never back-filled (system-wide
  ``hidden = never profiled`` convention; parity with ``inspect_model``).
* Best-effort: a profile or persist failure inside the refresh never turns
  the inspect read into a user-visible error.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.inspect.service import InspectService
from slayer.storage.base import resolve_storage


# ---------------------------------------------------------------------------
# Fixtures — a REAL sqlite-backed datasource so profiling returns live data.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def inspect_setup(tmp_path) -> AsyncIterator[tuple[InspectService, object]]:
    """Build a SQLite-backed engine + storage + InspectService over a
    populated ``orders`` table. Columns are saved WITHOUT sampled values so
    every non-PK column starts uncached (the back-fill trigger)."""
    db_file = str(tmp_path / "data.db")
    conn = sqlite3.connect(db_file)
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, "
        "status TEXT, secret TEXT, order_date DATE)"
    )
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
        [
            (1, 10.0, "paid", "x", "2024-01-01"),
            (2, 20.5, "paid", "y", "2024-02-15"),
            (3, 5.0, "refunded", "z", "2024-03-30"),
            (4, 99.99, "cancelled", "w", "2024-04-10"),
            (5, None, "paid", "v", "2024-05-20"),
        ],
    )
    conn.commit()
    conn.close()

    storage = resolve_storage(str(tmp_path / "storage"))
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=db_file)
    )
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="ds",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE,
                   description="Order total in USD."),
            Column(name="status", type=DataType.TEXT,
                   description="Order lifecycle status."),
            Column(name="order_date", type=DataType.DATE,
                   description="Date the order was placed."),
            Column(name="secret", type=DataType.TEXT,
                   description="Hidden but inspectable.", hidden=True),
        ],
        measures=[
            ModelMeasure(name="aov", formula="amount:sum / *:count",
                         description="Average order value."),
        ],
        aggregations=[
            Aggregation(name="custom_max", formula="MAX({col})",
                        description="A custom aggregation."),
        ],
    ))
    engine = SlayerQueryEngine(storage=storage)
    svc = InspectService(storage=storage, engine=engine)
    yield svc, storage


def _count_profile_calls(monkeypatch) -> list:
    """Monkeypatch ``profile_column`` (the function ``ensure_column_sample_fresh``
    delegates to) with a counter that still returns ``None`` so nothing
    persists. Returns the list its names are appended to."""
    calls: list = []
    import slayer.engine.profiling as prof

    real = prof.profile_column

    async def counting(*, model, column, engine):  # NOSONAR(S7503)
        calls.append(column.name)
        return await real(model=model, column=column, engine=engine)

    monkeypatch.setattr("slayer.engine.profiling.profile_column", counting)
    return calls


# ---------------------------------------------------------------------------
# (1) categorical back-fill on compact=False
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compact_false_backfills_categorical(inspect_setup) -> None:
    svc, storage = inspect_setup
    out = await svc.inspect(
        reference="ds.orders.status", entity_type="column", compact=False,
    )
    assert "Sample values:" in out
    assert "paid" in out
    # Persisted for later reads.
    reloaded = await storage.get_model("orders", data_source="ds")
    col = reloaded.get_column("status")
    assert col.sampled_values is not None
    assert "paid" in col.sampled_values


# ---------------------------------------------------------------------------
# (2) numeric/temporal back-fill on compact=False (full non-regression)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compact_false_backfills_numeric(inspect_setup) -> None:
    svc, storage = inspect_setup
    out = await svc.inspect(
        reference="ds.orders.amount", entity_type="column", compact=False,
    )
    assert "Sample values:" in out
    # Numeric render is a min .. max range.
    assert ".." in out
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("amount").sampled is not None


@pytest.mark.asyncio
async def test_compact_false_backfills_temporal(inspect_setup) -> None:
    """DATE/TIMESTAMP columns route through the numeric/temporal min/max path
    too — full non-regression includes temporal ranges."""
    svc, storage = inspect_setup
    out = await svc.inspect(
        reference="ds.orders.order_date", entity_type="column", compact=False,
    )
    assert "Sample values:" in out
    assert ".." in out  # min .. max range
    assert "2024-01-01" in out
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("order_date").sampled is not None


# ---------------------------------------------------------------------------
# (3) compact=True stays cheap — no refresh, no profile query
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compact_true_does_not_refresh(inspect_setup, monkeypatch) -> None:
    svc, storage = inspect_setup
    calls = _count_profile_calls(monkeypatch)
    out = await svc.inspect(
        reference="ds.orders.status", entity_type="column", compact=True,
    )
    assert "Sample values:" not in out
    assert calls == [], "compact=True must not run a profile query"
    # And nothing was persisted.
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("status").sampled_values is None


@pytest.mark.asyncio
async def test_compact_true_does_not_refresh_numeric(
    inspect_setup, monkeypatch,
) -> None:
    """DEV-1615 newly back-fills numeric on compact=False — pin that
    compact=True still does NOT profile/persist a numeric column either."""
    svc, storage = inspect_setup
    calls = _count_profile_calls(monkeypatch)
    out = await svc.inspect(
        reference="ds.orders.amount", entity_type="column", compact=True,
    )
    assert "Sample values:" not in out
    assert calls == [], "compact=True must not run a profile query for numeric"
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("amount").sampled is None


# ---------------------------------------------------------------------------
# (4) measures / aggregations never trigger a profile
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_measure_does_not_refresh(inspect_setup, monkeypatch) -> None:
    svc, _ = inspect_setup
    calls = _count_profile_calls(monkeypatch)
    out = await svc.inspect(
        reference="ds.orders.aov", entity_type="measure", compact=False,
    )
    assert "amount:sum / *:count" in out
    assert calls == [], "measures have no sample concept"


@pytest.mark.asyncio
async def test_aggregation_does_not_refresh(inspect_setup, monkeypatch) -> None:
    svc, _ = inspect_setup
    calls = _count_profile_calls(monkeypatch)
    out = await svc.inspect(
        reference="ds.orders.custom_max", entity_type="aggregation",
        compact=False,
    )
    assert "MAX({col})" in out
    assert calls == [], "aggregations have no sample concept"


# ---------------------------------------------------------------------------
# (5) engine=None — silent no-op, no crash
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_none_no_refresh_attempt(inspect_setup, monkeypatch) -> None:
    _, storage = inspect_setup
    svc = InspectService(storage=storage)  # no engine

    attempts: list = []

    async def spy(*, model, column, engine, storage):  # NOSONAR(S7503)
        attempts.append(column.name)
        return column

    # Count calls at the inspect-service call site: with engine=None the
    # service must not even attempt the helper (the gate is before the call).
    monkeypatch.setattr(
        "slayer.inspect.service.ensure_column_sample_fresh", spy,
    )
    out = await svc.inspect(
        reference="ds.orders.status", entity_type="column", compact=False,
    )
    assert attempts == [], "engine=None must not attempt a refresh"
    # Renders the column header but no live samples (uncached + no engine).
    assert "Column: ds.orders.status" in out
    assert "Sample values:" not in out
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("status").sampled_values is None


# ---------------------------------------------------------------------------
# inspect_model still passes ONLY categorical columns to the helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inspect_model_passes_only_categorical_to_helper(
    inspect_setup, monkeypatch,
) -> None:
    """Removing the helper's numeric early-return is global, but
    ``inspect_model`` must remain unaffected: it profiles numeric/temporal via
    its own batched min/max path and only routes CATEGORICAL columns through
    ``ensure_column_sample_fresh``."""
    svc, _ = inspect_setup
    seen: list = []

    import slayer.inspect.model_render as mr

    real = mr.ensure_column_sample_fresh

    async def recording(*, model, column, engine, storage):  # NOSONAR(S7503)
        seen.append((column.name, column.type))
        return await real(
            model=model, column=column, engine=engine, storage=storage,
        )

    monkeypatch.setattr(mr, "ensure_column_sample_fresh", recording)
    await svc.inspect(reference="ds.orders", entity_type="model", compact=False)

    names = {n for n, _ in seen}
    # Only the categorical TEXT column reaches the helper.
    assert "status" in names
    assert "amount" not in names, "numeric must not route through the helper"
    assert "order_date" not in names, "temporal must not route through the helper"
    assert all(t in (DataType.TEXT, DataType.BOOLEAN) for _, t in seen)


# ---------------------------------------------------------------------------
# (6) already-cached column short-circuits (no profile query)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_already_cached_does_not_reprofile(inspect_setup, monkeypatch) -> None:
    svc, storage = inspect_setup
    # Pre-populate the structured cache so the column reads as cached.
    await storage.update_column_sampled(
        data_source="ds", model_name="orders", column_name="status",
        sampled="paid, refunded", sampled_values=["paid", "refunded"],
        distinct_count=2,
    )
    calls = _count_profile_calls(monkeypatch)
    out = await svc.inspect(
        reference="ds.orders.status", entity_type="column", compact=False,
    )
    assert calls == [], "a cached column must not be re-profiled"
    assert "Sample values:" in out
    assert "paid" in out


# ---------------------------------------------------------------------------
# (7) JSON shape carries the refreshed samples in ``text``
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_json_text_carries_refreshed_samples(inspect_setup) -> None:
    svc, _ = inspect_setup
    out = await svc.inspect(
        reference="ds.orders.status", entity_type="column", compact=False,
        format="json",
    )
    payload = json.loads(out)
    assert "Sample values:" in payload["text"]
    assert "paid" in payload["text"]


# ---------------------------------------------------------------------------
# (8) batch refreshes every member, categorical AND numeric
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_refreshes_both_members(inspect_setup) -> None:
    svc, storage = inspect_setup
    out = await svc.inspect(
        reference=["ds.orders.status", "ds.orders.amount"],
        entity_type="column", compact=False,
    )
    assert "paid" in out          # categorical block
    assert ".." in out            # numeric block (min .. max)
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("status").sampled_values is not None
    assert reloaded.get_column("amount").sampled is not None


# ---------------------------------------------------------------------------
# (9) hidden column renders but is never back-filled
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hidden_column_renders_without_refresh(
    inspect_setup, monkeypatch,
) -> None:
    svc, storage = inspect_setup
    calls = _count_profile_calls(monkeypatch)
    out = await svc.inspect(
        reference="ds.orders.secret", entity_type="column", compact=False,
    )
    assert "Column: ds.orders.secret" in out
    assert calls == [], "hidden columns are never profiled"
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("secret").sampled_values is None


# ---------------------------------------------------------------------------
# (F3) best-effort: a profile failure never crashes the inspect read
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_profile_failure_renders_cleanly(inspect_setup, monkeypatch) -> None:
    """The refresh path runs (proving it is wired) but ``profile_column``
    raises; inspect must swallow it and still return a normal column render."""
    svc, _ = inspect_setup
    invoked: list = []

    async def boom(*, model, column, engine):  # NOSONAR(S7503)
        invoked.append(column.name)
        raise RuntimeError("profiling backend exploded")

    monkeypatch.setattr("slayer.engine.profiling.profile_column", boom)
    out = await svc.inspect(
        reference="ds.orders.status", entity_type="column", compact=False,
    )
    assert invoked == ["status"], "the refresh path must have executed"
    # No crash; column still renders (without fresh samples).
    assert "Column: ds.orders.status" in out


@pytest.mark.asyncio
async def test_persist_failure_renders_cleanly(inspect_setup, monkeypatch) -> None:
    """A persist failure (``update_column_sampled`` raises) is swallowed; the
    in-memory refresh still renders fresh samples this call."""
    svc, storage = inspect_setup

    async def boom(**kwargs):  # NOSONAR(S7503)
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(storage, "update_column_sampled", boom)
    out = await svc.inspect(
        reference="ds.orders.status", entity_type="column", compact=False,
    )
    # Persist failed but the in-memory refresh surfaces this call.
    assert "Sample values:" in out
    assert "paid" in out


@pytest.mark.asyncio
async def test_persist_failure_numeric_renders_cleanly(
    inspect_setup, monkeypatch,
) -> None:
    """Numeric variant of the persist-failure path (DEV-1615 behavior change):
    a persist failure on a numeric column still renders the in-memory min/max
    range this call."""
    svc, storage = inspect_setup

    async def boom(**kwargs):  # NOSONAR(S7503)
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(storage, "update_column_sampled", boom)
    out = await svc.inspect(
        reference="ds.orders.amount", entity_type="column", compact=False,
    )
    assert "Sample values:" in out
    assert ".." in out  # in-memory min .. max range
