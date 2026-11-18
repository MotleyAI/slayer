"""Column sample profiling: result shape, categorical ordering/overflow, forced refresh, lazy refresh.

Categorical: frequency-ordered top values (up to 50) plus ``distinct_count``; ``sampled`` is the
top-20 joined, overflow appends ``... (50+ distinct)``; all-NULL gives ``""`` / ``[]`` / ``0``.
"""

from __future__ import annotations

import asyncio
import logging
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.profiling import ensure_samples_fresh, refresh_table_backed_dataset_sampled
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import resolve_storage
from slayer.storage.sqlite_conn import transaction
from slayer.storage.base import StorageBackend


def _stored_col(dataset: SlayerDataset, name: str) -> DatasetField:
    col = dataset.get_field(name)
    assert col is not None
    return col


async def _sample(
    *, dataset: SlayerDataset, column: DatasetField, engine: SlayerQueryEngine, storage, force: bool = False,
) -> DatasetField:
    """Profile one column through the owner and return the refreshed column."""
    outcome = await ensure_samples_fresh(
        dataset=dataset, fields=[column], engine=engine, storage=storage, force=force,
    )
    return outcome.fields[0]


def _count_queries(*, engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch) -> list:
    log: list = []
    real = engine.execute

    async def counting(*args, **kwargs):
        log.append(kwargs.get("query", args[0] if args else None))
        return await real(*args, **kwargs)

    monkeypatch.setattr(engine, "execute", counting)
    return log


@pytest.fixture
def sqlite_setup():
    """Build a SQLite-backed engine + storage with a populated `orders` table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        with transaction(db_file) as conn:
            conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, status TEXT)")
            conn.executemany(
                "INSERT INTO orders VALUES (?, ?, ?)",
                [
                    (1, 10.0, "paid"),
                    (2, 20.5, "paid"),
                    (3, 5.0, "refunded"),
                    (4, 99.99, "cancelled"),
                    (5, None, "paid"),
                ],
            )

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)

        ds = DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        )

        async def _setup():
            await storage.save_datasource(ds)
            await storage.save_dataset(SlayerDataset(
                name="orders",
                sql_table="orders",
                data_source="ds",
                fields=[
                    DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                    DatasetField(name="amount", datatype=DataType.DOUBLE),
                    DatasetField(name="status", datatype=DataType.TEXT),
                ],
            ))

        asyncio.run(_setup())
        engine = SlayerQueryEngine(storage=storage)
        yield engine, storage


# ---------------------------------------------------------------------------
# Result shape
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_profile_returns_column_sample_for_categorical(sqlite_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    col = model.get_field("status")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample is not None
    # Low-cardinality TEXT → list-form + distinct_count + comma-joined text.
    assert sample.sampled_values is not None
    assert "paid" in sample.sampled_values
    assert "refunded" in sample.sampled_values
    assert sample.distinct_count == 3  # paid, refunded, cancelled


@pytest.mark.asyncio
async def test_profile_categorical_orders_by_frequency_desc(sqlite_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    """``paid`` appears 3x, ``refunded`` 1x, ``cancelled`` 1x → ``paid`` first.
    Tie between refunded and cancelled is broken alphabetically (asc).
    """
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    col = model.get_field("status")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample is not None
    assert sample.sampled_values is not None
    assert sample.sampled_values[0] == "paid"
    # Tie-break: alphabetical asc between the two 1-count values.
    assert sample.sampled_values[1:] == ["cancelled", "refunded"]


@pytest.mark.asyncio
async def test_profile_categorical_sampled_text_is_top_20_joined(sqlite_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    col = model.get_field("status")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample is not None
    assert sample.sampled is not None
    # Below the 20-cap → entire list joined; no overflow suffix.
    assert "paid" in sample.sampled
    assert "refunded" in sample.sampled
    assert "cancelled" in sample.sampled
    assert "(" not in sample.sampled  # no "(N distinct)" suffix


@pytest.mark.asyncio
async def test_profile_returns_min_max_for_numeric(sqlite_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    col = model.get_field("amount")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample is not None
    assert sample.sampled is not None
    assert ".." in sample.sampled
    # Numeric/temporal columns have no structured list and no distinct_count.
    assert sample.sampled_values is None
    assert sample.distinct_count is None


@pytest.mark.asyncio
async def test_profile_handles_pk_columns(sqlite_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    """A sole primary key is never profiled: the column comes back unchanged."""
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    col = model.get_field("id")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample == col


# ---------------------------------------------------------------------------
# Frequency ordering — extra fixture with controlled value frequencies
# ---------------------------------------------------------------------------


@pytest.fixture
def freq_setup():
    """SQLite ``items`` table with skewed frequencies so ordering is testable."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        with transaction(db_file) as conn:
            conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, label TEXT, flag INTEGER)")
            rows = []
            # category counts: alpha 5, beta 2, gamma 1
            for _ in range(5):
                rows.append((len(rows) + 1, "alpha", "x", 1))
            for _ in range(2):
                rows.append((len(rows) + 1, "beta", "x", 0))
            rows.append((len(rows) + 1, "gamma", "x", None))
            conn.executemany("INSERT INTO items VALUES (?, ?, ?, ?)", rows)

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)

        async def _setup():
            await storage.save_datasource(DatasourceConfig(
                name="ds", type="sqlite", database=db_file,
            ))
            await storage.save_dataset(SlayerDataset(
                name="items",
                sql_table="items",
                data_source="ds",
                fields=[
                    DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                    DatasetField(name="category", datatype=DataType.TEXT),
                    DatasetField(name="label", datatype=DataType.TEXT),
                    DatasetField(name="flag", datatype=DataType.BOOLEAN),
                ],
            ))

        asyncio.run(_setup())
        engine = SlayerQueryEngine(storage=storage)
        yield engine, storage


@pytest.mark.asyncio
async def test_categorical_distinct_count_matches_observed_distinct(freq_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    engine, storage = freq_setup
    model = await storage.get_dataset("items", data_source="ds")
    assert model is not None
    col = model.get_field("category")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample is not None
    assert sample.distinct_count == 3  # alpha, beta, gamma


@pytest.mark.asyncio
async def test_categorical_ordering_strict_frequency_desc(freq_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    engine, storage = freq_setup
    model = await storage.get_dataset("items", data_source="ds")
    assert model is not None
    col = model.get_field("category")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample is not None
    # 5 / 2 / 1 → unambiguous order, no ties.
    assert sample.sampled_values == ["alpha", "beta", "gamma"]


@pytest.mark.asyncio
async def test_categorical_all_single_value_alphabetical_tiebreak(freq_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    """All rows have the same ``label='x'`` → single distinct value, list=['x']."""
    engine, storage = freq_setup
    model = await storage.get_dataset("items", data_source="ds")
    assert model is not None
    col = model.get_field("label")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample is not None
    assert sample.sampled_values == ["x"]
    assert sample.distinct_count == 1


@pytest.mark.asyncio
async def test_boolean_column_treated_as_categorical(freq_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    """Boolean columns are categorical: both ``True``/``False`` are str-coerced."""
    engine, storage = freq_setup
    model = await storage.get_dataset("items", data_source="ds")
    assert model is not None
    col = model.get_field("flag")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample is not None
    # Both 1 (5 rows) and 0 (2 rows) present; NULL filtered. 1 first by freq.
    assert sample.sampled_values is not None
    assert sample.distinct_count == 2
    # Stored as strings (List[str] field type).
    assert all(isinstance(v, str) for v in sample.sampled_values)


# ---------------------------------------------------------------------------
# All-NULL column
# ---------------------------------------------------------------------------


@pytest.fixture
def all_null_setup():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        with transaction(db_file) as conn:
            conn.execute("CREATE TABLE empties (id INTEGER PRIMARY KEY, notes TEXT)")
            conn.executemany(
                "INSERT INTO empties VALUES (?, ?)",
                [(i, None) for i in range(1, 6)],
            )

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)

        async def _setup():
            await storage.save_datasource(DatasourceConfig(
                name="ds", type="sqlite", database=db_file,
            ))
            await storage.save_dataset(SlayerDataset(
                name="empties",
                sql_table="empties",
                data_source="ds",
                fields=[
                    DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                    DatasetField(name="notes", datatype=DataType.TEXT),
                ],
            ))

        asyncio.run(_setup())
        engine = SlayerQueryEngine(storage=storage)
        yield engine, storage


@pytest.mark.asyncio
async def test_all_null_categorical_returns_empty_list_and_empty_text(all_null_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    """Contract: ``sampled_values=[]``, ``sampled=""``, ``distinct_count=0``."""
    engine, storage = all_null_setup
    model = await storage.get_dataset("empties", data_source="ds")
    assert model is not None
    col = model.get_field("notes")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample is not None
    assert sample.sampled_values == []
    assert sample.sampled == ""
    assert sample.distinct_count == 0


# ---------------------------------------------------------------------------
# Overflow at the 50-cap boundary
# ---------------------------------------------------------------------------


@pytest.fixture
def overflow_setup():
    """SQLite ``hi_card`` table with 60 distinct values to exercise overflow."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        with transaction(db_file) as conn:
            conn.execute("CREATE TABLE hi_card (id INTEGER PRIMARY KEY, name TEXT)")
            rows = []
            # First 10 values are most common (3 rows each); next 50 are 1 row each.
            for i in range(10):
                for _ in range(3):
                    rows.append((len(rows) + 1, f"common_{i:02d}"))
            for i in range(50):
                rows.append((len(rows) + 1, f"rare_{i:02d}"))
            conn.executemany("INSERT INTO hi_card VALUES (?, ?)", rows)

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)

        async def _setup():
            await storage.save_datasource(DatasourceConfig(
                name="ds", type="sqlite", database=db_file,
            ))
            await storage.save_dataset(SlayerDataset(
                name="hi_card",
                sql_table="hi_card",
                data_source="ds",
                fields=[
                    DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                    DatasetField(name="name", datatype=DataType.TEXT),
                ],
            ))

        asyncio.run(_setup())
        engine = SlayerQueryEngine(storage=storage)
        yield engine, storage


@pytest.mark.asyncio
async def test_overflow_stores_top_50_by_frequency(overflow_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    """60 distinct values → top 50 stored. Top 10 (3-row each) come first."""
    engine, storage = overflow_setup
    model = await storage.get_dataset("hi_card", data_source="ds")
    assert model is not None
    col = model.get_field("name")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample is not None
    assert sample.sampled_values is not None
    assert len(sample.sampled_values) == 50
    # The 10 ``common_*`` values are the most frequent — they MUST be in the top 50.
    for i in range(10):
        assert f"common_{i:02d}" in sample.sampled_values
    # First 10 entries are the 3-count common values.
    assert sample.sampled_values[:10] == [f"common_{i:02d}" for i in range(10)]


@pytest.mark.asyncio
async def test_overflow_distinct_count_is_unknown(overflow_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    """On overflow we no longer fire a second count_distinct scan — the
    exact total is unknown (``distinct_count=None``), but the top-50 list
    is retained so the column is still cached."""
    engine, storage = overflow_setup
    model = await storage.get_dataset("hi_card", data_source="ds")
    assert model is not None
    col = model.get_field("name")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample is not None
    assert sample.distinct_count is None
    assert sample.sampled_values is not None
    assert len(sample.sampled_values) == 50


@pytest.mark.asyncio
async def test_overflow_text_includes_top_20_and_marker(overflow_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    """Text format on overflow: ``", ".join(top_20) + " ... (50+ distinct)"``."""
    engine, storage = overflow_setup
    model = await storage.get_dataset("hi_card", data_source="ds")
    assert model is not None
    col = model.get_field("name")
    assert col is not None
    sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert sample is not None
    assert sample.sampled is not None
    assert sample.sampled.endswith("(50+ distinct)")
    # First 20 by frequency present in the text — top 10 commons + 10 rares.
    for i in range(10):
        assert f"common_{i:02d}" in sample.sampled


@pytest.mark.asyncio
async def test_overflow_classification_unaffected_by_one_null_row() -> None:
    """51 non-null distinct values + 1 NULL row → still classified as overflow.

    Codex finding: LIMIT must absorb the NULL row so the post-filter
    non-null count can be compared cleanly against ``max_values``.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        with transaction(db_file) as conn:
            conn.execute("CREATE TABLE just_over (id INTEGER PRIMARY KEY, label TEXT)")
            rows = [(i + 1, f"v_{i:03d}") for i in range(51)]
            rows.append((52, None))  # type: ignore[arg-type]
            conn.executemany("INSERT INTO just_over VALUES (?, ?)", rows)

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        ))
        await storage.save_dataset(SlayerDataset(
            name="just_over",
            sql_table="just_over",
            data_source="ds",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="label", datatype=DataType.TEXT),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)

        model = await storage.get_dataset("just_over", data_source="ds")
        assert model is not None
        col = model.get_field("label")
        assert col is not None
        sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
        assert sample is not None
        # 51 non-null distinct → overflow. Exact total not computed (single
        # scan), top-50 retained.
        assert sample.distinct_count is None
        assert sample.sampled_values is not None
        assert len(sample.sampled_values) == 50


@pytest.mark.asyncio
async def test_non_overflow_at_50_boundary() -> None:
    """Exactly 50 non-null distinct → NOT overflow; full list persisted."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        with transaction(db_file) as conn:
            conn.execute("CREATE TABLE at_cap (id INTEGER PRIMARY KEY, label TEXT)")
            rows = [(i + 1, f"v_{i:03d}") for i in range(50)]
            conn.executemany("INSERT INTO at_cap VALUES (?, ?)", rows)

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        ))
        await storage.save_dataset(SlayerDataset(
            name="at_cap",
            sql_table="at_cap",
            data_source="ds",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="label", datatype=DataType.TEXT),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)

        model = await storage.get_dataset("at_cap", data_source="ds")
        assert model is not None
        col = model.get_field("label")
        assert col is not None
        sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
        assert sample is not None
        assert sample.distinct_count == 50
        assert sample.sampled_values is not None
        assert len(sample.sampled_values) == 50
        assert sample.sampled is not None
        assert "(" not in sample.sampled  # no overflow suffix


# ---------------------------------------------------------------------------
# Tie-break determinism at LIMIT boundary
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tiebreak_deterministic_at_limit_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    """60 values all with count=1 → top-50 by SQL-side ORDER BY value ASC.

    Values are inserted in REVERSE alphabetical order so that a buggy
    implementation that omits the SQL-side ``ORDER BY label ASC`` would
    pull the last 50 inserted (which happen to be values v_009..v_058 once
    the DB ignores insertion order, or anything but the alphabetically-first
    50). Only a correct ``ORDER BY _count DESC, label ASC`` + LIMIT yields
    [v_000, v_001, ..., v_049]. Python's belt-and-braces post-sort cannot
    rescue a wrong LIMIT cutoff.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        with transaction(db_file) as conn:
            conn.execute("CREATE TABLE ties (id INTEGER PRIMARY KEY, label TEXT)")
            # Insert in reverse: v_059, v_058, ..., v_001, v_000.
            rows = [(i + 1, f"v_{(59 - i):03d}") for i in range(60)]
            conn.executemany("INSERT INTO ties VALUES (?, ?)", rows)

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        ))
        await storage.save_dataset(SlayerDataset(
            name="ties",
            sql_table="ties",
            data_source="ds",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="label", datatype=DataType.TEXT),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)

        model = await storage.get_dataset("ties", data_source="ds")
        assert model is not None
        col = model.get_field("label")
        assert col is not None
        sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
        assert sample is not None
        assert sample.distinct_count is None  # overflow → exact total not computed
        assert sample.sampled_values is not None
        # Alphabetical asc tie-break at the LIMIT cutoff → first 50 by value.
        # Note: this can only be produced by SQL-side ORDER BY label ASC.
        # A Python-only sort over a different LIMIT-pruned subset would
        # include some v_05x values and miss some v_00x values.
        assert sample.sampled_values == [f"v_{i:03d}" for i in range(50)]

        # Re-profile produces the same list — deterministic across runs.
        queries = _count_queries(engine=engine, monkeypatch=monkeypatch)
        sample2 = await _sample(dataset=model, column=col, engine=engine, storage=storage, force=True)
        assert len(queries) == 1
        assert sample2 is not None
        assert sample2.sampled_values == sample.sampled_values


# ---------------------------------------------------------------------------
# Comma-containing values (the issue's headline bug)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_values_with_commas_preserved_in_structured_list() -> None:
    """``"R$ 1,000–3,000"`` survives in ``sampled_values`` as one item."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        with transaction(db_file) as conn:
            conn.execute("CREATE TABLE income (id INTEGER PRIMARY KEY, bracket TEXT)")
            comma_values = [
                "R$ 1,000–3,000",
                "R$ 3,000–5,000",
                "R$ 5,000–10,000",
            ]
            rows = [(i + 1, v) for i, v in enumerate(comma_values)]
            conn.executemany("INSERT INTO income VALUES (?, ?)", rows)

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        ))
        await storage.save_dataset(SlayerDataset(
            name="income",
            sql_table="income",
            data_source="ds",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="bracket", datatype=DataType.TEXT),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)

        model = await storage.get_dataset("income", data_source="ds")
        assert model is not None
        col = model.get_field("bracket")
        assert col is not None
        sample = await _sample(dataset=model, column=col, engine=engine, storage=storage)
        assert sample is not None
        assert sample.sampled_values is not None
        # The structured list has the exact 3 strings.
        assert sorted(sample.sampled_values) == sorted(comma_values)
        # Naive comma-split of the text would give 6 fragments, not 3 — that's
        # why downstream consumers must use ``sampled_values``.


# ---------------------------------------------------------------------------
# refresh_table_backed_model_sampled
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_persists_all_three_fields_for_categorical(sqlite_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    errors = await refresh_table_backed_dataset_sampled(
        dataset=model, engine=engine, storage=storage,
    )
    assert errors == []
    reloaded = await storage.get_dataset("orders", data_source="ds")
    assert reloaded is not None
    status_col = reloaded.get_field("status")
    assert status_col is not None
    # All three fields populated for categorical.
    assert status_col.sampled is not None
    assert status_col.sampled_values is not None
    assert status_col.distinct_count is not None


@pytest.mark.asyncio
async def test_refresh_persists_only_sampled_for_numeric(sqlite_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    """Numeric/temporal: ``sampled`` set, ``sampled_values`` and
    ``distinct_count`` stay None per the contract."""
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    await refresh_table_backed_dataset_sampled(
        dataset=model, engine=engine, storage=storage,
    )
    reloaded = await storage.get_dataset("orders", data_source="ds")
    assert reloaded is not None
    amount_col = reloaded.get_field("amount")
    assert amount_col is not None
    assert amount_col.sampled is not None
    assert ".." in amount_col.sampled
    assert amount_col.sampled_values is None
    assert amount_col.distinct_count is None


@pytest.mark.asyncio
async def test_refresh_skips_hidden_columns(sqlite_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    model.fields.append(
        DatasetField(name="hidden_one", datatype=DataType.TEXT, hidden=True),
    )
    await storage.save_dataset(model)
    stored = await storage.get_dataset("orders", data_source="ds")
    assert stored is not None
    await refresh_table_backed_dataset_sampled(
        dataset=stored,
        engine=engine,
        storage=storage,
    )
    reloaded = await storage.get_dataset("orders", data_source="ds")
    assert reloaded is not None
    assert _stored_col(reloaded, "hidden_one").sampled is None
    assert _stored_col(reloaded, "hidden_one").sampled_values is None
    assert _stored_col(reloaded, "hidden_one").distinct_count is None


@pytest.mark.asyncio
async def test_refresh_only_columns_filter(sqlite_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    await refresh_table_backed_dataset_sampled(
        dataset=model, engine=engine, storage=storage,
        only_fields={"status"},
    )
    reloaded = await storage.get_dataset("orders", data_source="ds")
    assert reloaded is not None
    assert _stored_col(reloaded, "status").sampled is not None
    assert _stored_col(reloaded, "status").sampled_values is not None
    assert _stored_col(reloaded, "amount").sampled is None


@pytest.mark.asyncio
async def test_refresh_skips_sql_mode_models(sqlite_setup) -> None:
    """sql-mode model: silently skipped (broader coverage: DEV-1377)."""
    engine, storage = sqlite_setup
    sql_model = SlayerDataset(
        name="sql_orders",
        sql="SELECT * FROM orders",
        data_source="ds",
        fields=[DatasetField(name="amount", datatype=DataType.DOUBLE)],
    )
    errors = await refresh_table_backed_dataset_sampled(
        dataset=sql_model, engine=engine, storage=storage,
    )
    assert errors == []


@pytest.mark.asyncio
async def test_refresh_continues_after_per_column_failure(sqlite_setup: tuple[SlayerQueryEngine, StorageBackend]) -> None:
    """Best-effort: one bad column doesn't stop the rest."""
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    model.fields = [
        DatasetField(name="amount", sql="no_such_fn(status)", datatype=DataType.DOUBLE) if c.name == "amount" else c
        for c in model.fields
    ]
    await storage.save_dataset(model)
    errors = await refresh_table_backed_dataset_sampled(
        dataset=model, engine=engine, storage=storage,
    )
    assert any("amount" in e and "no such function" in e for e in errors)
    reloaded = await storage.get_dataset("orders", data_source="ds")
    assert reloaded is not None
    assert _stored_col(reloaded, "amount").sampled is None
    assert _stored_col(reloaded, "status").sampled is not None
    assert _stored_col(reloaded, "status").sampled_values is not None


@pytest.mark.asyncio
async def test_refresh_passes_all_three_kwargs_to_storage(sqlite_setup: tuple[SlayerQueryEngine, StorageBackend], monkeypatch: pytest.MonkeyPatch) -> None:
    """The refresh path calls ``update_column_sampled`` with all three new
    kwargs — TDD pin for the API surface change."""
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None

    captured: list[dict] = []
    original = storage.update_field_sampled

    async def capturing(**kwargs):
        captured.append(kwargs)
        return await original(**kwargs)

    monkeypatch.setattr(storage, "update_field_sampled", capturing)
    await refresh_table_backed_dataset_sampled(
        dataset=model, engine=engine, storage=storage,
    )
    # Every call has sampled / sampled_values / distinct_count kwargs.
    assert captured, "refresh should have invoked update_column_sampled"
    for kw in captured:
        assert "sampled" in kw
        assert "sampled_values" in kw
        assert "distinct_count" in kw


# ---------------------------------------------------------------------------
# ensure_samples_fresh — lazy cache-aware refresh
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_fresh_returns_input_when_cache_hit(
    sqlite_setup: tuple[SlayerQueryEngine, StorageBackend], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cached categorical column short-circuits: no query, no persist, column unchanged."""
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    col = model.get_field("status")
    assert col is not None
    col.sampled = "paid, refunded, cancelled"
    col.sampled_values = ["paid", "refunded", "cancelled"]
    col.distinct_count = 3

    persist_calls = {"n": 0}
    original_persist = storage.update_field_sampled

    async def counting_persist(**kwargs):
        persist_calls["n"] += 1
        return await original_persist(**kwargs)

    monkeypatch.setattr(storage, "update_field_sampled", counting_persist)
    queries = _count_queries(engine=engine, monkeypatch=monkeypatch)

    result = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert result == col
    assert queries == []
    assert persist_calls["n"] == 0


@pytest.mark.asyncio
async def test_ensure_fresh_categorical_miss_profiles_and_persists(
    sqlite_setup: tuple[SlayerQueryEngine, StorageBackend],
) -> None:
    """A stale categorical column is profiled, persisted, and returned refreshed."""
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    col = model.get_field("status")
    assert col is not None
    col.sampled = None
    col.sampled_values = None
    col.distinct_count = None

    refreshed = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert refreshed.sampled is not None
    assert refreshed.sampled_values is not None
    assert refreshed.distinct_count is not None
    reloaded = await storage.get_dataset("orders", data_source="ds")
    assert reloaded is not None
    reloaded_col = reloaded.get_field("status")
    assert reloaded_col is not None
    assert reloaded_col.sampled_values is not None
    assert reloaded_col.distinct_count is not None


@pytest.mark.asyncio
async def test_ensure_fresh_returns_input_when_profile_raises(
    sqlite_setup: tuple[SlayerQueryEngine, StorageBackend], monkeypatch: pytest.MonkeyPatch, caplog,
) -> None:
    """A failed profile query is logged with context; nothing is persisted and the column is unchanged."""
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    col = model.get_field("status")
    assert col is not None
    col.sampled = None
    col.sampled_values = None
    col.distinct_count = None

    real_execute = engine.execute

    async def fail_top_values(*args, **kwargs):
        q = kwargs.get("query", args[0] if args else None)
        if isinstance(q, SlayerQuery) and q.dimensions:
            raise RuntimeError("simulated profile failure")
        return await real_execute(*args, **kwargs)

    persist_calls: list = []
    original_persist = storage.update_field_sampled

    async def tracking_persist(**kwargs):
        persist_calls.append(kwargs)
        return await original_persist(**kwargs)

    monkeypatch.setattr(engine, "execute", fail_top_values)
    monkeypatch.setattr(storage, "update_field_sampled", tracking_persist)

    with caplog.at_level(logging.WARNING, logger="slayer.engine.profiling"):
        result = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert result == col
    # Writing None on failure would clobber a stale cache.
    assert persist_calls == []
    assert any(
        "status" in rec.getMessage() and "orders" in rec.getMessage()
        for rec in caplog.records
    ), "profile failure must be logged with context"


@pytest.mark.asyncio
async def test_ensure_fresh_swallows_persist_failure_returns_refreshed(
    sqlite_setup: tuple[SlayerQueryEngine, StorageBackend], monkeypatch: pytest.MonkeyPatch, caplog,
) -> None:
    """A persist failure is logged; the in-memory refresh is still returned."""
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    col = model.get_field("status")
    assert col is not None
    col.sampled = None
    col.sampled_values = None
    col.distinct_count = None

    async def boom_persist(**_kwargs):  # NOSONAR(S7503) — required async signature: monkeypatches update_column_sampled (async)
        raise RuntimeError("simulated persist failure")

    monkeypatch.setattr(storage, "update_field_sampled", boom_persist)

    with caplog.at_level(logging.WARNING, logger="slayer.engine.profiling"):
        result = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert result.sampled_values is not None
    assert result.distinct_count is not None
    assert any(
        "persist" in rec.getMessage().lower() or "update_column" in rec.getMessage()
        for rec in caplog.records
    ), "persist failure must be logged with context"


@pytest.mark.asyncio
async def test_ensure_fresh_refreshes_uncached_numeric_temporal(
    sqlite_setup: tuple[SlayerQueryEngine, StorageBackend],
) -> None:
    """An uncached numeric column gets its min/max range filled and persisted."""
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    col = model.get_field("amount")
    assert col is not None
    assert col.sampled is None

    result = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert result is not col
    assert result.sampled is not None
    assert ".." in result.sampled
    assert result.sampled_values is None
    assert result.distinct_count is None
    reloaded = await storage.get_dataset("orders", data_source="ds")
    assert reloaded is not None
    assert _stored_col(reloaded, "amount").sampled is not None


@pytest.mark.asyncio
async def test_ensure_fresh_refreshes_uncached_temporal(tmp_path) -> None:
    """A DATE column is back-filled with its min/max range too."""
    db_file = str(tmp_path / "t.db")
    with transaction(db_file) as conn:
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, ts DATE)")
        conn.executemany(
            "INSERT INTO events VALUES (?, ?)",
            [(1, "2024-01-01"), (2, "2024-06-30")],
        )
    storage = resolve_storage(str(tmp_path / "st"))
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=db_file)
    )
    await storage.save_dataset(SlayerDataset(
        name="events", sql_table="events", data_source="ds",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="ts", datatype=DataType.DATE),
        ],
    ))
    engine = SlayerQueryEngine(storage=storage)
    model = await storage.get_dataset("events", data_source="ds")
    assert model is not None
    col = model.get_field("ts")
    assert col is not None
    assert col.sampled is None

    result = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert result is not col
    assert result.sampled is not None
    assert ".." in result.sampled
    reloaded = await storage.get_dataset("events", data_source="ds")
    assert reloaded is not None
    assert _stored_col(reloaded, "ts").sampled is not None


@pytest.mark.asyncio
async def test_ensure_fresh_cached_numeric_short_circuits(
    sqlite_setup: tuple[SlayerQueryEngine, StorageBackend], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An already-cached numeric column runs no profiling query."""
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None
    col = model.get_field("amount")
    assert col is not None
    col.sampled = "5.0 .. 99.99"

    queries = _count_queries(engine=engine, monkeypatch=monkeypatch)
    result = await _sample(dataset=model, column=col, engine=engine, storage=storage)
    assert result == col
    assert queries == [], "a cached numeric column must not be re-profiled"


@pytest.mark.asyncio
async def test_ensure_fresh_skips_hidden_and_primary_key(
    sqlite_setup: tuple[SlayerQueryEngine, StorageBackend], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hidden / PK columns are never profiled or persisted."""
    engine, storage = sqlite_setup
    model = await storage.get_dataset("orders", data_source="ds")
    assert model is not None

    persist_calls: list = []
    original_persist = storage.update_field_sampled

    async def counting_persist(**kwargs):  # NOSONAR(S7503) — required async signature: monkeypatches update_column_sampled (async)
        persist_calls.append(kwargs.get("column_name"))
        return await original_persist(**kwargs)

    monkeypatch.setattr(storage, "update_field_sampled", counting_persist)
    queries = _count_queries(engine=engine, monkeypatch=monkeypatch)

    pk_col = model.get_field("id")
    assert pk_col is not None
    assert pk_col.primary_key is True

    result = await _sample(dataset=model, column=pk_col, engine=engine, storage=storage)
    assert result == pk_col
    assert queries == [], "PK columns must never be profiled"
    assert "id" not in persist_calls

    hidden = DatasetField(name="hidden_one", datatype=DataType.TEXT, hidden=True)
    model.fields.append(hidden)
    await storage.save_dataset(model)
    refreshed_model = await storage.get_dataset("orders", data_source="ds")
    assert refreshed_model is not None
    hidden_col = refreshed_model.get_field("hidden_one")
    assert hidden_col is not None
    result_hidden = await _sample(
        dataset=refreshed_model, column=hidden_col, engine=engine, storage=storage,
    )
    assert result_hidden == hidden_col
    assert queries == [], "hidden columns must never be profiled"
    assert "hidden_one" not in persist_calls


@pytest.mark.asyncio
async def test_ensure_fresh_does_not_hard_gate_sql_mode(
    sqlite_setup: tuple[SlayerQueryEngine, StorageBackend], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lazy path profiles sql-mode models too; only forced refresh is table-backed-only."""
    engine, storage = sqlite_setup
    sql_model = SlayerDataset(
        name="sql_orders",
        sql="SELECT * FROM orders",
        data_source="ds",
        fields=[DatasetField(name="status", datatype=DataType.TEXT)],
    )
    await storage.save_dataset(sql_model)
    col = sql_model.get_field("status")
    assert col is not None

    queries = _count_queries(engine=engine, monkeypatch=monkeypatch)
    result = await _sample(dataset=sql_model, column=col, engine=engine, storage=storage)
    assert queries, "sql-mode models must be profiled on the lazy path"
    assert result.sampled_values == ["paid", "cancelled", "refunded"]
