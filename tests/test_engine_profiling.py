"""Profile-column helper extracted from inspect_model (DEV-1375).

Pins:
* `profile_column` returns the same structure shape (``ColumnSample`` with
  ``sampled`` / ``sampled_values`` / ``distinct_count``) as the existing
  ``_collect_dim_profile`` / ``_format_dim_profile`` produces (DEV-1480).
* `refresh_table_backed_model_sampled` iterates non-hidden columns,
  persists each via storage with all three new fields, returns per-column
  error strings.
* sql-mode and query-backed models are silently skipped (mirrors ingest
  behaviour; broader coverage tracked in DEV-1377).
* Per-column DB exceptions don't stop the loop.

DEV-1480: categorical profiling now returns frequency-ordered top values
(up to 50, was 20) plus a total ``distinct_count``. Text ``sampled`` is the
top-20 joined; overflow appends ` ... (N distinct)`. All-NULL columns get
``sampled=""``, ``sampled_values=[]``, ``distinct_count=0``.
"""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from typing import Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.profiling import (
    ColumnSample,
    profile_column,
    refresh_table_backed_model_sampled,
)
from slayer.storage.base import resolve_storage


@pytest.fixture
def sqlite_setup():
    """Build a SQLite-backed engine + storage with a populated `orders` table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        conn = sqlite3.connect(db_file)
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
        conn.commit()
        conn.close()

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)

        ds = DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        )

        async def _setup():
            await storage.save_datasource(ds)
            await storage.save_model(SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="ds",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="amount", type=DataType.DOUBLE),
                    Column(name="status", type=DataType.TEXT),
                ],
            ))

        asyncio.run(_setup())
        engine = SlayerQueryEngine(storage=storage)
        yield engine, storage


# ---------------------------------------------------------------------------
# profile_column — return-type contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_profile_column_returns_column_sample_for_categorical(sqlite_setup) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    col = model.get_column("status")
    sample = await profile_column(model=model, column=col, engine=engine)
    assert sample is not None
    assert isinstance(sample, ColumnSample)
    # Low-cardinality TEXT → list-form + distinct_count + comma-joined text.
    assert sample.sampled_values is not None
    assert "paid" in sample.sampled_values
    assert "refunded" in sample.sampled_values
    assert sample.distinct_count == 3  # paid, refunded, cancelled


@pytest.mark.asyncio
async def test_profile_column_categorical_orders_by_frequency_desc(sqlite_setup) -> None:
    """``paid`` appears 3x, ``refunded`` 1x, ``cancelled`` 1x → ``paid`` first.
    Tie between refunded and cancelled is broken alphabetically (asc).
    """
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    col = model.get_column("status")
    sample = await profile_column(model=model, column=col, engine=engine)
    assert sample is not None
    assert sample.sampled_values is not None
    assert sample.sampled_values[0] == "paid"
    # Tie-break: alphabetical asc between the two 1-count values.
    assert sample.sampled_values[1:] == ["cancelled", "refunded"]


@pytest.mark.asyncio
async def test_profile_column_categorical_sampled_text_is_top_20_joined(sqlite_setup) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    col = model.get_column("status")
    sample = await profile_column(model=model, column=col, engine=engine)
    assert sample is not None
    assert sample.sampled is not None
    # Below the 20-cap → entire list joined; no overflow suffix.
    assert "paid" in sample.sampled
    assert "refunded" in sample.sampled
    assert "cancelled" in sample.sampled
    assert "(" not in sample.sampled  # no "(N distinct)" suffix


@pytest.mark.asyncio
async def test_profile_column_returns_min_max_for_numeric(sqlite_setup) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    col = model.get_column("amount")
    sample = await profile_column(model=model, column=col, engine=engine)
    assert sample is not None
    assert sample.sampled is not None
    assert ".." in sample.sampled
    # Numeric/temporal columns have no structured list and no distinct_count.
    assert sample.sampled_values is None
    assert sample.distinct_count is None


@pytest.mark.asyncio
async def test_profile_column_handles_pk_columns(sqlite_setup) -> None:
    """PK columns are still profiled-eligible only at the caller's discretion."""
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    col = model.get_column("id")
    sample = await profile_column(model=model, column=col, engine=engine)
    # Caller may get None (PK skipped) or a ColumnSample — both are acceptable.
    assert sample is None or isinstance(sample, ColumnSample)


# ---------------------------------------------------------------------------
# Frequency ordering — extra fixture with controlled value frequencies
# ---------------------------------------------------------------------------


@pytest.fixture
def freq_setup():
    """SQLite ``items`` table with skewed frequencies so ordering is testable."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        conn = sqlite3.connect(db_file)
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, label TEXT, flag INTEGER)")
        rows = []
        # category counts: alpha 5, beta 2, gamma 1
        for _ in range(5):
            rows.append((len(rows) + 1, "alpha", "x", 1))
        for _ in range(2):
            rows.append((len(rows) + 1, "beta", "x", 0))
        rows.append((len(rows) + 1, "gamma", "x", None))
        conn.executemany("INSERT INTO items VALUES (?, ?, ?, ?)", rows)
        conn.commit()
        conn.close()

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)

        async def _setup():
            await storage.save_datasource(DatasourceConfig(
                name="ds", type="sqlite", database=db_file,
            ))
            await storage.save_model(SlayerModel(
                name="items",
                sql_table="items",
                data_source="ds",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="category", type=DataType.TEXT),
                    Column(name="label", type=DataType.TEXT),
                    Column(name="flag", type=DataType.BOOLEAN),
                ],
            ))

        asyncio.run(_setup())
        engine = SlayerQueryEngine(storage=storage)
        yield engine, storage


@pytest.mark.asyncio
async def test_categorical_distinct_count_matches_observed_distinct(freq_setup) -> None:
    engine, storage = freq_setup
    model = await storage.get_model("items", data_source="ds")
    col = model.get_column("category")
    sample = await profile_column(model=model, column=col, engine=engine)
    assert sample is not None
    assert sample.distinct_count == 3  # alpha, beta, gamma


@pytest.mark.asyncio
async def test_categorical_ordering_strict_frequency_desc(freq_setup) -> None:
    engine, storage = freq_setup
    model = await storage.get_model("items", data_source="ds")
    col = model.get_column("category")
    sample = await profile_column(model=model, column=col, engine=engine)
    assert sample is not None
    # 5 / 2 / 1 → unambiguous order, no ties.
    assert sample.sampled_values == ["alpha", "beta", "gamma"]


@pytest.mark.asyncio
async def test_categorical_all_single_value_alphabetical_tiebreak(freq_setup) -> None:
    """All rows have the same ``label='x'`` → single distinct value, list=['x']."""
    engine, storage = freq_setup
    model = await storage.get_model("items", data_source="ds")
    col = model.get_column("label")
    sample = await profile_column(model=model, column=col, engine=engine)
    assert sample is not None
    assert sample.sampled_values == ["x"]
    assert sample.distinct_count == 1


@pytest.mark.asyncio
async def test_boolean_column_treated_as_categorical(freq_setup) -> None:
    """Boolean columns are categorical: both ``True``/``False`` are str-coerced."""
    engine, storage = freq_setup
    model = await storage.get_model("items", data_source="ds")
    col = model.get_column("flag")
    sample = await profile_column(model=model, column=col, engine=engine)
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
        conn = sqlite3.connect(db_file)
        conn.execute("CREATE TABLE empties (id INTEGER PRIMARY KEY, notes TEXT)")
        conn.executemany(
            "INSERT INTO empties VALUES (?, ?)",
            [(i, None) for i in range(1, 6)],
        )
        conn.commit()
        conn.close()

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)

        async def _setup():
            await storage.save_datasource(DatasourceConfig(
                name="ds", type="sqlite", database=db_file,
            ))
            await storage.save_model(SlayerModel(
                name="empties",
                sql_table="empties",
                data_source="ds",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="notes", type=DataType.TEXT),
                ],
            ))

        asyncio.run(_setup())
        engine = SlayerQueryEngine(storage=storage)
        yield engine, storage


@pytest.mark.asyncio
async def test_all_null_categorical_returns_empty_list_and_empty_text(all_null_setup) -> None:
    """Contract: ``sampled_values=[]``, ``sampled=""``, ``distinct_count=0``."""
    engine, storage = all_null_setup
    model = await storage.get_model("empties", data_source="ds")
    col = model.get_column("notes")
    sample = await profile_column(model=model, column=col, engine=engine)
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
        conn = sqlite3.connect(db_file)
        conn.execute("CREATE TABLE hi_card (id INTEGER PRIMARY KEY, name TEXT)")
        rows = []
        # First 10 values are most common (3 rows each); next 50 are 1 row each.
        for i in range(10):
            for _ in range(3):
                rows.append((len(rows) + 1, f"common_{i:02d}"))
        for i in range(50):
            rows.append((len(rows) + 1, f"rare_{i:02d}"))
        conn.executemany("INSERT INTO hi_card VALUES (?, ?)", rows)
        conn.commit()
        conn.close()

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)

        async def _setup():
            await storage.save_datasource(DatasourceConfig(
                name="ds", type="sqlite", database=db_file,
            ))
            await storage.save_model(SlayerModel(
                name="hi_card",
                sql_table="hi_card",
                data_source="ds",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="name", type=DataType.TEXT),
                ],
            ))

        asyncio.run(_setup())
        engine = SlayerQueryEngine(storage=storage)
        yield engine, storage


@pytest.mark.asyncio
async def test_overflow_stores_top_50_by_frequency(overflow_setup) -> None:
    """60 distinct values → top 50 stored. Top 10 (3-row each) come first."""
    engine, storage = overflow_setup
    model = await storage.get_model("hi_card", data_source="ds")
    col = model.get_column("name")
    sample = await profile_column(model=model, column=col, engine=engine)
    assert sample is not None
    assert sample.sampled_values is not None
    assert len(sample.sampled_values) == 50
    # The 10 ``common_*`` values are the most frequent — they MUST be in the top 50.
    for i in range(10):
        assert f"common_{i:02d}" in sample.sampled_values
    # First 10 entries are the 3-count common values.
    assert sample.sampled_values[:10] == [f"common_{i:02d}" for i in range(10)]


@pytest.mark.asyncio
async def test_overflow_distinct_count_is_true_total(overflow_setup) -> None:
    """``distinct_count`` reflects the column's full cardinality, not 50."""
    engine, storage = overflow_setup
    model = await storage.get_model("hi_card", data_source="ds")
    col = model.get_column("name")
    sample = await profile_column(model=model, column=col, engine=engine)
    assert sample is not None
    assert sample.distinct_count == 60


@pytest.mark.asyncio
async def test_overflow_text_includes_top_20_and_total(overflow_setup) -> None:
    """Text format: ``", ".join(top_20) + " ... (60 distinct)"``."""
    engine, storage = overflow_setup
    model = await storage.get_model("hi_card", data_source="ds")
    col = model.get_column("name")
    sample = await profile_column(model=model, column=col, engine=engine)
    assert sample is not None
    assert sample.sampled is not None
    assert sample.sampled.endswith("(60 distinct)")
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
        conn = sqlite3.connect(db_file)
        conn.execute("CREATE TABLE just_over (id INTEGER PRIMARY KEY, label TEXT)")
        rows = [(i + 1, f"v_{i:03d}") for i in range(51)]
        rows.append((52, None))
        conn.executemany("INSERT INTO just_over VALUES (?, ?)", rows)
        conn.commit()
        conn.close()

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        ))
        await storage.save_model(SlayerModel(
            name="just_over",
            sql_table="just_over",
            data_source="ds",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="label", type=DataType.TEXT),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)

        model = await storage.get_model("just_over", data_source="ds")
        col = model.get_column("label")
        sample = await profile_column(model=model, column=col, engine=engine)
        assert sample is not None
        # Exactly 51 non-null distinct → overflow.
        assert sample.distinct_count == 51
        assert sample.sampled_values is not None
        assert len(sample.sampled_values) == 50


@pytest.mark.asyncio
async def test_non_overflow_at_50_boundary() -> None:
    """Exactly 50 non-null distinct → NOT overflow; full list persisted."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        conn = sqlite3.connect(db_file)
        conn.execute("CREATE TABLE at_cap (id INTEGER PRIMARY KEY, label TEXT)")
        rows = [(i + 1, f"v_{i:03d}") for i in range(50)]
        conn.executemany("INSERT INTO at_cap VALUES (?, ?)", rows)
        conn.commit()
        conn.close()

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        ))
        await storage.save_model(SlayerModel(
            name="at_cap",
            sql_table="at_cap",
            data_source="ds",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="label", type=DataType.TEXT),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)

        model = await storage.get_model("at_cap", data_source="ds")
        col = model.get_column("label")
        sample = await profile_column(model=model, column=col, engine=engine)
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
async def test_tiebreak_deterministic_at_limit_boundary() -> None:
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
        conn = sqlite3.connect(db_file)
        conn.execute("CREATE TABLE ties (id INTEGER PRIMARY KEY, label TEXT)")
        # Insert in reverse: v_059, v_058, ..., v_001, v_000.
        rows = [(i + 1, f"v_{(59 - i):03d}") for i in range(60)]
        conn.executemany("INSERT INTO ties VALUES (?, ?)", rows)
        conn.commit()
        conn.close()

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        ))
        await storage.save_model(SlayerModel(
            name="ties",
            sql_table="ties",
            data_source="ds",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="label", type=DataType.TEXT),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)

        model = await storage.get_model("ties", data_source="ds")
        col = model.get_column("label")
        sample = await profile_column(model=model, column=col, engine=engine)
        assert sample is not None
        assert sample.distinct_count == 60
        assert sample.sampled_values is not None
        # Alphabetical asc tie-break at the LIMIT cutoff → first 50 by value.
        # Note: this can only be produced by SQL-side ORDER BY label ASC.
        # A Python-only sort over a different LIMIT-pruned subset would
        # include some v_05x values and miss some v_00x values.
        assert sample.sampled_values == [f"v_{i:03d}" for i in range(50)]

        # Re-profile produces the same list — deterministic across runs.
        sample2 = await profile_column(model=model, column=col, engine=engine)
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
        conn = sqlite3.connect(db_file)
        conn.execute("CREATE TABLE income (id INTEGER PRIMARY KEY, bracket TEXT)")
        comma_values = [
            "R$ 1,000–3,000",
            "R$ 3,000–5,000",
            "R$ 5,000–10,000",
        ]
        rows = [(i + 1, v) for i, v in enumerate(comma_values)]
        conn.executemany("INSERT INTO income VALUES (?, ?)", rows)
        conn.commit()
        conn.close()

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        ))
        await storage.save_model(SlayerModel(
            name="income",
            sql_table="income",
            data_source="ds",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="bracket", type=DataType.TEXT),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)

        model = await storage.get_model("income", data_source="ds")
        col = model.get_column("bracket")
        sample = await profile_column(model=model, column=col, engine=engine)
        assert sample is not None
        assert sample.sampled_values is not None
        # The structured list has the exact 3 strings.
        assert sorted(sample.sampled_values) == sorted(comma_values)
        # Naive comma-split of the text would give 6 fragments, not 3 — that's
        # why downstream consumers must use ``sampled_values``.


# ---------------------------------------------------------------------------
# allowed_aggregations / Column.filter on the source column must not break
# the overflow count_distinct second query.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_overflow_with_allowed_aggregations_whitelist_omitting_count_distinct() -> None:
    """Column with ``allowed_aggregations=["count"]`` (no count_distinct) →
    overflow profile still succeeds because the second query uses a transient
    ModelExtension column that bypasses the whitelist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        conn = sqlite3.connect(db_file)
        conn.execute("CREATE TABLE locked (id INTEGER PRIMARY KEY, label TEXT)")
        rows = [(i + 1, f"v_{i:03d}") for i in range(60)]
        conn.executemany("INSERT INTO locked VALUES (?, ?)", rows)
        conn.commit()
        conn.close()

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        ))
        await storage.save_model(SlayerModel(
            name="locked",
            sql_table="locked",
            data_source="ds",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                # Whitelist explicitly omits ``count_distinct``.
                Column(
                    name="label", type=DataType.TEXT,
                    allowed_aggregations=["count"],
                ),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)

        model = await storage.get_model("locked", data_source="ds")
        col = model.get_column("label")
        sample = await profile_column(model=model, column=col, engine=engine)
        # Profile must complete and surface the true total.
        assert sample is not None
        assert sample.distinct_count == 60


@pytest.mark.asyncio
async def test_overflow_with_column_filter_bypassed_by_model_extension() -> None:
    """Column.filter applies a CASE-WHEN at aggregation time. The overflow
    ``count_distinct`` second query must bypass it via ModelExtension so the
    persisted ``distinct_count`` reflects the column's RAW cardinality, not
    the post-filter subset.

    Test setup: 60 distinct values, ``Column.filter`` restricts to the first
    10. If the implementation routes count_distinct through the source column
    (not via ModelExtension), the filter would reduce the count to 10 instead
    of 60.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        conn = sqlite3.connect(db_file)
        conn.execute("CREATE TABLE filt (id INTEGER PRIMARY KEY, label TEXT)")
        rows = [(i + 1, f"v_{i:03d}") for i in range(60)]
        conn.executemany("INSERT INTO filt VALUES (?, ?)", rows)
        conn.commit()
        conn.close()

        storage_dir = f"{tmpdir}/storage"
        storage = resolve_storage(storage_dir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=db_file,
        ))
        await storage.save_model(SlayerModel(
            name="filt",
            sql_table="filt",
            data_source="ds",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(
                    name="label", type=DataType.TEXT,
                    filter="label LIKE 'v_00%'",  # matches first 10 values only
                ),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)

        model = await storage.get_model("filt", data_source="ds")
        col = model.get_column("label")
        sample = await profile_column(model=model, column=col, engine=engine)
        # ModelExtension bypass: count_distinct on the raw column ignores the
        # CASE-WHEN filter. distinct_count is the full 60, not 10.
        assert sample is not None
        assert sample.distinct_count == 60


# ---------------------------------------------------------------------------
# refresh_table_backed_model_sampled
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_persists_all_three_fields_for_categorical(sqlite_setup) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    errors = await refresh_table_backed_model_sampled(
        model=model, engine=engine, storage=storage,
    )
    assert errors == []
    reloaded = await storage.get_model("orders", data_source="ds")
    status_col = reloaded.get_column("status")
    # All three fields populated for categorical.
    assert status_col.sampled is not None
    assert status_col.sampled_values is not None
    assert status_col.distinct_count is not None


@pytest.mark.asyncio
async def test_refresh_persists_only_sampled_for_numeric(sqlite_setup) -> None:
    """Numeric/temporal: ``sampled`` set, ``sampled_values`` and
    ``distinct_count`` stay None per the contract."""
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    await refresh_table_backed_model_sampled(
        model=model, engine=engine, storage=storage,
    )
    reloaded = await storage.get_model("orders", data_source="ds")
    amount_col = reloaded.get_column("amount")
    assert amount_col.sampled is not None
    assert ".." in amount_col.sampled
    assert amount_col.sampled_values is None
    assert amount_col.distinct_count is None


@pytest.mark.asyncio
async def test_refresh_skips_hidden_columns(sqlite_setup) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    model.columns.append(
        Column(name="hidden_one", type=DataType.TEXT, hidden=True),
    )
    await storage.save_model(model)
    await refresh_table_backed_model_sampled(
        model=await storage.get_model("orders", data_source="ds"),
        engine=engine,
        storage=storage,
    )
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("hidden_one").sampled is None
    assert reloaded.get_column("hidden_one").sampled_values is None
    assert reloaded.get_column("hidden_one").distinct_count is None


@pytest.mark.asyncio
async def test_refresh_only_columns_filter(sqlite_setup) -> None:
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")
    await refresh_table_backed_model_sampled(
        model=model, engine=engine, storage=storage,
        only_columns={"status"},
    )
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("status").sampled is not None
    assert reloaded.get_column("status").sampled_values is not None
    assert reloaded.get_column("amount").sampled is None


@pytest.mark.asyncio
async def test_refresh_skips_sql_mode_models(sqlite_setup) -> None:
    """sql-mode model: silently skipped per DEV-1375 v1; broader coverage in
    DEV-1377."""
    engine, storage = sqlite_setup
    sql_model = SlayerModel(
        name="sql_orders",
        sql="SELECT * FROM orders",
        data_source="ds",
        columns=[Column(name="amount", type=DataType.DOUBLE)],
    )
    errors = await refresh_table_backed_model_sampled(
        model=sql_model, engine=engine, storage=storage,
    )
    assert errors == []


@pytest.mark.asyncio
async def test_refresh_continues_after_per_column_failure(sqlite_setup, monkeypatch) -> None:
    """Best-effort: one bad column doesn't stop the rest."""
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")

    call_count = {"n": 0}
    real_profile_column = profile_column

    async def boom_then_ok(*, model, column, engine) -> Optional[ColumnSample]:
        call_count["n"] += 1
        if column.name == "amount":
            raise RuntimeError("simulated profile failure")
        return await real_profile_column(model=model, column=column, engine=engine)

    monkeypatch.setattr(
        "slayer.engine.profiling.profile_column", boom_then_ok,
    )
    errors = await refresh_table_backed_model_sampled(
        model=model, engine=engine, storage=storage,
    )
    assert any("amount" in e and "simulated" in e for e in errors)
    reloaded = await storage.get_model("orders", data_source="ds")
    assert reloaded.get_column("amount").sampled is None
    assert reloaded.get_column("status").sampled is not None
    assert reloaded.get_column("status").sampled_values is not None


@pytest.mark.asyncio
async def test_refresh_passes_all_three_kwargs_to_storage(sqlite_setup, monkeypatch) -> None:
    """The refresh path calls ``update_column_sampled`` with all three new
    kwargs — TDD pin for the API surface change."""
    engine, storage = sqlite_setup
    model = await storage.get_model("orders", data_source="ds")

    captured: list[dict] = []
    original = storage.update_column_sampled

    async def capturing(**kwargs):
        captured.append(kwargs)
        return await original(**kwargs)

    monkeypatch.setattr(storage, "update_column_sampled", capturing)
    await refresh_table_backed_model_sampled(
        model=model, engine=engine, storage=storage,
    )
    # Every call has sampled / sampled_values / distinct_count kwargs.
    assert captured, "refresh should have invoked update_column_sampled"
    for kw in captured:
        assert "sampled" in kw
        assert "sampled_values" in kw
        assert "distinct_count" in kw
