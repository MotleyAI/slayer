"""DEV-1714 Stage 10 — integration VALUE tests for duration-windowed measures.

These exercise the ``_wm_`` range-join primitive against real in-process
databases (DuckDB + SQLite, no Docker) and assert hand-computed rolling-90d
VALUES — the class of coverage that catches the DEV-1496 "silent wrong
results" failure mode that SQL-shape assertions alone cannot.

Window arithmetic (window='90d', monthly buckets; 2024 is a leap year):

  bucket_end(M) = first day of the month AFTER M   (exclusive upper bound)
  lower(M)      = bucket_end(M) - 90 days
  rolling(M)    = SUM(amount) over rows with created_at in [lower, bucket_end)

Seeded orders::

    2024-01-01: 100   2024-01-15: 200   2024-02-15: 300   2024-03-15: 400

    Jan  bucket_end=2024-02-01  lower=2023-11-03  -> 100+200        = 300
    Feb  bucket_end=2024-03-01  lower=2023-12-02  -> 100+200+300    = 600
    Mar  bucket_end=2024-04-01  lower=2024-01-02  -> 200+300+400    = 900

The March bucket is the load-bearing case: 2024-01-01 < 2024-01-02, so the
first 100 falls OUT of March's trailing window (boundary), while 200 and 300
fall IN even though they precede a March-only ``date_range`` — proving the
window reaches before the range start.
"""

import sqlite3

import pytest

from slayer.async_utils import run_sync
from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

# (id, amount, created_at, region) — ``region`` is NULL for the Feb/Mar rows so
# the NULL-dimension join-back semantics can be asserted; the sum/avg value
# tests group by month only, so region does not affect their expectations.
_ORDERS = [
    (1, 100.0, "2024-01-01 00:00:00", "US"),
    (2, 200.0, "2024-01-15 00:00:00", "US"),
    (3, 300.0, "2024-02-15 00:00:00", None),
    (4, 400.0, "2024-03-15 00:00:00", None),
]

def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="wm",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            Column(name="region", sql="region", type=DataType.TEXT),
        ],
    )


# DEV-1733 — a SECOND table shaped so that ordering by a composite OVER a
# windowed measure cannot be satisfied by any of the ways the feature can be
# built wrong. ``_ORDERS`` above cannot do this: January's rolling value equals
# its plain sum (all January rows fall inside January's own window), and Feb and
# Mar both have one row, so no composite over a count can reorder them.
#
# Rows, with the SAME monthly-bucket / 90d window arithmetic as the docstring
# (lower(Jan)=2023-11-03, lower(Feb)=2023-12-02, lower(Mar)=2024-01-02):
#
#   month | rows          | count | rolling sum                     | plain sum
#   Jan   | 1000, 10, 10  |   3   | 1000+10+10          = 1020      | 1020
#   Feb   | 1             |   1   | 1000+10+10+1        = 1021      |    1
#   Mar   | 1             |   1   | 10+10+1+1           =   22      |    1
#
# The 2024-01-01 row of 1000 falls OUT of March's window (Jan 1 < Jan 2), which
# is what makes March's rolling value collapse to 22 while its plain sum is 1.
#
# ``id:count / revenue:sum(window='90d')`` DESC:
#   correct (rolling)    Mar 1/22=.04545 > Jan 3/1020=.00294 > Feb 1/1021=.00098
#                        -> Mar, Jan, Feb
# Every wrong build gives a different answer:
#   rolling sum only     Feb 1021 > Jan 1020 > Mar 22          -> Feb, Jan, Mar
#   plain sum only       Jan 1020 > Feb 1 = Mar 1              -> Jan first
#   count / PLAIN sum    Feb 1.0 = Mar 1.0 > Jan .00294        -> Jan LAST
#   divisor dropped      Jan 3 > Feb 1 = Mar 1                 -> Jan first
_COMPOSITE_ORDERS = [
    (1, 1000.0, "2024-01-01 00:00:00"),
    (2, 10.0, "2024-01-20 00:00:00"),
    (3, 10.0, "2024-01-21 00:00:00"),
    (4, 1.0, "2024-02-15 00:00:00"),
    (5, 1.0, "2024-03-15 00:00:00"),
]


def _composite_orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders_c",
        sql_table="orders_c",
        data_source="wm",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
        ],
    )


def _key(row, suffix: str) -> str:
    """The single result-column key ending with ``suffix`` (result keys are
    ``model.column`` — e.g. ``orders.created_at`` / ``orders.rev_90d``)."""
    return next(k for k in row if k.endswith(suffix))


@pytest.fixture(scope="module")
def _duckdb_windowed_storage(tmp_path_factory):
    duckdb = pytest.importorskip("duckdb")
    tmp = tmp_path_factory.mktemp("wm_duckdb")
    db_path = tmp / "w.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE orders (id INTEGER, amount DOUBLE, created_at TIMESTAMP, region VARCHAR)")
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", _ORDERS)
    conn.execute("CREATE TABLE orders_c (id INTEGER, amount DOUBLE, created_at TIMESTAMP)")
    conn.executemany("INSERT INTO orders_c VALUES (?, ?, ?)", _COMPOSITE_ORDERS)
    conn.close()
    storage = YAMLStorage(base_dir=str(tmp / "storage"))
    run_sync(storage.save_datasource(
        DatasourceConfig(name="wm", type="duckdb", database=str(db_path)),
    ))
    run_sync(storage.save_model(_orders_model()))
    run_sync(storage.save_model(_composite_orders_model()))
    return storage


@pytest.fixture(scope="module")
def _sqlite_windowed_storage(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("wm_sqlite")
    db_path = tmp / "w.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE orders (id INTEGER, amount REAL, created_at TIMESTAMP, region TEXT)")
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", _ORDERS)
    conn.execute("CREATE TABLE orders_c (id INTEGER, amount REAL, created_at TIMESTAMP)")
    conn.executemany("INSERT INTO orders_c VALUES (?, ?, ?)", _COMPOSITE_ORDERS)
    conn.commit()
    conn.close()
    storage = YAMLStorage(base_dir=str(tmp / "storage"))
    run_sync(storage.save_datasource(
        DatasourceConfig(name="wm", type="sqlite", database=str(db_path)),
    ))
    run_sync(storage.save_model(_orders_model()))
    run_sync(storage.save_model(_composite_orders_model()))
    return storage


@pytest.fixture(params=["duckdb", "sqlite"])
def windowed_engine(request) -> SlayerQueryEngine:
    """Per-test engine (binds to the current event loop) over the module-scoped
    per-dialect storage. ``getfixturevalue`` avoids eagerly building the DuckDB
    store when only the SQLite param runs (and vice-versa)."""
    storage = request.getfixturevalue(f"_{request.param}_windowed_storage")
    return SlayerQueryEngine(storage=storage)


@pytest.mark.integration
class TestWindowedMeasureValues:
    async def test_rolling_90d_sum_by_month(self, windowed_engine: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_90d"}],
        )
        result = await windowed_engine.execute(query=query)
        by_month = {
            str(r[_key(r, "created_at")])[:7]: float(r[_key(r, "rev_90d")])
            for r in result.data
        }
        assert by_month["2024-01"] == 300.0, by_month
        assert by_month["2024-02"] == 600.0, by_month
        # March excludes the 2024-01-01 row (Jan 1 < Jan 2 lower bound).
        assert by_month["2024-03"] == 900.0, by_month

    async def test_rolling_90d_avg_by_month(self, windowed_engine: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "revenue:avg(window='90d')", "name": "rev_avg"}],
        )
        result = await windowed_engine.execute(query=query)
        by_month = {
            str(r[_key(r, "created_at")])[:7]: float(r[_key(r, "rev_avg")])
            for r in result.data
        }
        # March window holds {200, 300, 400} -> mean 300.
        assert by_month["2024-03"] == 300.0, by_month

    async def test_rolling_90d_window_reaches_before_date_range_start(
        self, windowed_engine: SlayerQueryEngine,
    ) -> None:
        """A March-only ``date_range`` must still let the trailing window reach
        back before its start: March's rolling sum stays 900 (pulling the
        out-of-range Jan-15 200 and Feb-15 300), because ``date_range`` is
        stripped from the ``_src`` scope."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-03-31"],
            )],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_90d"}],
        )
        result = await windowed_engine.execute(query=query)
        assert result.row_count == 1, result.data
        assert float(result.data[0][_key(result.data[0], "rev_90d")]) == 900.0, result.data

    async def test_null_dimension_group_gets_its_real_windowed_value(
        self, windowed_engine: SlayerQueryEngine,
    ) -> None:
        """A base group whose dimension is NULL receives its REAL windowed value.

        This used to come back NULL, and was pinned as a documented consequence:
        the ``_src`` join-back inside the ``_wm_`` CTE compared the grain with a
        plain ``=``, and ``NULL = NULL`` is not TRUE, so the group matched no
        rows. The outer join-back and the cross-model join-back were already
        null-safe, so the answer depended on which isolation shape a measure
        landed in. The inner comparison is now null-safe too, which is what makes
        the value appear.

        Executed against real engines because this is a VALUE change, not a
        spelling one — and the two dialects spell null-safe equality differently
        (``IS NOT DISTINCT FROM`` on DuckDB, bare ``IS`` on SQLite).
        """
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="region")],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_90d"}],
        )
        result = await windowed_engine.execute(query=query)
        null_rows = [r for r in result.data if r[_key(r, "region")] is None]
        assert null_rows, result.data
        by_month = {
            str(r[_key(r, "created_at")])[:7]: float(r[_key(r, "rev_90d")])
            for r in null_rows
        }
        # The NULL-region rows are 300 (2024-02-15) and 400 (2024-03-15).
        # February's 90-day window reaches back over 300 only; March's reaches
        # back over both, so the two months differ and neither matches the
        # single-month sum by accident.
        assert by_month.get("2024-02") == 300.0, result.data
        assert by_month.get("2024-03") == 700.0, result.data
        # Sanity: a non-NULL dimension group still computes a real rolling value
        # (the US rows, both in January, roll up to 300).
        us_rows = [r for r in result.data if r[_key(r, "region")] == "US"]
        assert us_rows, result.data
        r0 = us_rows[0]
        assert float(r0[_key(r0, "rev_90d")]) == 300.0, r0


@pytest.mark.integration
class TestHiddenOrderOnlyWindowedValues(object):
    """DEV-1733 — a windowed measure used ONLY as an ORDER BY target.

    Before DEV-1733 this shape silently rendered a PLAIN ``SUM`` (the window
    was dropped), so the rows came back in the wrong order with no error.
    These assert the ROLLING value drives the sort, against real engines —
    the trailing-window arithmetic is the part SQLite is a weak proxy for.

    Rolling-90d by month (see module docstring): Jan 300, Feb 600, Mar 900 —
    strictly increasing, so every ordering below is tie-free and fully
    deterministic. The plain (non-windowed) monthly sums are Jan 300, Feb 300,
    Mar 400, which TIE at 300; a plain-sum regression therefore produces an
    engine-dependent order rather than the pinned one.

    Because plain and rolling are monotonically related on the month grain,
    ``test_order_only_windowed_region_grain_discriminates`` carries the
    assertion that plain sums cannot satisfy under any tie-break.
    """

    async def test_order_only_windowed_sorts_by_rolling_value(
        self, windowed_engine: SlayerQueryEngine,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "id:count", "name": "n"}],
            order=[{"column": "revenue:sum(window='90d')", "direction": "desc"}],
        )
        result = await windowed_engine.execute(query=query)
        months = [str(r[_key(r, "created_at")])[:7] for r in result.data]
        assert months == ["2024-03", "2024-02", "2024-01"], result.data

    async def test_order_only_windowed_is_stripped_from_response(
        self, windowed_engine: SlayerQueryEngine,
    ) -> None:
        """Law 2: the hidden windowed value drives the sort but never surfaces
        as a result column."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "id:count", "name": "n"}],
            order=[{"column": "revenue:sum(window='90d')", "direction": "desc"}],
        )
        result = await windowed_engine.execute(query=query)
        assert all("window" not in c and "90d" not in c for c in result.columns), result.columns
        for row in result.data:
            assert all("window" not in k and "90d" not in k for k in row), row

    async def test_order_only_windowed_top_n(
        self, windowed_engine: SlayerQueryEngine,
    ) -> None:
        """ASC + LIMIT 1 must return January: 300 is the unique minimum of the
        ROLLING values (300 / 600 / 900), so top-N is deterministic.

        This pins LIMIT interaction with a hidden order target; it is NOT
        independent B1 protection — under plain monthly sums January and
        February tie at 300, so a regression could return January by luck of
        the tie-break. ``test_order_only_windowed_region_grain_discriminates``
        and ``test_order_only_windowed_inside_composite`` carry that duty."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "id:count", "name": "n"}],
            order=[{"column": "revenue:sum(window='90d')", "direction": "asc"}],
            limit=1,
        )
        result = await windowed_engine.execute(query=query)
        assert result.row_count == 1, result.data
        assert str(result.data[0][_key(result.data[0], "created_at")])[:7] == "2024-01", result.data

    async def test_order_only_windowed_inside_composite(
        self, windowed_engine: SlayerQueryEngine,
    ) -> None:
        """S-b: a windowed measure inside an order-only composite.

        Runs against ``orders_c``, whose rows are shaped so that the correct
        answer is reachable ONLY by dividing the ROLLING sum — see the
        ``_COMPOSITE_ORDERS`` table above for the full derivation. Ordering by
        ``id:count / revenue:sum(window='90d')`` DESC must give Mar, Jan, Feb;
        each way of building the feature wrong gives a different order:

          rolling sum only    -> Feb, Jan, Mar
          plain sum only      -> Jan first
          count / PLAIN sum   -> Jan last
          divisor dropped     -> Jan first
        """
        query = SlayerQuery(
            source_model="orders_c",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "id:count", "name": "n"}],
            order=[{"column": "id:count / revenue:sum(window='90d')", "direction": "desc"}],
        )
        result = await windowed_engine.execute(query=query)
        months = [str(r[_key(r, "created_at")])[:7] for r in result.data]
        assert months == ["2024-03", "2024-01", "2024-02"], (
            f"ordering must use count / ROLLING sum.\ngot: {months}\n"
            f"rows: {result.data}"
        )
        assert all("window" not in c and "90d" not in c for c in result.columns), result.columns

    async def test_composite_dataset_rolling_values_are_as_derived(
        self, windowed_engine: SlayerQueryEngine,
    ) -> None:
        """Control for the test above: declare the same windowed measure so its
        ROLLING values are directly observable. If this drifts, the composite
        ordering expectation is derived from stale arithmetic rather than a real
        regression."""
        query = SlayerQuery(
            source_model="orders_c",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "roll"}],
        )
        result = await windowed_engine.execute(query=query)
        by_month = {
            str(r[_key(r, "created_at")])[:7]: float(r[_key(r, "roll")])
            for r in result.data
        }
        assert by_month["2024-01"] == 1020.0, by_month
        assert by_month["2024-02"] == 1021.0, by_month
        # March drops the 2024-01-01 row of 1000 (Jan 1 < the Jan 2 lower bound).
        assert by_month["2024-03"] == 22.0, by_month

    async def test_hidden_and_public_windowed_together(
        self, windowed_engine: SlayerQueryEngine,
    ) -> None:
        """A declared windowed measure plus a DIFFERENT order-only windowed
        one — two ``_wm_`` CTEs, one projected and one trimmed, both correct."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_90d"}],
            order=[{"column": "revenue:sum(window='30d')", "direction": "desc"}],
        )
        result = await windowed_engine.execute(query=query)
        by_month = {
            str(r[_key(r, "created_at")])[:7]: float(r[_key(r, "rev_90d")])
            for r in result.data
        }
        # The PUBLIC 90d values must be unaffected by the hidden 30d ordering.
        assert by_month["2024-01"] == 300.0, by_month
        assert by_month["2024-02"] == 600.0, by_month
        assert by_month["2024-03"] == 900.0, by_month
        assert all("30d" not in c for c in result.columns), result.columns

    async def test_order_only_windowed_region_grain_discriminates(
        self, windowed_engine: SlayerQueryEngine,
    ) -> None:
        """The ROLLING value drives the sort on a grain with a NULL dimension.

        This test used to discriminate through the NULL group's value being
        NULL: rolling gave ``US/Jan = 300`` and NULL elsewhere, while a plain
        ``SUM`` gave ``NULL/Mar = 400`` first, so the two orderings could not
        coincide. Now that the ``_src`` join-back is null-safe the NULL groups
        carry real rolling values (Feb 300, Mar 700), and on THIS seed the
        rolling and plain-sum orderings agree — so the ordering alone no longer
        proves the window survived.

        The discrimination is therefore structural as well: the ORDER BY must
        reference the windowed CTE's column. A plain-``SUM`` regression (the
        DEV-1733 defect, where the window was silently dropped) emits no ``_wm_``
        CTE at all, so it cannot satisfy that no matter how the rows tie.
        """
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="region")],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "id:count", "name": "n"}],
            order=[{"column": "revenue:sum(window='90d')", "direction": "desc"}],
        )
        sql = (await windowed_engine.execute(query=query, dry_run=True)).sql or ""
        assert "_wm_" in sql, (
            f"no windowed CTE was emitted — the window was dropped and the "
            f"ORDER BY is over a plain SUM:\n{sql}"
        )
        order_at = sql.rfind("ORDER BY")
        assert order_at != -1, sql
        assert "_wm_" in sql[order_at:], (
            f"the ORDER BY does not reference the windowed CTE:\n{sql}"
        )

        result = await windowed_engine.execute(query=query)
        regions = [r[_key(r, "region")] for r in result.data]
        assert len(regions) == 3, result.data
        # Rolling values: US/Jan 300, NULL/Feb 300, NULL/Mar 700 -> Mar leads.
        assert regions[0] is None, (
            f"the NULL/Mar group has the largest ROLLING value (700, its own "
            f"400 plus February's 300 inside the 90-day window), so it must "
            f"sort first. got: {regions}\nrows: {result.data}"
        )
        assert set(regions[1:]) == {"US", None}, result.data
        assert all("90d" not in c for c in result.columns), result.columns


# =========================================================================== #
# DEV-1732 — frame-bound equivalence VALUE tests.
#
# The acceptance criterion in its strongest form: a `date_range` and an
# equivalent explicit filter must produce IDENTICAL numbers, verified against
# real databases rather than SQL shape.
#
# Its own seed and storages (not the `_ORDERS` set above) so the extra rows
# these tests need cannot perturb the Stage-10 expectations.
#
# (id, amount, created_at, status)::
#
#     1: 2024-01-01  100  paid
#     2: 2024-01-15  200  paid
#     3: 2024-02-15  300  unpaid
#     4: 2024-03-10  400  paid
#     5: 2024-03-20  500  paid     <- AFTER a 2024-03-15 frame end, before
#                                     March's bucket_end (2024-04-01)
#
# Monthly buckets, window='90d'; 2024 is a leap year:
#
#     bucket_end(Mar) = 2024-04-01, lower = 2024-04-01 - 90d = 2024-01-02
#     March window [2024-01-02, 2024-04-01) holds rows 2,3,4,5 -> 1400
#     (row 1 at 2024-01-01 falls just outside the lower bound)
#     paid-only March window holds rows 2,4,5 -> 1100
#     February bucket sum = 300 (row 3) — the `time_shift` prev for March
# =========================================================================== #

_FB_ORDERS = [
    (1, 100.0, "2024-01-01 00:00:00", "paid"),
    (2, 200.0, "2024-01-15 00:00:00", "paid"),
    (3, 300.0, "2024-02-15 00:00:00", "unpaid"),
    (4, 400.0, "2024-03-10 00:00:00", "paid"),
    (5, 500.0, "2024-03-20 00:00:00", "paid"),
]

_MARCH_ONLY = ["2024-03-01", "2024-03-31"]
_MARCH_ONLY_FILTER = "created_at >= '2024-03-01' and created_at <= '2024-03-31'"
# Frame end mid-bucket: 2024-03-15 sits inside March, whose bucket_end is
# 2024-04-01 — so row 5 (2024-03-20) is past the caller's stated end but inside
# the trailing window.
_MID_BUCKET_END = ["2024-01-01", "2024-03-15"]
_MID_BUCKET_END_FILTER = "created_at >= '2024-01-01' and created_at <= '2024-03-15'"


def _fb_orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="fb",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            Column(name="status", sql="status", type=DataType.TEXT),
        ],
    )


@pytest.fixture(scope="module")
def _duckdb_fb_storage(tmp_path_factory):
    duckdb = pytest.importorskip("duckdb")
    tmp = tmp_path_factory.mktemp("fb_duckdb")
    db_path = tmp / "fb.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE orders (id INTEGER, amount DOUBLE, created_at TIMESTAMP, status VARCHAR)",
    )
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", _FB_ORDERS)
    conn.close()
    storage = YAMLStorage(base_dir=str(tmp / "storage"))
    run_sync(storage.save_datasource(
        DatasourceConfig(name="fb", type="duckdb", database=str(db_path)),
    ))
    run_sync(storage.save_model(_fb_orders_model()))
    return storage


@pytest.fixture(scope="module")
def _sqlite_fb_storage(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("fb_sqlite")
    db_path = tmp / "fb.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE orders (id INTEGER, amount REAL, created_at TIMESTAMP, status TEXT)",
    )
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", _FB_ORDERS)
    conn.commit()
    conn.close()
    storage = YAMLStorage(base_dir=str(tmp / "storage"))
    run_sync(storage.save_datasource(
        DatasourceConfig(name="fb", type="sqlite", database=str(db_path)),
    ))
    run_sync(storage.save_model(_fb_orders_model()))
    return storage


@pytest.fixture(params=["duckdb", "sqlite"])
def frame_bound_engine(request) -> SlayerQueryEngine:
    storage = request.getfixturevalue(f"_{request.param}_fb_storage")
    return SlayerQueryEngine(storage=storage)


def _by_month(result, suffix: str) -> dict:
    return {
        str(r[_key(r, "created_at")])[:7]: r[_key(r, suffix)]
        for r in result.data
    }


@pytest.mark.integration
class TestFrameBoundEquivalenceValues:
    """DEV-1732: the two spellings of a time frame must agree numerically."""

    async def _windowed_by_month(
        self, engine: SlayerQueryEngine, *, date_range=None, filters=None,
    ) -> dict:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=date_range,
            )],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_90d"}],
            filters=filters,
        )
        return _by_month(await engine.execute(query=query), "rev_90d")

    async def test_date_range_and_explicit_filter_agree(
        self, frame_bound_engine: SlayerQueryEngine,
    ) -> None:
        """The headline acceptance criterion. Before DEV-1732 the explicit
        spelling truncated ``_src`` to March, yielding 900 instead of 1400."""
        via_range = await self._windowed_by_month(
            frame_bound_engine, date_range=_MARCH_ONLY,
        )
        via_filter = await self._windowed_by_month(
            frame_bound_engine, filters=[_MARCH_ONLY_FILTER],
        )
        assert via_range == via_filter, (via_range, via_filter)
        assert float(via_range["2024-03"]) == 1400.0, via_range

    async def test_mixed_spelling_agrees_with_split_spelling(
        self, frame_bound_engine: SlayerQueryEngine,
    ) -> None:
        """``date_range`` + a population filter must equal the single combined
        filter string — i.e. the top-level ``and`` really is split, and the
        surviving ``status`` conjunct still constrains ``_src``.

        1100, not 1400: the unpaid February 300 is excluded by ``status``. A
        rule that dropped the WHOLE mixed predicate would return 1400 here.
        """
        via_range = await self._windowed_by_month(
            frame_bound_engine, date_range=_MARCH_ONLY, filters=["status = 'paid'"],
        )
        via_filter = await self._windowed_by_month(
            frame_bound_engine, filters=[f"{_MARCH_ONLY_FILTER} and status = 'paid'"],
        )
        assert via_range == via_filter, (via_range, via_filter)
        assert float(via_range["2024-03"]) == 1100.0, via_range

    async def test_upper_bound_widening_matches_date_range_mid_bucket(
        self, frame_bound_engine: SlayerQueryEngine,
    ) -> None:
        """The accepted upper-bound consequence, pinned by VALUE.

        The frame ends 2024-03-15, mid-March, but March's ``bucket_end`` is
        2024-04-01 — so the trailing window legitimately reaches row 5
        (2024-03-20), which is past the caller's stated end. That widening is
        exactly what ``date_range`` already does; DEV-1732's contract is that
        the explicit spelling does the same thing, not something safer.

        1400 (includes row 5), not 900 (the pre-DEV-1732 truncated answer).
        """
        via_range = await self._windowed_by_month(
            frame_bound_engine, date_range=_MID_BUCKET_END,
        )
        via_filter = await self._windowed_by_month(
            frame_bound_engine, filters=[_MID_BUCKET_END_FILTER],
        )
        assert via_range == via_filter, (via_range, via_filter)
        assert float(via_range["2024-03"]) == 1400.0, via_range

    async def test_time_shift_prev_agrees_across_spellings(
        self, frame_bound_engine: SlayerQueryEngine,
    ) -> None:
        """The same rule in the shifted CTE. Before DEV-1732 the explicit
        spelling truncated the shifted CTE to March rows, so March's ``prev``
        came back NULL while the ``date_range`` spelling gave February's 300."""
        async def _prev(*, date_range=None, filters=None):
            query = SlayerQuery(
                source_model="orders",
                time_dimensions=[TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=date_range,
                )],
                measures=[{
                    "formula": "time_shift(revenue:sum, -1, 'month')", "name": "prev",
                }],
                filters=filters,
            )
            return _by_month(await frame_bound_engine.execute(query=query), "prev")

        via_range = await _prev(date_range=_MARCH_ONLY)
        via_filter = await _prev(filters=[_MARCH_ONLY_FILTER])
        assert via_range == via_filter, (via_range, via_filter)
        assert via_range["2024-03"] is not None, via_range
        assert float(via_range["2024-03"]) == 300.0, via_range
