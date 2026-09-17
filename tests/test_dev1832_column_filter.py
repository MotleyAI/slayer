"""DEV-1832 task 1.5 — ``Column.filter`` is a conditional derived definition.

Every ``#### Scenario`` of ``models/column-filters`` › *Column.filter is a
conditional derived definition*, plus the three BREAKING behaviour changes as
explicit fail-without-fix tests: an aggregation's parameters are no longer
masked by the source column's filter (#1); a filtered column used as a parameter
now IS masked (#2); a filtered column in dimension / filter / order / partition
position now reads the masked value (#3). Single-column aggregate SQL and the
ranked masked-pick are unchanged (regression guards).
"""

from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _norm
from tests._dev1832_fixtures import (
    COUNT_BY_QAMOUNT,
    COUNT_QAMT_MINUS_1,
    NORTH_SPEND_EXPR_BY_STATUS,
    QAMT_SUM,
    SUM_QAMT_MINUS_1,
    WAVG_AMOUNT_WEIGHT_QAMT,
    WAVG_QAMT_WEIGHT_QTY,
    WAVG_QAMT_WEIGHT_QTY_WRONG,
    _SALES_ROWS,
    dev1832_models,
    gen,
    make_exec_engine,
    orders_q,
    rows_by,
    sales_model,
    sales_q,
    status_key,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_qdouble(request):
    """Exec engine whose ``sales`` carries a derived-over-filtered ``q_double``."""
    models = dev1832_models()
    sales = next(m for m in models if m.name == "sales")
    sales.columns.append(
        Column(name="q_double", type=DataType.DOUBLE, sql="q_amount * 2"))
    async for engine in make_exec_engine(request, models=models):
        yield engine


def _measure(formula: str, name: str = "m") -> ModelMeasure:
    return ModelMeasure(formula=formula, name=name)


async def _save_validated(model: SlayerModel) -> None:
    """Save ``model`` with save-time validation on (raises on cycle / bad path)."""
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=os.path.join(d, "store"))
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite"))
        await storage.save_model(model, _validate=True)


async def _tsales_engine(rows: list[tuple]) -> SlayerQueryEngine:
    """A seeded SQLite engine over a timed ``tsales(id, product, amount, ts)``."""
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    con.execute("CREATE TABLE tsales (id INTEGER PRIMARY KEY, product TEXT, "
                "amount REAL, ts TEXT)")
    con.executemany("INSERT INTO tsales VALUES (?,?,?,?)", rows)
    con.commit()
    con.close()
    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=db_path))
    await storage.save_model(_tsales_model(), _validate=False)
    return SlayerQueryEngine(storage=storage)


def _tsales_model() -> SlayerModel:
    return SlayerModel(
        name="tsales", sql_table="tsales", data_source="test",
        default_time_dimension="ts",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="product", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="ts", type=DataType.TIMESTAMP),
            Column(name="q_amount", type=DataType.DOUBLE, sql="amount",
                   filter="product = 'Q'"),
        ])


# --------------------------------------------------------------------------- #
# Single-column and ranked masked pick are unchanged (regression guards).
# --------------------------------------------------------------------------- #
class TestSingleColumnUnchanged:
    async def test_single_column_aggregate_sql_unchanged(self) -> None:
        sql = _norm(await gen(sales_q(measures=[_measure("q_amount:sum")])))
        assert "SUM(CASE WHEN" in sql.upper().replace("SUM (", "SUM("), sql
        assert "'Q'" in sql and "amount" in sql

    async def test_single_column_value(self, exec_backend) -> None:
        resp = await exec_backend.execute(sales_q(measures=[_measure("q_amount:sum")]))
        assert float(resp.data[0]["sales.m"]) == pytest.approx(QAMT_SUM)

    async def test_ranked_over_filtered_picks_masked_value_sql(self) -> None:
        # q_amount:last needs a ranking time column; the masked value is picked.
        sql = await gen(
            SlayerQuery(source_model="tsales", measures=[_measure("q_amount:last")]),
            models=[_tsales_model()])
        assert "CASE WHEN" in sql.upper() and "'Q'" in sql, sql

    async def test_ranked_over_filtered_picks_masked_value_exec(self) -> None:
        # Latest row (Jun) is non-Q, so the masked pick is NULL — never the raw 999.
        engine = await _tsales_engine([
            (1, "Q", 100.0, "2024-01-01"),
            (2, "Q", 70.0, "2024-03-01"),
            (3, "P", 999.0, "2024-06-01"),
        ])
        resp = await engine.execute(
            SlayerQuery(source_model="tsales", measures=[_measure("q_amount:last")]))
        assert resp.data[0]["tsales.m"] is None


# --------------------------------------------------------------------------- #
# The three BREAKING behaviour changes (fail-without-fix).
# --------------------------------------------------------------------------- #
class TestBehaviourChanges:
    async def test_parameters_not_masked_by_source_filter(self, exec_backend) -> None:
        """#1: weighted_avg(q_amount, weight=quantity) masks the value, not the
        weight — the former filter-everything form is now wrong."""
        resp = await exec_backend.execute(
            sales_q(measures=[_measure("weighted_avg(q_amount, weight=quantity)")]))
        got = float(resp.data[0]["sales.m"])
        assert got == pytest.approx(WAVG_QAMT_WEIGHT_QTY)
        assert got != pytest.approx(WAVG_QAMT_WEIGHT_QTY_WRONG)

    async def test_filtered_column_used_as_parameter_is_masked(self, exec_backend) -> None:
        """#2: weighted_avg(amount, weight=q_amount) masks the weight (NULL on
        non-Q rows), not the former silently-unfiltered weight."""
        resp = await exec_backend.execute(
            sales_q(measures=[_measure("weighted_avg(amount, weight=q_amount)")]))
        assert float(resp.data[0]["sales.m"]) == pytest.approx(WAVG_AMOUNT_WEIGHT_QAMT)

    async def test_filtered_column_as_dimension_groups_non_matching_under_null(
        self, exec_backend,
    ) -> None:
        """#3: grouping by q_amount reads the masked value — non-Q rows form the
        NULL group, each Q amount its own cell."""
        resp = await exec_backend.execute(
            sales_q(dimensions=["q_amount"], measures=[_measure("*:count", "n")]))
        counts = {k[0]: int(v["sales.n"]) for k, v in rows_by(resp, "sales.q_amount").items()}
        assert counts == COUNT_BY_QAMOUNT


# --------------------------------------------------------------------------- #
# Masked value in filter / order / partition positions (#3, other positions).
# --------------------------------------------------------------------------- #
class TestFilteredColumnPositions:
    async def test_row_filter_reads_masked_value(self, exec_backend) -> None:
        expected = sum(r[4] for r in _SALES_ROWS
                       if r[3] == "Q" and r[4] is not None and r[4] > 10)
        resp = await exec_backend.execute(
            sales_q(measures=[_measure("amount:sum")], filters=["q_amount > 10"]))
        assert float(resp.data[0]["sales.m"]) == pytest.approx(expected)

    async def test_order_reads_masked_value(self, exec_backend) -> None:
        query = sales_q(
            dimensions=["q_amount"], measures=[_measure("amount:sum")],
            order=[OrderItem(column=ColumnRef(name="q_amount"), direction="asc")])
        resp = await exec_backend.execute(query)
        ordered = [row["sales.q_amount"] for row in resp.data]
        # Only the masked Q amounts are cells, in ascending order; non-Q rows
        # collapse into the single NULL cell (its position is dialect-default).
        assert [v for v in ordered if v is not None] == [10.0, 60.0, 80.0, 100.0]
        assert None in ordered
        sql = await gen(query)
        assert "CASE WHEN" in sql.upper() and "'Q'" in sql, sql

    async def test_partition_key_reads_masked_value(self, exec_backend) -> None:
        query = sales_q(
            dimensions=["q_amount"],
            measures=[_measure("amount:sum(partition_by=q_amount)")])
        resp = await exec_backend.execute(query)
        cells = {row["sales.q_amount"] for row in resp.data}
        assert cells == set(COUNT_BY_QAMOUNT)  # one cell per masked value, incl. NULL
        sql = await gen(query)
        assert "CASE WHEN" in sql.upper() and "'Q'" in sql, sql


# --------------------------------------------------------------------------- #
# Filtered leaf inside an aggregated expression.
# --------------------------------------------------------------------------- #
class TestFilteredLeafInExpression:
    async def test_sum_over_filtered_leaf_expression(self, exec_backend) -> None:
        resp = await exec_backend.execute(
            sales_q(measures=[_measure("sum(q_amount - 1)")]))
        assert float(resp.data[0]["sales.m"]) == pytest.approx(SUM_QAMT_MINUS_1)

    async def test_count_over_filtered_leaf_expression(self, exec_backend) -> None:
        resp = await exec_backend.execute(
            sales_q(measures=[_measure("count(q_amount - 1)")]))
        assert int(resp.data[0]["sales.m"]) == COUNT_QAMT_MINUS_1


# --------------------------------------------------------------------------- #
# Filtered leaf on a joined model contributes its filter's crossings.
# --------------------------------------------------------------------------- #
class TestFilteredLeafOnJoinedModel:
    async def test_filtered_joined_leaf_expression(self, exec_backend) -> None:
        resp = await exec_backend.execute(orders_q(
            dimensions=["status"],
            measures=[_measure("sum(amount - customers.north_spend)")]))
        got = {s: float(v["orders.m"]) for s, v in status_key(resp).items()}
        for status, expected in NORTH_SPEND_EXPR_BY_STATUS.items():
            assert got[status] == pytest.approx(expected)

    async def test_fanning_filter_leaf_fails_closed(self) -> None:
        # bad_pop_spend = spend filtered by regions.bad_pop > 0, crossing the
        # fanning regions→region_events hop.
        with pytest.raises(ValueError, match="unproven join hop"):
            await gen(orders_q(
                measures=[_measure("sum(amount - customers.bad_pop_spend)")]))


# --------------------------------------------------------------------------- #
# Derived column over a filtered column expands the wrapped value.
# --------------------------------------------------------------------------- #
class TestDerivedOverFiltered:
    async def test_derived_column_expands_wrapped_value_sql(self) -> None:
        model = sales_model()
        model.columns.append(
            Column(name="q_double", type=DataType.DOUBLE, sql="q_amount * 2"))
        sql = await gen(sales_q(measures=[_measure("q_double:sum")]), models=[model])
        assert "CASE WHEN" in sql.upper() and "'Q'" in sql, sql

    async def test_derived_column_expands_wrapped_value_exec(self, exec_qdouble) -> None:
        # q_double = q_amount * 2, so sum over Q rows = 2 * QAMT_SUM; a wrong
        # expansion (raw amount) would sum every row and miss this value.
        resp = await exec_qdouble.execute(sales_q(measures=[_measure("q_double:sum")]))
        assert float(resp.data[0]["sales.m"]) == pytest.approx(2 * QAMT_SUM)


# --------------------------------------------------------------------------- #
# Variables substitute into both fields.
# --------------------------------------------------------------------------- #
class TestVariableSubstitution:
    async def test_variables_substitute_into_sql_and_filter(self) -> None:
        model = sales_model()
        model.columns.append(Column(
            name="vcol", type=DataType.DOUBLE, sql="amount * {mult}",
            filter="product = '{prod}'"))
        sql = await gen(
            sales_q(measures=[_measure("vcol:sum")], variables={"mult": 2, "prod": "Q"}),
            models=[model])
        assert "{" not in sql, sql
        assert "'Q'" in sql and "2" in sql, sql


# --------------------------------------------------------------------------- #
# Save-time cycle and path validation.
# --------------------------------------------------------------------------- #
class TestCycleAndPathValidation:
    async def test_filtered_columns_pass_cycle_validation(self) -> None:
        # A filtered physical column and a filtered derived column both save —
        # the wrapped value's reference to its own physical column is not a cycle.
        await _save_validated(SlayerModel(
            name="fcyc", sql_table="sales", data_source="test",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="product", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE, filter="product = 'Q'"),
                Column(name="der", type=DataType.DOUBLE, sql="amount",
                       filter="product = 'Q'"),
            ]))

    async def test_filter_naming_another_derived_is_a_legal_edge(self) -> None:
        # der2's filter names der1 (another derived column): a dependency EDGE,
        # not a cycle — the model saves.
        await _save_validated(SlayerModel(
            name="fedge", sql_table="sales", data_source="test",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="product", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="der1", type=DataType.DOUBLE, sql="amount"),
                Column(name="der2", type=DataType.DOUBLE, sql="amount",
                       filter="der1 > 0"),
            ]))

    async def test_filter_naming_own_column_is_a_cycle(self) -> None:
        with pytest.raises(ValueError, match="(?i)circular|cycle"):
            await _save_validated(SlayerModel(
                name="floop", sql_table="sales", data_source="test",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="amount", type=DataType.DOUBLE),
                    Column(name="loop", type=DataType.DOUBLE, sql="amount",
                           filter="loop > 0"),
                ]))

    async def test_filter_naming_own_physical_column_is_not_a_cycle(self) -> None:
        # A filter naming its own column reads the PHYSICAL column when that column
        # is a bare physical one (``val`` → ``val``), not the recursive masked
        # value — so it is not a cycle (contrast the DERIVED ``loop`` above).
        await _save_validated(SlayerModel(
            name="fself", sql_table="sales", data_source="test",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="val", type=DataType.DOUBLE, sql="val",
                       filter="val > 0"),
            ]))

    async def test_mutual_filter_reference_is_a_cycle(self) -> None:
        # der_a's filter names der_b and vice versa → a 2-node cycle.
        with pytest.raises(ValueError, match="(?i)circular|cycle"):
            await _save_validated(SlayerModel(
                name="fmutual", sql_table="sales", data_source="test",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="amount", type=DataType.DOUBLE),
                    Column(name="der_a", type=DataType.DOUBLE, sql="amount",
                           filter="der_b > 0"),
                    Column(name="der_b", type=DataType.DOUBLE, sql="amount",
                           filter="der_a > 0"),
                ]))

    async def test_broken_filter_path_fails_at_save(self) -> None:
        with pytest.raises(ValueError) as ei:
            await _save_validated(SlayerModel(
                name="fbad", sql_table="sales", data_source="test",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="amount", type=DataType.DOUBLE),
                    Column(name="bad", type=DataType.DOUBLE, sql="amount",
                           filter="ghost.col = 1"),
                ]))
        assert "ghost" in str(ei.value).lower()
