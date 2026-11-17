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
import re
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasourceConfig,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import FieldRef, OrderItem, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

from slayer.core.errors import DatasetFieldCycleError
from slayer.core.keys import FieldSqlKey
from slayer.engine.field_dependency import _filter_dependencies
from slayer.engine.population.filter_reachability import key_has_host_local_ref
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.field_expansion import expand_field_definition_parts_sync

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
    WEIGHTED_AVG_QAMT_WEIGHT_QTY,
    _SALES_ROWS,
    dev1832_datasets,
    gen,
    make_exec_engine,
    orders_q,
    rows_by,
    sales_dataset,
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
    models = dev1832_datasets()
    sales = next(m for m in models if m.name == "sales")
    sales.fields.append(
        DatasetField(name="q_double", datatype=DataType.DOUBLE, sql="q_amount * 2"))
    async for engine in make_exec_engine(request, datasets=models):
        yield engine


def _metric(expression: str, name: str = "m") -> DatasetMetric:
    return DatasetMetric(expression=expression, name=name)


async def _save_validated(dataset: SlayerDataset) -> None:
    """Save ``model`` with save-time validation on (raises on cycle / bad path)."""
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=os.path.join(d, "store"))
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite"))
        await storage.save_dataset(dataset, _validate=True)


async def _tsales_engine(rows: list[tuple]) -> SlayerQueryEngine:
    """A seeded SQLite engine over a timed ``tsales(id, product, amount, ts)``."""
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    with transaction(db_path) as con:
        con.execute("CREATE TABLE tsales (id INTEGER PRIMARY KEY, product TEXT, "
                    "amount REAL, ts TEXT)")
        con.executemany("INSERT INTO tsales VALUES (?,?,?,?)", rows)
    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=db_path))
    await storage.save_dataset(_tsales_dataset(), _validate=False)
    return SlayerQueryEngine(storage=storage)


def _tsales_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="tsales", sql_table="tsales", data_source="test",
        default_time_dimension="ts",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="product", datatype=DataType.TEXT),
            DatasetField(name="amount", datatype=DataType.DOUBLE),
            DatasetField(name="ts", datatype=DataType.TIMESTAMP),
            DatasetField(name="q_amount", datatype=DataType.DOUBLE, sql="amount",
                   filter="product = 'Q'"),
        ])


# --------------------------------------------------------------------------- #
# Single-column and ranked masked pick are unchanged (regression guards).
# --------------------------------------------------------------------------- #
class TestSingleColumnUnchanged:
    async def test_single_column_aggregate_sql_unchanged(self) -> None:
        sql = _norm(await gen(sales_q(measures=[_metric("sum(q_amount)")])))
        assert "SUM(CASE WHEN" in sql.upper().replace("SUM (", "SUM("), sql
        assert "'Q'" in sql
        assert "amount" in sql

    async def test_single_column_value(self, exec_backend) -> None:
        resp = await exec_backend.execute(sales_q(measures=[_metric("sum(q_amount)")]))
        assert float(resp.data[0]["sales.m"]) == pytest.approx(QAMT_SUM)

    async def test_ranked_over_filtered_picks_masked_value_sql(self) -> None:
        # q_amount:last needs a ranking time column; the masked value is picked.
        sql = await gen(
            SlayerQuery(population="tsales", metrics=[_metric("last(q_amount)")]),
            datasets=[_tsales_dataset()])
        assert "CASE WHEN" in sql.upper(), sql
        assert "'Q'" in sql, sql

    async def test_ranked_over_filtered_picks_masked_value_exec(self) -> None:
        # Latest row (Jun) is non-Q, so the masked pick is NULL — never the raw 999.
        engine = await _tsales_engine([
            (1, "Q", 100.0, "2024-01-01"),
            (2, "Q", 70.0, "2024-03-01"),
            (3, "P", 999.0, "2024-06-01"),
        ])
        resp = await engine.execute(
            SlayerQuery(population="tsales", metrics=[_metric("last(q_amount)")]))
        assert resp.data[0]["tsales.m"] is None

    @pytest.mark.parametrize("position", ["measure", "dimension", "filter", "last"])
    async def test_filtered_physical_column_masked_once(self, position: str) -> None:
        # A filtered column with no sql (or sql == name) wraps exactly one CASE WHEN.
        model = _tsales_dataset()
        model.fields.append(
            DatasetField(name="amt", datatype=DataType.DOUBLE, filter="product = 'Q'"))
        query = {
            "measure": SlayerQuery(population="tsales", metrics=[_metric("sum(amt)")]),
            "dimension": SlayerQuery(
                population="tsales", dimensions=[FieldRef(name="amt")],
                metrics=[_metric("count(id)")]),
            "filter": SlayerQuery(
                population="tsales", metrics=[_metric("count(id)")], filter=["amt > 0"]),
            "last": SlayerQuery(population="tsales", metrics=[_metric("last(amt)")]),
        }[position]
        sql = await gen(query, datasets=[model])
        expected = 2 if position == "dimension" else 1  # projection + GROUP BY
        masks = re.findall(r"CASE\s+WHEN\s+tsales\.product = 'Q'", sql, flags=re.I)
        assert len(masks) == expected, sql

    async def test_ranked_over_filtered_picks_masked_value_exec_positive(self) -> None:
        # Latest row (Jun) IS a Q row, so the masked pick is its value — 70, never 999.
        engine = await _tsales_engine([
            (1, "P", 999.0, "2024-01-01"),
            (2, "Q", 70.0, "2024-06-01"),
        ])
        resp = await engine.execute(
            SlayerQuery(population="tsales", metrics=[_metric("last(q_amount)")]))
        assert float(resp.data[0]["tsales.m"]) == pytest.approx(70.0)


# --------------------------------------------------------------------------- #
# The three BREAKING behaviour changes (fail-without-fix).
# --------------------------------------------------------------------------- #
class TestBehaviourChanges:
    async def test_parameters_not_masked_by_source_filter(self, exec_backend) -> None:
        """#1: the custom wavg(q_amount, weight=quantity) masks the value, not the
        weight — the former filter-everything form is now wrong. The built-in
        weighted_avg skips the masked (NULL) values' weights, like any NULL value."""
        resp = await exec_backend.execute(
            sales_q(measures=[_metric("wavg(q_amount, weight=quantity)")]))
        got = float(resp.data[0]["sales.m"])
        assert got == pytest.approx(WAVG_QAMT_WEIGHT_QTY)
        assert got != pytest.approx(WAVG_QAMT_WEIGHT_QTY_WRONG)
        builtin = await exec_backend.execute(
            sales_q(measures=[_metric("weighted_avg(q_amount, weight=quantity)")]))
        assert float(builtin.data[0]["sales.m"]) == pytest.approx(WEIGHTED_AVG_QAMT_WEIGHT_QTY)

    async def test_filtered_column_used_as_parameter_is_masked(self, exec_backend) -> None:
        """#2: weighted_avg(amount, weight=q_amount) masks the weight (NULL on
        non-Q rows), not the former silently-unfiltered weight."""
        resp = await exec_backend.execute(
            sales_q(measures=[_metric("weighted_avg(amount, weight=q_amount)")]))
        assert float(resp.data[0]["sales.m"]) == pytest.approx(WAVG_AMOUNT_WEIGHT_QAMT)

    async def test_filtered_column_as_dimension_groups_non_matching_under_null(
        self, exec_backend,
    ) -> None:
        """#3: grouping by q_amount reads the masked value — non-Q rows form the
        NULL group, each Q amount its own cell."""
        resp = await exec_backend.execute(
            sales_q(dimensions=["q_amount"], measures=[_metric("count(*)", "n")]))
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
            sales_q(measures=[_metric("sum(amount)")], filters=["q_amount > 10"]))
        assert float(resp.data[0]["sales.m"]) == pytest.approx(expected)

    async def test_order_reads_masked_value(self, exec_backend) -> None:
        query = sales_q(
            dimensions=["q_amount"], measures=[_metric("sum(amount)")],
            order=[OrderItem(field=FieldRef(name="q_amount"), direction="asc")])
        resp = await exec_backend.execute(query)
        ordered = [row["sales.q_amount"] for row in resp.data]
        # Only the masked Q amounts are cells, in ascending order; non-Q rows
        # collapse into the single NULL cell (its position is dialect-default).
        assert [v for v in ordered if v is not None] == [10.0, 60.0, 80.0, 100.0]
        assert None in ordered
        sql = await gen(query)
        assert "CASE WHEN" in sql.upper(), sql
        assert "'Q'" in sql, sql

    async def test_partition_key_reads_masked_value(self, exec_backend) -> None:
        query = sales_q(
            dimensions=["q_amount"],
            measures=[_metric("sum(amount, partition_by=q_amount)")])
        resp = await exec_backend.execute(query)
        cells = {row["sales.q_amount"] for row in resp.data}
        assert cells == set(COUNT_BY_QAMOUNT)  # one cell per masked value, incl. NULL
        sql = await gen(query)
        assert "CASE WHEN" in sql.upper(), sql
        assert "'Q'" in sql, sql


# --------------------------------------------------------------------------- #
# Filtered leaf inside an aggregated expression.
# --------------------------------------------------------------------------- #
class TestFilteredLeafInExpression:
    async def test_sum_over_filtered_leaf_expression(self, exec_backend) -> None:
        resp = await exec_backend.execute(
            sales_q(measures=[_metric("sum(q_amount - 1)")]))
        assert float(resp.data[0]["sales.m"]) == pytest.approx(SUM_QAMT_MINUS_1)

    async def test_count_over_filtered_leaf_expression(self, exec_backend) -> None:
        resp = await exec_backend.execute(
            sales_q(measures=[_metric("count(q_amount - 1)")]))
        assert int(resp.data[0]["sales.m"]) == COUNT_QAMT_MINUS_1


# --------------------------------------------------------------------------- #
# Filtered leaf on a joined model contributes its filter's crossings.
# --------------------------------------------------------------------------- #
class TestFilteredLeafOnJoinedModel:
    async def test_filtered_joined_leaf_expression(self, exec_backend) -> None:
        resp = await exec_backend.execute(orders_q(
            dimensions=["status"],
            measures=[_metric("sum(amount - customers.north_spend)")]))
        got = {s: float(v["orders.m"]) for s, v in status_key(resp).items()}
        for status, expected in NORTH_SPEND_EXPR_BY_STATUS.items():
            assert got[status] == pytest.approx(expected)

    async def test_fanning_filter_leaf_fails_closed(self) -> None:
        # bad_pop_spend = spend filtered by regions.bad_pop > 0, crossing the
        # fanning regions→region_events hop.
        query = orders_q(measures=[_metric("sum(amount - customers.bad_pop_spend)")])
        with pytest.raises(ValueError, match="unproven or fanning join hop") as ei:
            await gen(query)
        assert "Aggregate the target column directly" in str(ei.value)

    async def test_fanning_filter_default_parameter_fails_closed(self) -> None:
        # A filtered PHYSICAL column (no sql) whose filter crosses the fanning hop,
        # used as a definition default: its crossings close like a derived one's.
        models = dev1832_datasets()
        cust = next(m for m in models if m.name == "customers")
        cust.fields.append(
            DatasetField(name="bad_pop_w", datatype=DataType.DOUBLE, filter="regions.bad_pop > 0"))
        cust.aggregations.append(Aggregation(
            name="wsum_badw", expression="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="bad_pop_w")]))
        query = orders_q(measures=[_metric("wsum_badw(customers.spend)")])
        with pytest.raises(ValueError, match="unproven join hop") as ei:
            await gen(query, datasets=models)
        assert "region_events" in str(ei.value)


# --------------------------------------------------------------------------- #
# Host-locality reaches into Column.filter, not just Column.sql (DEV-1832 / Codex).
# --------------------------------------------------------------------------- #
class TestHostLocalFilterReachability:
    """A host-declared derived column whose VALUE crosses into a joined model but
    whose ``Column.filter`` reads a host-local column is host-local: its mask
    must stay at the host, where that column is bound. Host-locality must consult
    the filter fragment, not only the value (which the crossed set already
    covers)."""

    def _orders_bundle(self, *, sql: str | None, filter_: str):
        models = dev1832_datasets()
        orders = next(m for m in models if m.name == "orders")
        orders.fields.append(DatasetField(
            name="probe", datatype=DataType.DOUBLE, sql=sql, filter=filter_))
        b = ResolvedSourceBundle(
            dialect="sqlite",
            population=orders,
            referenced_datasets=[m for m in models if m.name != "orders"])
        key = FieldSqlKey(path=(), dataset="orders", field_name="probe")
        return orders, key, b

    def test_cross_model_value_with_host_local_filter_is_host_local(self) -> None:
        # value crosses orders → customers (to-one); filter reads host orders.amount.
        orders, key, b = self._orders_bundle(
            sql="customers.discount", filter_="amount > 5")
        assert key_has_host_local_ref(
            key=key, anchor_dataset=orders, anchor_relation="orders",
            bundle=b, cache={}) is True

    def test_cross_model_value_and_cross_model_filter_is_not_host_local(self) -> None:
        # Control: both fragments cross to customers — nothing anchors at the host.
        orders, key, b = self._orders_bundle(
            sql="customers.discount", filter_="customers.tier = 'gold'")
        assert key_has_host_local_ref(
            key=key, anchor_dataset=orders, anchor_relation="orders",
            bundle=b, cache={}) is False

    def test_filtered_physical_column_with_cross_model_filter_is_host_local(
        self,
    ) -> None:
        # A column with NO sql masks its own physical value (host-local); a
        # cross-model filter must NOT strip that host-locality (Codex edge case).
        orders, key, b = self._orders_bundle(
            sql=None, filter_="customers.tier = 'gold'")
        assert key_has_host_local_ref(
            key=key, anchor_dataset=orders, anchor_relation="orders",
            bundle=b, cache={}) is True


# --------------------------------------------------------------------------- #
# Derived column over a filtered column expands the wrapped value.
# --------------------------------------------------------------------------- #
class TestDerivedOverFiltered:
    async def test_derived_column_expands_wrapped_value_sql(self) -> None:
        model = sales_dataset()
        model.fields.append(
            DatasetField(name="q_double", datatype=DataType.DOUBLE, sql="q_amount * 2"))
        sql = await gen(sales_q(measures=[_metric("sum(q_double)")]), datasets=[model])
        assert "CASE WHEN" in sql.upper(), sql
        assert "'Q'" in sql, sql

    async def test_derived_column_expands_wrapped_value_exec(self, exec_qdouble) -> None:
        # q_double = q_amount * 2, so sum over Q rows = 2 * QAMT_SUM; a wrong
        # expansion (raw amount) would sum every row and miss this value.
        resp = await exec_qdouble.execute(sales_q(measures=[_metric("sum(q_double)")]))
        assert float(resp.data[0]["sales.m"]) == pytest.approx(2 * QAMT_SUM)


# --------------------------------------------------------------------------- #
# Variables substitute into both fields.
# --------------------------------------------------------------------------- #
class TestVariableSubstitution:
    async def test_variables_substitute_into_sql_and_filter(self) -> None:
        model = sales_dataset()
        model.fields.append(DatasetField(
            name="vcol", datatype=DataType.DOUBLE, sql="amount * {mult}",
            filter="product = '{prod}'"))
        sql = await gen(
            sales_q(measures=[_metric("sum(vcol)")], variables={"mult": 2, "prod": "Q"}),
            datasets=[model])
        assert "{" not in sql, sql
        assert "'Q'" in sql, sql
        assert "2" in sql, sql


# --------------------------------------------------------------------------- #
# Save-time cycle and path validation.
# --------------------------------------------------------------------------- #
class TestCycleAndPathValidation:
    async def test_filtered_columns_pass_cycle_validation(self) -> None:
        # A filtered physical column and a filtered derived column both save —
        # the wrapped value's reference to its own physical column is not a cycle.
        await _save_validated(SlayerDataset(
            name="fcyc", sql_table="sales", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="product", datatype=DataType.TEXT),
                DatasetField(name="amount", datatype=DataType.DOUBLE, filter="product = 'Q'"),
                DatasetField(name="der", datatype=DataType.DOUBLE, sql="amount",
                       filter="product = 'Q'"),
            ]))

    async def test_filter_naming_another_derived_is_a_legal_edge(self) -> None:
        # der2's filter names der1 (another derived column): a dependency EDGE,
        # not a cycle — the model saves.
        await _save_validated(SlayerDataset(
            name="fedge", sql_table="sales", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="product", datatype=DataType.TEXT),
                DatasetField(name="amount", datatype=DataType.DOUBLE),
                DatasetField(name="der1", datatype=DataType.DOUBLE, sql="amount"),
                DatasetField(name="der2", datatype=DataType.DOUBLE, sql="amount",
                       filter="der1 > 0"),
            ]))

    async def test_filter_naming_own_column_is_a_cycle(self) -> None:
        model = SlayerDataset(
            name="floop", sql_table="sales", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="amount", datatype=DataType.DOUBLE),
                DatasetField(name="loop", datatype=DataType.DOUBLE, sql="amount",
                       filter="loop > 0"),
            ])
        with pytest.raises(ValueError, match="(?i)circular|cycle"):
            await _save_validated(model)

    async def test_filter_naming_own_physical_column_is_not_a_cycle(self) -> None:
        # A filter naming its own column reads the PHYSICAL column when that column
        # is a bare physical one (``val`` → ``val``), not the recursive masked
        # value — so it is not a cycle (contrast the DERIVED ``loop`` above).
        await _save_validated(SlayerDataset(
            name="fself", sql_table="sales", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="val", datatype=DataType.DOUBLE, sql="val",
                       filter="val > 0"),
            ]))

    async def test_query_over_self_filtered_physical_column_reads_physical(self) -> None:
        # Companion to the save test above: a query over the same column must also
        # expand — its filter's self-reference reads the physical column, so exactly
        # one CASE WHEN and no ColumnCycleError (CR: physical-read exemption).
        model = SlayerDataset(
            name="fself", sql_table="fself", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="val", datatype=DataType.DOUBLE, sql="val", filter="val > 0"),
            ])
        sql = await gen(
            SlayerQuery(population="fself", metrics=[_metric("sum(val)")]),
            datasets=[model])
        masks = re.findall(r"CASE\s+WHEN\s+fself\.val > 0", sql, flags=re.I)
        assert len(masks) == 1, sql

    def test_cross_column_filter_cycle_still_raises_in_expansion(self) -> None:
        # The physical-read exemption is LOCAL to a column's own filter fragment,
        # never inherited into a referenced column's definition — so column
        # expansion's own cycle-safety still fires on a genuine cross-column cycle
        # (x.filter→y, y.sql→x), defence-in-depth below elaboration (Codex leak).
        model = SlayerDataset(
            name="cyc", sql_table="cyc", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="x", datatype=DataType.DOUBLE, sql="x", filter="y > 0"),
                DatasetField(name="y", datatype=DataType.DOUBLE, sql="x")])
        x_col = model.get_field("x")
        assert x_col is not None
        with pytest.raises(DatasetFieldCycleError):
            expand_field_definition_parts_sync(
                field=x_col, dataset=model, alias_path="cyc",
                datasets_by_name={"cyc": model}, dialect="sqlite")

    async def test_mutual_filter_reference_is_a_cycle(self) -> None:
        # der_a's filter names der_b and vice versa → a 2-node cycle.
        model = SlayerDataset(
            name="fmutual", sql_table="sales", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="amount", datatype=DataType.DOUBLE),
                DatasetField(name="der_a", datatype=DataType.DOUBLE, sql="amount",
                       filter="der_b > 0"),
                DatasetField(name="der_b", datatype=DataType.DOUBLE, sql="amount",
                       filter="der_a > 0"),
            ])
        with pytest.raises(ValueError, match="(?i)circular|cycle"):
            await _save_validated(model)

    async def test_broken_filter_path_fails_at_save(self) -> None:
        model = SlayerDataset(
            name="fbad", sql_table="sales", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="amount", datatype=DataType.DOUBLE),
                DatasetField(name="bad", datatype=DataType.DOUBLE, sql="amount",
                       filter="ghost.col = 1"),
            ])
        with pytest.raises(ValueError) as ei:
            await _save_validated(model)
        assert "ghost" in str(ei.value).lower()

    def test_filter_to_known_but_unloaded_peer_is_skipped(self) -> None:
        # A reverse-direction join declared on the peer is invisible when the peer
        # failed to load; the filter is unproven, not broken — skip, never raise (CR).
        host = SlayerDataset(
            name="host", data_source="test", sql_table="host",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="masked", datatype=DataType.DOUBLE, sql="id",
                       filter="peer.flag = 1")])
        col = host.get_field("masked")
        assert col is not None
        deps = _filter_dependencies(
            field=col, host=host, reachable={"host": host},
            known_but_unloaded=frozenset({"peer"}))
        assert deps == []  # skipped — no spurious ValueError

    def test_filter_to_genuinely_unknown_token_still_raises(self) -> None:
        # The same reference with the token NOT known to the datasource is a broken
        # path and still fails, naming the token (the ghost case is preserved).
        host = SlayerDataset(
            name="host", data_source="test", sql_table="host",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="masked", datatype=DataType.DOUBLE, sql="id",
                       filter="peer.flag = 1")])
        col = host.get_field("masked")
        assert col is not None
        reachable = {"host": host}
        none_known: frozenset[str] = frozenset()
        with pytest.raises(ValueError, match="does not"):
            _filter_dependencies(
                field=col, host=host, reachable=reachable,
                known_but_unloaded=none_known)
