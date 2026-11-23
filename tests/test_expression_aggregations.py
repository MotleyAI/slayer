"""Same-dataset expression aggregation — ``sum(amount - cost)``.

Aggregating a row-level scalar expression built from bare host-dataset fields,
scalar-allowlist calls, arithmetic, and literals; deterministic auto-naming via
the shared computed-dimension sanitizer; loud errors for the unsupported shapes
(cross-dataset refs, filtered-field operands, nested aggregations/transforms);
per-field gates advisory for expressions.
"""

from __future__ import annotations

import re
import tempfile

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.format import NumberFormatType
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    FieldKey,
    Grain,
    reroot_value_key,
    substitute_value_keys,
    walk_value_keys,
)
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import OrderItem, QueryMetric, SlayerQuery, TimeDimension
from slayer.core.scope import DatasetScope
from slayer.engine.binding import bind_expr
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.core.syntax import AggCall, canonical_metric_text, parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _norm


# ===========================================================================
# Dataset + harness.
# ===========================================================================


def _orders(**overrides) -> SlayerDataset:
    fields = dict(
        name="orders",
        data_source="test",
        sql_table="orders",
        default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="region", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="cost", datatype=DataType.FLOAT),
            DatasetField(name="price", datatype=DataType.FLOAT),
            DatasetField(name="tax", datatype=DataType.FLOAT),
            DatasetField(
                name="quantity",
                datatype=DataType.FLOAT,
                allowed_aggregations=["min", "max"],
            ),
            DatasetField(name="name", datatype=DataType.STRING),
            DatasetField(name="email", datatype=DataType.STRING),
            DatasetField(name="net", expression="amount - tax", datatype=DataType.FLOAT),
            DatasetField(
                name="ok_amount",
                expression="amount",
                datatype=DataType.FLOAT,
                filter="status = 'ok'",
            ),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
        ],
        aggregations=[Aggregation(name="my_agg", expression="AVG({value})")],
    )
    fields.update(overrides)
    return SlayerDataset.model_validate(fields)


def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers",
        data_source="test",
        sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="discount", datatype=DataType.FLOAT),
        ],
    )


async def _dry(query, *, dataset: SlayerDataset | None = None):
    dataset = dataset or _orders()
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(
            DatasourceConfig(name=dataset.data_source, type="postgres")
        )
        await storage.save_dataset(dataset)
        await storage.save_dataset(_customers())
        engine = SlayerQueryEngine(storage=storage)
        return await engine.execute(query, dry_run=True)


def _q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    return SlayerQuery(**kw)


# ===========================================================================
# Parse level.
# ===========================================================================


class TestExpressionParse:
    def test_arithmetic_source(self) -> None:
        node = parse_expr("sum(amount - cost)")
        assert isinstance(node, AggCall)
        assert node.agg == "sum"
        assert node.source == parse_expr("amount - cost")

    def test_expression_source_canonical_text_functional(self) -> None:
        # An expression source renders functionally, so its canonical text
        # can't collide with the distinct parse tree ``amount - sum(cost)``.
        expr_agg = canonical_metric_text(parse_expr("sum(amount - cost)"))
        assert expr_agg == "sum(amount - cost)"
        assert expr_agg != canonical_metric_text(parse_expr("amount - sum(cost)"))
        assert canonical_metric_text(parse_expr("sum(amount)")) == "sum(amount)"

    def test_scalar_call_source(self) -> None:
        node = parse_expr("count_distinct(upper(email))")
        assert isinstance(node, AggCall)
        assert node.agg == "count_distinct"
        assert node.source == parse_expr("upper(email)")

    def test_constant_source(self) -> None:
        node = parse_expr("count(1)")
        assert isinstance(node, AggCall)
        assert node.agg == "count"

    def test_formatting_insensitive_parse(self) -> None:
        assert parse_expr("sum(amount-cost)") == parse_expr("sum( amount - cost )")

    def test_kwargs_compose_with_expression(self) -> None:
        orders = _orders()
        bundle = ResolvedSourceBundle(dialect="postgres", population=orders, referenced_datasets=[_customers()])
        key = bind_expr(
            parse_expr("sum(orders.amount - orders.cost, partition_by=orders.region)"), scope=DatasetScope(population=orders), bundle=bundle,
        ).value_key
        assert isinstance(key, AggregateKey)
        assert isinstance(key.source, ArithmeticKey)
        assert key.partition_keys == Grain.of([FieldKey(path=(), leaf="region")])


# ===========================================================================
# SQL generation.
# ===========================================================================


class TestExpressionSql:
    async def test_simple_arithmetic(self) -> None:
        resp = await _dry(_q(metrics=["sum(orders.amount - orders.cost)"], dimensions=["orders.status"]))
        sql = _norm(resp.sql)
        assert re.search(r"SUM\s*\(.*?amount.*?-.*?cost.*?\)", sql), sql
        assert "orders.amount_cost_sum" in resp.columns

    async def test_scalar_call_inside(self) -> None:
        resp = await _dry(_q(metrics=["count_distinct(upper(orders.email))"]))
        sql = resp.sql.upper()
        assert "COUNT(DISTINCT" in _norm(sql).replace("( ", "(")
        assert "UPPER(" in sql

    async def test_constant_only_expression(self) -> None:
        resp = await _dry(_q(metrics=["count(1)"]))
        assert re.search(r"COUNT\s*\(\s*1\s*\)", resp.sql, re.IGNORECASE), resp.sql

    async def test_percentile_over_expression(self) -> None:
        resp = await _dry(_q(metrics=["percentile(orders.price * orders.quantity, p=0.5)"]))
        assert "price" in resp.sql
        assert "quantity" in resp.sql
        assert "orders.price_quantity_percentile_p_0_5" in resp.columns

    async def test_custom_agg_over_expression(self) -> None:
        resp = await _dry(_q(metrics=["my_agg(orders.price * orders.quantity)"]))
        sql = _norm(resp.sql)
        assert re.search(r"AVG\s*\(.*?price.*?\*.*?quantity.*?\)", sql), sql

    async def test_derived_sql_column_operand(self) -> None:
        resp = await _dry(_q(metrics=["sum(orders.net * 2)"]))
        sql = _norm(resp.sql)
        assert re.search(r"SUM\s*\(.*?amount.*?-.*?tax.*?\)", sql), sql

    async def test_expression_in_having_filter(self) -> None:
        resp = await _dry(
            _q(
                metrics=["sum(orders.amount - orders.cost)"],
                dimensions=["orders.status"],
                filter=["sum(orders.amount - orders.cost) > 0"],
            )
        )
        assert "HAVING" in resp.sql.upper()

    async def test_expression_in_order(self) -> None:
        resp = await _dry(
            _q(
                metrics=["sum(orders.amount - orders.cost)"],
                dimensions=["orders.status"],
                order=[OrderItem(field="sum(orders.amount - orders.cost)", direction="desc")],
            )
        )
        assert "ORDER BY" in resp.sql.upper()
        assert "amount_cost_sum" in resp.sql

    async def test_expression_with_window_kwarg(self) -> None:
        resp = await _dry(
            _q(
                metrics=["sum(orders.amount - orders.cost, window='30d')"],
                dimensions=["orders.status"],
                time_dimensions=[
                    TimeDimension(
                        field="orders.created_at", granularity=TimeGranularity.MONTH
                    )
                ],
            )
        )
        assert any("amount_cost_sum" in col for col in resp.columns)

    async def test_stage_scope_expression(self) -> None:
        stage1 = SlayerQuery.model_validate({
            "name": "stage1",
            "population": "orders",
            "dimensions": ["orders.status"],
            "metrics": [{"metric": "sum(orders.amount - orders.cost)"}],
        })
        root = SlayerQuery.model_validate({
            "population": "stage1",
            "dimensions": ["stage1.status"],
            "metrics": [{"metric": "max(stage1.amount_cost_sum)"}],
        })
        model = _orders()
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="postgres")
            )
            await storage.save_dataset(model)
            await storage.save_dataset(_customers())
            engine = SlayerQueryEngine(storage=storage)
            resp = await engine.execute([stage1, root], dry_run=True)
        assert "amount_cost_sum" in resp.sql

    async def test_expression_inside_computed_dimension(self) -> None:
        resp = await _dry(
            _q(
                dimensions=[
                    "orders.region",
                    {
                        "field": (
                            "CASE WHEN sum(orders.amount - orders.cost, partition_by=orders.region) > 0 "
                            "THEN 1 ELSE 0 END"
                        ),
                        "alias": "profitable",
                    },
                ],
                metrics=[QueryMetric(metric="sum(orders.amount)", alias="t")],
            )
        )
        assert re.search(r"SUM\s*\(.*?amount.*?-.*?cost.*?\)", _norm(resp.sql)), resp.sql

    async def test_bare_expression_agg_in_dimension_needs_partition_by(self) -> None:
        q = _q(
            dimensions=[
                "orders.region",
                {
                    "field": (
                        "CASE WHEN sum(orders.amount - orders.cost) > 0 THEN 1 ELSE 0 END"
                    ),
                    "alias": "profitable",
                },
            ],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="t")],
        )
        with pytest.raises(ValueError, match="partition_by"):
            await _dry(q)


# ===========================================================================
# Naming.
# ===========================================================================


class TestExpressionNaming:
    async def test_derived_key(self) -> None:
        resp = await _dry(_q(metrics=["sum(orders.amount - orders.cost)"]))
        assert "orders.amount_cost_sum" in resp.columns

    async def test_formatting_insensitive_key(self) -> None:
        r1 = await _dry(_q(metrics=["sum(orders.amount-orders.cost)"]))
        r2 = await _dry(_q(metrics=["sum( orders.amount - orders.cost )"]))
        assert list(r1.columns) == list(r2.columns)

    async def test_rename_override(self) -> None:
        resp = await _dry(
            _q(metrics=[{"metric": "sum(orders.amount - orders.cost)", "alias": "profit"}])
        )
        assert "profit" in resp.columns
        assert "orders.amount_cost_sum" not in resp.columns

    async def test_long_expression_capped_and_formatting_stable(self) -> None:
        spaced = "sum(orders.amount + orders.cost + orders.price + orders.quantity + orders.amount * orders.cost * orders.price * orders.quantity)"
        packed = "sum(orders.amount+orders.cost+orders.price+orders.quantity+orders.amount*orders.cost*orders.price*orders.quantity)"
        r1 = await _dry(_q(metrics=[spaced]))
        r2 = await _dry(_q(metrics=[packed]))
        assert list(r1.columns) == list(r2.columns)
        key = next(c for c in r1.columns if c.endswith("_sum"))
        assert key.startswith("orders.")
        assert len(key) < 80
        # Over the length cap the key folds through the stable-hash rule — an
        # eight-hex-digit digest segment, not plain truncation.
        assert re.search(r"_[0-9a-f]{8}_", key), key

    async def test_expression_partition_by_suffix_key(self) -> None:
        resp = await _dry(
            _q(
                dimensions=["orders.region"],
                metrics=["sum(orders.amount - orders.cost, partition_by=orders.region)"],
            )
        )
        assert "orders.amount_cost_sum_partition_by_region" in resp.columns

    async def test_display_classification(self) -> None:
        # Decision 11: classification derives from the inferred value class,
        # defaulting to plain numeric — identical display metadata to a plain
        # sum over an unformatted field (both preserving-unformatted, so both
        # omit the attributes entry, per the response-meta contract);
        # count-class expressions format as integers.
        resp = await _dry(
            _q(
                metrics=[
                    "sum(orders.amount - orders.cost)",
                    "sum(orders.amount)",
                    "count_distinct(upper(orders.email))",
                ]
            )
        )
        attrs = resp.attributes.metrics
        # Both are preserving-unformatted, so neither gets an entry.
        assert "orders.amount_cost_sum" not in attrs
        assert "orders.amount_sum" not in attrs
        count_meta = attrs["orders.upper_email_count_distinct"]
        assert count_meta.format is not None
        assert count_meta.format.type == NumberFormatType.INTEGER

    async def test_colliding_derived_keys_fail_loudly(self) -> None:
        q = _q(metrics=["sum(orders.amount - orders.cost)", "sum(orders.amount + orders.cost)"])
        with pytest.raises(ValueError, match="Rename") as ei:
            await _dry(q)
        assert "amount_cost_sum" in str(ei.value)


# ===========================================================================
# Key traversal — the expression source variant must not be a fail-open leaf.
# ===========================================================================


def _expr_agg_key(path: tuple = ()) -> AggregateKey:
    return AggregateKey(
        source=ArithmeticKey(
            op="-",
            operands=(
                FieldKey(path=path, leaf="amount"),
                FieldKey(path=path, leaf="cost"),
            ),
        ),
        agg="sum",
    )


class TestExpressionKeyTraversal:
    def test_walk_reaches_expression_operands(self) -> None:
        seen = list(walk_value_keys(_expr_agg_key()))
        assert FieldKey(leaf="amount") in seen
        assert FieldKey(leaf="cost") in seen

    def test_reroot_rewrites_expression_operands(self) -> None:
        rerooted = reroot_value_key(
            _expr_agg_key(path=("customers",)), target_path=("customers",)
        )
        column_keys = [
            k for k in walk_value_keys(rerooted) if isinstance(k, FieldKey)
        ]
        assert column_keys
        assert all(k.path == () for k in column_keys)

    def test_substitute_replaces_expression_operand(self) -> None:
        replacement = FieldKey(leaf="net")
        out = substitute_value_keys(
            _expr_agg_key(), {FieldKey(leaf="amount"): replacement}
        )
        seen = list(walk_value_keys(out))
        assert replacement in seen
        assert FieldKey(leaf="amount") not in seen


# ===========================================================================
# Unsupported shapes — clear errors.
# ===========================================================================


class TestExpressionErrors:
    async def test_cross_model_expression_accepted(self) -> None:
        # The cross-dataset boundary is lifted (covered in test_dev1832_*).
        resp = await _dry(_q(metrics=["sum(orders.amount - customers.discount)"]))
        assert resp.sql

    async def test_filtered_column_operand_accepted(self) -> None:
        # A filtered operand desugars to CASE WHEN, not a rejection.
        resp = await _dry(_q(metrics=["sum(orders.ok_amount - orders.cost)"]))
        assert resp.sql
        assert "CASE WHEN" in resp.sql.upper()

    async def test_mixed_row_and_attached_source_accepted(self) -> None:
        # ``sum(sum(amount))`` is a degenerate re-aggregation; a source mixing
        # a row leaf with an attached value is a row-grain aggregation and
        # compiles, not the expression gate.
        q = _q(metrics=["sum(orders.amount + sum(orders.amount))"])
        resp = await _dry(q)
        assert resp.sql


# ===========================================================================
# Gates and types.
# ===========================================================================


class TestExpressionGates:
    async def test_whitelist_does_not_block_expression(self) -> None:
        # quantity whitelists only min/max, but the expression is a new
        # derived quantity — gates are advisory for multi-token operands.
        resp = await _dry(_q(metrics=["sum(orders.price * orders.quantity)"]))
        assert resp.sql

    async def test_pk_gate_does_not_block_expression(self) -> None:
        resp = await _dry(_q(metrics=["max(orders.id * 2)"]))
        assert resp.sql

    async def test_single_column_functional_still_gated(self) -> None:
        q = _q(metrics=["sum(orders.quantity)"])
        with pytest.raises(ValueError):
            await _dry(q)

    async def test_confidently_non_numeric_rejected(self) -> None:
        q = _q(metrics=["sum(lower(orders.name))"])
        with pytest.raises(ValueError, match="(?i)numeric"):
            await _dry(q)

    async def test_boolean_expression_accepted(self) -> None:
        # A boolean operand is its integer inside a numeric aggregation.
        resp = await _dry(_q(metrics=["sum(True)"]))
        assert "SUM(CAST(TRUE AS INT))" in (resp.sql or "")

    async def test_boolean_scalar_expression_accepted(self) -> None:
        # like(...) is boolean; iif follows its branch types.
        for measure in ["sum(like(orders.name, 'A%'))", "sum(iif(orders.amount, True, False))"]:
            resp = await _dry(_q(metrics=[measure]))
            assert "SUM(CAST(" in (resp.sql or ""), resp.sql

    async def test_numeric_iif_expression_allowed(self) -> None:
        # iif with numeric branches stays numeric — must not be over-rejected.
        resp = await _dry(_q(metrics=["sum(iif(orders.amount, 1, 0))"]))
        assert resp.sql


async def _classify(query) -> tuple[str, str]:
    """Dry-run outcome as (kind, msg): 'ok' or the exception type name. AttributeError surfaces the crash."""
    try:
        await _dry(query)
        return ("ok", "")
    except Exception as e:  # noqa: BLE001 - parity harness classifies by exception type
        return (type(e).__name__, str(e))


class TestExpressionPartitionAttribution:
    """Partition-key attribution over relationships must treat expression sources like their plain-field twin (no AttributeError)."""

    async def test_joined_partition_by_matches_column_twin(self) -> None:
        expr = await _classify(
            _q(
                dimensions=["customers.discount"],
                metrics=["sum(orders.amount - orders.cost, partition_by=customers.discount)"],
            )
        )
        col = await _classify(
            _q(
                dimensions=["customers.discount"],
                metrics=["sum(orders.amount, partition_by=customers.discount)"],
            )
        )
        assert expr[0] == col[0] == "ok", (expr, col)

    async def test_computed_dim_expr_agg_with_cross_model_measure(self) -> None:
        case = "CASE WHEN {agg} > 0 THEN 'profit' ELSE 'loss' END"
        expr = await _classify(
            _q(
                dimensions=[case.format(agg="sum(orders.amount - orders.cost, partition_by=orders.region)"), "orders.region"],
                metrics=["count(customers.id)"],
            )
        )
        col = await _classify(
            _q(
                dimensions=[case.format(agg="sum(orders.amount, partition_by=orders.region)"), "orders.region"],
                metrics=["count(customers.id)"],
            )
        )
        assert expr[0] == col[0] == "ok", (expr, col)
