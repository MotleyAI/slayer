"""Same-model expression aggregation — ``sum(amount - cost)`` (DEV-1826 task 1.4).

Aggregating a row-level scalar expression built from bare host-model columns,
scalar-allowlist calls, arithmetic, and literals; deterministic auto-naming via
the shared computed-dimension sanitizer; loud errors for the unsupported shapes
(cross-model refs, filtered-column operands, nested aggregations/transforms);
per-column gates advisory for expressions.
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
    ColumnKey,
    reroot_value_key,
    substitute_value_keys,
)
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import OrderItem, SlayerQuery, TimeDimension
from slayer.engine.binding import walk_value_keys
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.syntax import AggCall, Ref, canonical_measure_text, parse_expr
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _norm


# ===========================================================================
# Model + harness.
# ===========================================================================


def _orders(**overrides) -> SlayerModel:
    fields = dict(
        name="orders",
        data_source="test",
        sql_table="orders",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="region", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="cost", type=DataType.DOUBLE),
            Column(name="price", type=DataType.DOUBLE),
            Column(name="tax", type=DataType.DOUBLE),
            Column(
                name="quantity",
                type=DataType.DOUBLE,
                allowed_aggregations=["min", "max"],
            ),
            Column(name="name", type=DataType.TEXT),
            Column(name="email", type=DataType.TEXT),
            Column(name="net", sql="amount - tax", type=DataType.DOUBLE),
            Column(
                name="ok_amount",
                sql="amount",
                type=DataType.DOUBLE,
                filter="status = 'ok'",
            ),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
        aggregations=[Aggregation(name="my_agg", formula="AVG({value})")],
    )
    fields.update(overrides)
    return SlayerModel(**fields)


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="test",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="discount", type=DataType.DOUBLE),
        ],
    )


async def _dry(query, *, model: SlayerModel | None = None):
    model = model or _orders()
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(
            DatasourceConfig(name=model.data_source, type="postgres")
        )
        await storage.save_model(model)
        await storage.save_model(_customers())
        engine = SlayerQueryEngine(storage=storage)
        return await engine.execute(query, dry_run=True)


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
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
        # can't collide with the distinct parse tree ``amount - (cost:sum)``.
        expr_agg = canonical_measure_text(parse_expr("sum(amount - cost)"))
        assert expr_agg == "sum(amount - cost)"
        assert expr_agg != canonical_measure_text(parse_expr("amount - cost:sum"))
        # Column sources keep their colon spelling (functional/colon parity).
        assert canonical_measure_text(parse_expr("sum(amount)")) == "amount:sum"

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
        node = parse_expr("sum(amount - cost, partition_by=region)")
        assert isinstance(node, AggCall)
        assert dict(node.kwargs)["partition_by"] == Ref(name="region")


# ===========================================================================
# SQL generation.
# ===========================================================================


class TestExpressionSql:
    async def test_simple_arithmetic(self) -> None:
        resp = await _dry(_q(measures=["sum(amount - cost)"], dimensions=["status"]))
        sql = _norm(resp.sql)
        assert re.search(r"SUM\s*\(.*?amount.*?-.*?cost.*?\)", sql), sql
        assert "orders.amount_cost_sum" in resp.columns

    async def test_scalar_call_inside(self) -> None:
        resp = await _dry(_q(measures=["count_distinct(upper(email))"]))
        sql = resp.sql.upper()
        assert "COUNT(DISTINCT" in _norm(sql).replace("( ", "(")
        assert "UPPER(" in sql

    async def test_constant_only_expression(self) -> None:
        resp = await _dry(_q(measures=["count(1)"]))
        assert re.search(r"COUNT\s*\(\s*1\s*\)", resp.sql, re.IGNORECASE), resp.sql

    async def test_percentile_over_expression(self) -> None:
        resp = await _dry(_q(measures=["percentile(price * quantity, p=0.5)"]))
        assert "price" in resp.sql
        assert "quantity" in resp.sql
        assert "orders.price_quantity_percentile_p_0_5" in resp.columns

    async def test_custom_agg_over_expression(self) -> None:
        resp = await _dry(_q(measures=["my_agg(price * quantity)"]))
        sql = _norm(resp.sql)
        assert re.search(r"AVG\s*\(.*?price.*?\*.*?quantity.*?\)", sql), sql

    async def test_derived_sql_column_operand(self) -> None:
        resp = await _dry(_q(measures=["sum(net * 2)"]))
        sql = _norm(resp.sql)
        assert re.search(r"SUM\s*\(.*?amount.*?-.*?tax.*?\)", sql), sql

    async def test_expression_in_having_filter(self) -> None:
        resp = await _dry(
            _q(
                measures=["sum(amount - cost)"],
                dimensions=["status"],
                filters=["sum(amount - cost) > 0"],
            )
        )
        assert "HAVING" in resp.sql.upper()

    async def test_expression_in_order(self) -> None:
        resp = await _dry(
            _q(
                measures=["sum(amount - cost)"],
                dimensions=["status"],
                order=[OrderItem(column="sum(amount - cost)", direction="desc")],
            )
        )
        assert "ORDER BY" in resp.sql.upper()
        assert "amount_cost_sum" in resp.sql

    async def test_expression_with_window_kwarg(self) -> None:
        resp = await _dry(
            _q(
                measures=["sum(amount - cost, window='30d')"],
                dimensions=["status"],
                time_dimensions=[
                    TimeDimension(
                        dimension="created_at", granularity=TimeGranularity.MONTH
                    )
                ],
            )
        )
        assert any("amount_cost_sum" in col for col in resp.columns)

    async def test_stage_scope_expression(self) -> None:
        stage1 = SlayerQuery(
            name="stage1",
            source_model="orders",
            dimensions=["status"],
            measures=[{"formula": "sum(amount - cost)"}],
        )
        root = SlayerQuery(
            source_model="stage1",
            dimensions=["status"],
            measures=[{"formula": "amount_cost_sum:max"}],
        )
        model = _orders()
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="postgres")
            )
            await storage.save_model(model)
            await storage.save_model(_customers())
            engine = SlayerQueryEngine(storage=storage)
            resp = await engine.execute([stage1, root], dry_run=True)
        assert "amount_cost_sum" in resp.sql

    async def test_expression_inside_computed_dimension(self) -> None:
        resp = await _dry(
            _q(
                dimensions=[
                    "region",
                    {
                        "expression": (
                            "CASE WHEN sum(amount - cost, partition_by=region) > 0 "
                            "THEN 1 ELSE 0 END"
                        ),
                        "name": "profitable",
                    },
                ],
                measures=[ModelMeasure(formula="amount:sum", name="t")],
            )
        )
        assert re.search(r"SUM\s*\(.*?amount.*?-.*?cost.*?\)", _norm(resp.sql)), resp.sql

    async def test_bare_expression_agg_in_dimension_needs_partition_by(self) -> None:
        q = _q(
            dimensions=[
                "region",
                {
                    "expression": (
                        "CASE WHEN sum(amount - cost) > 0 THEN 1 ELSE 0 END"
                    ),
                    "name": "profitable",
                },
            ],
            measures=[ModelMeasure(formula="amount:sum", name="t")],
        )
        with pytest.raises(ValueError, match="partition_by"):
            await _dry(q)


# ===========================================================================
# Naming.
# ===========================================================================


class TestExpressionNaming:
    async def test_derived_key(self) -> None:
        resp = await _dry(_q(measures=["sum(amount - cost)"]))
        assert "orders.amount_cost_sum" in resp.columns

    async def test_formatting_insensitive_key(self) -> None:
        r1 = await _dry(_q(measures=["sum(amount-cost)"]))
        r2 = await _dry(_q(measures=["sum( amount - cost )"]))
        assert list(r1.columns) == list(r2.columns)

    async def test_rename_override(self) -> None:
        resp = await _dry(
            _q(measures=[{"formula": "sum(amount - cost)", "name": "profit"}])
        )
        assert "orders.profit" in resp.columns
        assert "orders.amount_cost_sum" not in resp.columns

    async def test_long_expression_capped_and_formatting_stable(self) -> None:
        spaced = "sum(amount + cost + price + quantity + amount * cost * price * quantity)"
        packed = "sum(amount+cost+price+quantity+amount*cost*price*quantity)"
        r1 = await _dry(_q(measures=[spaced]))
        r2 = await _dry(_q(measures=[packed]))
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
                dimensions=["region"],
                measures=["sum(amount - cost, partition_by=region)"],
            )
        )
        assert "orders.amount_cost_sum_partition_by_region" in resp.columns

    async def test_display_classification(self) -> None:
        # Decision 11: classification derives from the inferred value class,
        # defaulting to plain numeric — identical display metadata to a plain
        # sum over an unformatted column (both preserving-unformatted, so both
        # omit the attributes entry, per the response-meta contract);
        # count-class expressions format as integers.
        resp = await _dry(
            _q(
                measures=[
                    "sum(amount - cost)",
                    "amount:sum",
                    "count_distinct(upper(email))",
                ]
            )
        )
        attrs = resp.attributes.measures
        # Both are preserving-unformatted, so neither gets an entry.
        assert "orders.amount_cost_sum" not in attrs
        assert "orders.amount_sum" not in attrs
        count_meta = attrs["orders.upper_email_count_distinct"]
        assert count_meta.format is not None
        assert count_meta.format.type == NumberFormatType.INTEGER

    async def test_colliding_derived_keys_fail_loudly(self) -> None:
        q = _q(measures=["sum(amount - cost)", "sum(amount + cost)"])
        with pytest.raises(ValueError, match="rename") as ei:
            await _dry(q)
        assert "amount_cost_sum" in str(ei.value)


# ===========================================================================
# Key traversal — the expression source variant must not be a fail-open leaf
# (DEV-1827 lesson; design Risks).
# ===========================================================================


def _expr_agg_key(path: tuple = ()) -> AggregateKey:
    return AggregateKey(
        source=ArithmeticKey(
            op="-",
            operands=(
                ColumnKey(path=path, leaf="amount"),
                ColumnKey(path=path, leaf="cost"),
            ),
        ),
        agg="sum",
    )


class TestExpressionKeyTraversal:
    def test_walk_reaches_expression_operands(self) -> None:
        seen = list(walk_value_keys(_expr_agg_key()))
        assert ColumnKey(leaf="amount") in seen
        assert ColumnKey(leaf="cost") in seen

    def test_reroot_rewrites_expression_operands(self) -> None:
        rerooted = reroot_value_key(
            _expr_agg_key(path=("customers",)), target_path=("customers",)
        )
        column_keys = [
            k for k in walk_value_keys(rerooted) if isinstance(k, ColumnKey)
        ]
        assert column_keys
        assert all(k.path == () for k in column_keys)

    def test_substitute_replaces_expression_operand(self) -> None:
        replacement = ColumnKey(leaf="net")
        out = substitute_value_keys(
            _expr_agg_key(), {ColumnKey(leaf="amount"): replacement}
        )
        seen = list(walk_value_keys(out))
        assert replacement in seen
        assert ColumnKey(leaf="amount") not in seen


# ===========================================================================
# Unsupported shapes — clear errors.
# ===========================================================================


class TestExpressionErrors:
    async def test_cross_model_expression_rejected(self) -> None:
        q = _q(measures=["sum(amount - customers.discount)"])
        with pytest.raises(ValueError, match="(?i)cross-model"):
            await _dry(q)

    async def test_filtered_column_operand_rejected(self) -> None:
        q = _q(measures=["sum(ok_amount - cost)"])
        with pytest.raises(ValueError, match="ok_amount") as ei:
            await _dry(q)
        assert "filter" in str(ei.value).lower()

    async def test_nested_aggregation_rejected(self) -> None:
        q = _q(measures=["sum(sum(amount))"])
        with pytest.raises(ValueError, match="(?i)nest"):
            await _dry(q)

    async def test_nested_transform_rejected(self) -> None:
        q = _q(measures=["sum(cumsum(amount) - 1)"])
        with pytest.raises(ValueError, match="(?i)nest"):
            await _dry(q)

    async def test_first_last_over_expression_rejected(self) -> None:
        # first/last need a plain column — the ranked kernel can't rank an
        # expression, so reject at binding instead of crashing at SQL time.
        q = _q(measures=["last(amount - cost, created_at)"])
        with pytest.raises(ValueError, match="(?i)expression"):
            await _dry(q)


# ===========================================================================
# Gates and types.
# ===========================================================================


class TestExpressionGates:
    async def test_whitelist_does_not_block_expression(self) -> None:
        # quantity whitelists only min/max, but the expression is a new
        # derived quantity — gates are advisory for multi-token operands.
        resp = await _dry(_q(measures=["sum(price * quantity)"]))
        assert resp.sql

    async def test_pk_gate_does_not_block_expression(self) -> None:
        resp = await _dry(_q(measures=["max(id * 2)"]))
        assert resp.sql

    async def test_single_column_functional_still_gated(self) -> None:
        q_func = _q(measures=["sum(quantity)"])
        with pytest.raises(ValueError) as e_func:
            await _dry(q_func)
        q_colon = _q(measures=["quantity:sum"])
        with pytest.raises(ValueError) as e_colon:
            await _dry(q_colon)
        assert type(e_func.value) is type(e_colon.value)
        assert str(e_func.value) == str(e_colon.value)

    async def test_confidently_non_numeric_rejected(self) -> None:
        q = _q(measures=["sum(lower(name))"])
        with pytest.raises(ValueError, match="(?i)numeric"):
            await _dry(q)

    async def test_boolean_expression_rejected(self) -> None:
        # A boolean operand is non-numeric: SUM(<bool>) errors on Postgres /
        # SQL Server, so reject it like a text operand.
        q = _q(measures=["sum(True)"])
        with pytest.raises(ValueError, match="(?i)boolean"):
            await _dry(q)

    async def test_boolean_scalar_expression_rejected(self) -> None:
        # like(...) is boolean; iif follows its branch types — both reach SQL
        # generation as SUM(<bool>) unless rejected here.
        for measure in ["sum(like(name, 'A%'))", "sum(iif(amount, True, False))"]:
            q = _q(measures=[measure])
            with pytest.raises(ValueError, match="(?i)boolean"):
                await _dry(q)

    async def test_numeric_iif_expression_allowed(self) -> None:
        # iif with numeric branches stays numeric — must not be over-rejected.
        resp = await _dry(_q(measures=["sum(iif(amount, 1, 0))"]))
        assert resp.sql


async def _classify(query) -> tuple[str, str]:
    """Dry-run outcome as (kind, msg): 'ok' or the exception type name. AttributeError surfaces the crash."""
    try:
        await _dry(query)
        return ("ok", "")
    except Exception as e:  # noqa: BLE001 - parity harness classifies by exception type
        return (type(e).__name__, str(e))


class TestExpressionPartitionAttribution:
    """Partition-key attribution over joins must treat expression sources like their plain-column twin (no AttributeError)."""

    async def test_joined_partition_by_matches_column_twin(self) -> None:
        expr = await _classify(
            _q(
                dimensions=["customers.discount"],
                measures=["sum(amount - cost, partition_by=customers.discount)"],
            )
        )
        col = await _classify(
            _q(
                dimensions=["customers.discount"],
                measures=["sum(amount, partition_by=customers.discount)"],
            )
        )
        assert expr[0] == col[0] == "ok", (expr, col)

    async def test_computed_dim_expr_agg_with_cross_model_measure(self) -> None:
        case = "CASE WHEN {agg} > 0 THEN 'profit' ELSE 'loss' END"
        expr = await _classify(
            _q(
                dimensions=[case.format(agg="sum(amount - cost, partition_by=region)"), "region"],
                measures=["count(customers.id)"],
            )
        )
        col = await _classify(
            _q(
                dimensions=[case.format(agg="sum(amount, partition_by=region)"), "region"],
                measures=["count(customers.id)"],
            )
        )
        assert expr[0] == col[0] == "ok", (expr, col)
