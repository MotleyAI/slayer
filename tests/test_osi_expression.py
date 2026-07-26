"""OSI metric/field expression -> SLayer formula transform (slayer/osi/expression.py).

``convert_expression`` walks a SQL aggregation expression with sqlglot and emits a
SLayer colon-syntax formula, materializing hidden derived Columns for non-bare
aggregate operands, and clean-failing anything inexpressible.

Callbacks (so the transform is model-agnostic and unit-testable in isolation):
- ``owner_of(qualifier, column) -> model | None``  which dataset owns a column
- ``ref_of(model, column) -> anchor-relative dotted ref | None``  (None = unreachable)
"""

import sqlglot

from slayer.osi.expression import convert_expression

# Single-model context: everything is owned by "orders"; refs are local (bare).
OWNER = lambda q, c: "orders"          # noqa: E731
REF = lambda m, c: c                    # noqa: E731


def _run(expr: str, *, entity_name: str = "m", owner_of=OWNER, ref_of=REF, **kw):
    return convert_expression(
        expr, entity_name=entity_name, owner_of=owner_of, ref_of=ref_of, **kw
    )


def _norm(sql: str) -> str:
    return sqlglot.parse_one(sql).sql()


# ─────────────────────────── simple aggregations ───────────────────────────

def test_sum_bare():
    r = _run("SUM(amount)")
    assert r.ok and r.formula == "amount:sum" and r.materialized == []


def test_count_star():
    assert _run("COUNT(*)").formula == "*:count"


def test_count_col():
    assert _run("COUNT(customer_id)").formula == "customer_id:count"


def test_count_distinct():
    assert _run("COUNT(DISTINCT customer_id)").formula == "customer_id:count_distinct"


def test_avg_min_max():
    assert _run("AVG(amount)").formula == "amount:avg"
    assert _run("MIN(amount)").formula == "amount:min"
    assert _run("MAX(amount)").formula == "amount:max"


# ─────────────────────── arithmetic + constants + scalar ────────────────────

def test_difference_of_aggs():
    assert _run("SUM(amount) - SUM(quantity)").formula == "amount:sum - quantity:sum"


def test_divide_by_constant():
    assert _run("SUM(amount) / 100").formula == "amount:sum / 100"


def test_ratio_is_plain_arithmetic():
    assert _run("(SUM(amount)) / (COUNT(*))").formula == "amount:sum / *:count"


def test_scalar_passthrough_nullif():
    r = _run("SUM(amount) / NULLIF(COUNT(*), 0)")
    assert r.ok and r.formula == "amount:sum / nullif(*:count, 0)"


def test_constant_times_agg():
    assert _run("0.9 * SUM(amount)").formula == "0.9 * amount:sum"


# ───────────────────── derived-column materialization ───────────────────────

def test_materialize_arithmetic_operand():
    r = _run("SUM(quantity * amount)", entity_name="revenue_line")
    assert r.ok
    assert len(r.materialized) == 1
    mc = r.materialized[0]
    assert mc.owning_model == "orders"
    assert mc.name.startswith("_revenue_line")
    assert _norm(mc.sql) == _norm("quantity * amount")
    assert r.formula == f"{mc.name}:sum"


def test_materialize_scalar_operand():
    r = _run("SUM(COALESCE(amount, 0))", entity_name="safe_rev")
    assert r.ok and len(r.materialized) == 1
    assert _norm(r.materialized[0].sql) == _norm("COALESCE(amount, 0)")
    assert r.formula == f"{r.materialized[0].name}:sum"


def test_materialize_case_filtered_count():
    r = _run("COUNT(CASE WHEN status = 'paid' THEN 1 END)", entity_name="paid_ct")
    assert r.ok and len(r.materialized) == 1
    assert "CASE" in r.materialized[0].sql.upper()
    assert r.formula == f"{r.materialized[0].name}:count"


def test_materialize_dedups_identical_operand():
    r = _run("SUM(quantity * amount) - MIN(quantity * amount)", entity_name="x")
    assert r.ok
    # The identical operand is materialized once and reused.
    assert len(r.materialized) == 1
    nm = r.materialized[0].name
    assert r.formula == f"{nm}:sum - {nm}:min"


def test_cross_dataset_operand_clean_fails():
    # quantity belongs to orders, price to products -> operand spans datasets.
    owner = lambda q, c: "orders" if c == "quantity" else "products"  # noqa: E731
    r = _run("SUM(quantity * price)", owner_of=owner)
    assert not r.ok and r.formula is None and r.reason


# ───────────────────────────── percentile ──────────────────────────────────

def test_percentile_cont():
    assert _run("PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount)").formula == (
        "amount:percentile(p=0.9)"
    )


def test_percentile_cont_half_is_median():
    assert _run("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)").formula == (
        "amount:median"
    )


def test_percentile_disc():
    assert _run("PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY amount)").formula == (
        "amount:percentile(p=0.9)"
    )


def test_percentile_out_of_range_clean_fails():
    r = _run("PERCENTILE_CONT(1.5) WITHIN GROUP (ORDER BY amount)")
    assert not r.ok and r.reason


def test_percentile_nonliteral_clean_fails():
    r = _run("PERCENTILE_CONT(amount) WITHIN GROUP (ORDER BY amount)")
    assert not r.ok


def test_percentile_unsupported_dialect_warns_but_emits():
    r = _run(
        "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount)",
        percentile_unsupported=True,
    )
    assert r.ok and r.formula == "amount:percentile(p=0.9)"
    assert r.warnings


# ───────────────────────────── clean-fails ─────────────────────────────────

def test_top_level_case_clean_fails():
    r = _run("CASE WHEN status = 'x' THEN 1 ELSE 0 END")
    assert not r.ok and r.formula is None and r.reason


def test_bare_unaggregated_column_clean_fails():
    r = _run("amount")
    assert not r.ok


def test_window_function_clean_fails():
    r = _run("SUM(amount) OVER (PARTITION BY customer_id)")
    assert not r.ok


def test_nested_aggregate_clean_fails():
    r = _run("SUM(SUM(amount))")
    assert not r.ok


def test_string_literal_clean_fails():
    r = _run("'hello'")
    assert not r.ok


def test_non_passthrough_function_clean_fails():
    r = _run("WEIRDFUNC(amount)")
    assert not r.ok


def test_sum_distinct_clean_fails():
    # Only COUNT(DISTINCT) is supported; SUM/AVG/MIN/MAX DISTINCT clean-fail.
    assert not _run("SUM(DISTINCT amount)").ok
    assert not _run("AVG(DISTINCT amount)").ok


# ───────────────────────── cross-model refs ────────────────────────────────

def test_qualified_column_emits_dotted_ref():
    owner = lambda q, c: "customers"  # noqa: E731
    ref = lambda m, c: f"customers.{c}"  # noqa: E731
    r = _run("COUNT(DISTINCT customers.customer_id)", owner_of=owner, ref_of=ref)
    assert r.ok and r.formula == "customers.customer_id:count_distinct"


def test_unreachable_ref_clean_fails():
    owner = lambda q, c: "regions"  # noqa: E731
    ref = lambda m, c: None          # noqa: E731  (no join path from anchor)
    r = _run("SUM(regions.population)", owner_of=owner, ref_of=ref)
    assert not r.ok and r.reason


def test_multihop_ref_backed_by_real_join_graph():
    # Prove the expression layer builds the SAME dotted path the converter would,
    # driven by JoinGraph.shortest_path (orders -> customers -> regions).
    from slayer.core.enums import DataType
    from slayer.core.models import Column, ModelJoin, SlayerModel
    from slayer.engine.join_graph import JoinGraph

    models = [
        SlayerModel(name="orders", sql_table="orders", data_source="d",
                    columns=[Column(name="customer_id", type=DataType.INT)],
                    joins=[ModelJoin(target_model="customers",
                                     join_pairs=[["customer_id", "customer_id"]])]),
        SlayerModel(name="customers", sql_table="customers", data_source="d",
                    columns=[Column(name="region_id", type=DataType.INT)],
                    joins=[ModelJoin(target_model="regions",
                                     join_pairs=[["region_id", "region_id"]])]),
        SlayerModel(name="regions", sql_table="regions", data_source="d",
                    columns=[Column(name="population", type=DataType.INT)]),
    ]
    graph = JoinGraph.build_from_models(models)
    anchor = "orders"

    def ref_of(model: str, column: str):
        path = graph.shortest_path(anchor, model)
        if path is None:
            return None
        return ".".join([*path, column])

    r = _run("SUM(regions.population)", owner_of=lambda q, c: "regions", ref_of=ref_of)
    assert r.ok and r.formula == "customers.regions.population:sum"
