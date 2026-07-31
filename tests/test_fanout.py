"""Unit tests for DEV-1683 fan-out detection (slayer/engine/fanout.py).

These exercise the pure detector on hand-built ``EnrichedQuery`` objects and
the per-hop cardinality helper — no database, no engine. The end-to-end
behaviour against a real SQLite database lives in
``tests/integration/test_fanout_integration.py``.
"""

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import FanoutError, SlayerError
from slayer.core.models import Column, SlayerModel
from slayer.engine.enriched import (
    CrossModelMeasure,
    EnrichedDimension,
    EnrichedMeasure,
    EnrichedQuery,
)
from slayer.engine.fanout import (
    FANOUT_INVARIANT_AGGREGATIONS,
    _hop_is_fanning,
    check_fanout,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _model(name, pk_cols):
    cols = [Column(name=c, type=DataType.INT, primary_key=True) for c in pk_cols]
    # add a non-pk value column so the model is realistic
    cols.append(Column(name="val", sql="val", type=DataType.DOUBLE))
    return SlayerModel(name=name, sql_table=name, data_source="db", columns=cols)


def _measure(name, agg, model_name, alias=None, sql=None):
    return EnrichedMeasure(
        name=name, sql=sql, aggregation=agg,
        alias=alias or f"{model_name}.{name}_{agg}", model_name=model_name,
    )


def _dim(name, model_name, alias=None):
    return EnrichedDimension(
        name=name, sql=name, type=DataType.TEXT,
        alias=alias or f"{model_name}.{name}", model_name=model_name,
    )


def _cm(agg, owner, alias, rerooted):
    """Minimal CrossModelMeasure carrying a re-rooted enriched query."""
    return CrossModelMeasure(
        name=f"{owner}.{alias}",
        alias=alias,
        target_model_name=owner,
        target_model_sql_table=owner,
        target_model_sql=None,
        measure=_measure("score", agg, owner, alias=alias),
        join_pairs=[["customer_id", "id"]],
        shared_dimensions=[],
        shared_time_dimensions=[],
        source_model_name="orders",
        source_sql_table="orders",
        source_sql=None,
        rerooted_enriched=rerooted,
    )


# ---------------------------------------------------------------------------
# _hop_is_fanning — per-hop cardinality (metadata-only, fail-closed)
# ---------------------------------------------------------------------------

def test_hop_safe_when_target_join_col_covers_pk():
    # orders -> customers ON customer_id = customers.id ; id is customers' PK
    customers = _model("customers", ["id"])
    assert _hop_is_fanning(customers, [["customer_id", "id"]]) is False


def test_hop_fanning_when_join_col_is_not_pk():
    # customers -> orders ON id = orders.customer_id ; customer_id is NOT orders' PK
    orders = _model("orders", ["id"])
    assert _hop_is_fanning(orders, [["id", "customer_id"]]) is True


def test_hop_fanning_on_partial_composite_pk_coverage():
    # target PK is (a, b); join only covers a -> not many-to-one
    t = _model("t", ["a", "b"])
    assert _hop_is_fanning(t, [["x", "a"]]) is True


def test_hop_safe_on_full_composite_pk_coverage():
    t = _model("t", ["a", "b"])
    assert _hop_is_fanning(t, [["x", "a"], ["y", "b"]]) is False


def test_hop_fanning_when_target_has_no_declared_pk():
    t = SlayerModel(
        name="t", sql_table="t", data_source="db",
        columns=[Column(name="id", type=DataType.INT), Column(name="val", sql="val", type=DataType.DOUBLE)],
    )
    assert _hop_is_fanning(t, [["x", "id"]]) is True


def test_hop_fanning_when_target_unresolvable():
    assert _hop_is_fanning(None, [["x", "id"]]) is True


# ---------------------------------------------------------------------------
# Invariant set
# ---------------------------------------------------------------------------

def test_invariant_set_contents():
    assert FANOUT_INVARIANT_AGGREGATIONS == {
        "min", "max", "count_distinct", "count_distinct_approx", "first", "last",
    }


@pytest.mark.parametrize("agg", ["min", "max", "count_distinct", "count_distinct_approx", "first", "last"])
def test_invariant_aggregation_never_fires(agg):
    eq = EnrichedQuery(
        model_name="customers",
        measures=[_measure("balance", agg, "customers")],
        dimensions=[_dim("status", "orders")],
        resolved_joins=[("orders", "orders", "customers.id = orders.customer_id", "left")],
        fanning_join_aliases={"orders"},
    )
    check_fanout(eq)  # must not raise


@pytest.mark.parametrize("agg", ["sum", "count", "avg", "weighted_avg", "median",
                                 "percentile", "stddev_samp", "var_pop", "corr",
                                 "my_custom_agg"])
def test_non_invariant_aggregation_fires(agg):
    """Everything outside the invariant set fires, including weighted_avg and
    custom model-level aggregation names (fail-closed)."""
    eq = EnrichedQuery(
        model_name="customers",
        measures=[_measure("balance", agg, "customers")],
        dimensions=[_dim("status", "orders")],
        resolved_joins=[("orders", "orders", "customers.id = orders.customer_id", "left")],
        fanning_join_aliases={"orders"},
    )
    with pytest.raises(FanoutError):
        check_fanout(eq)


# ---------------------------------------------------------------------------
# Mode 1 — base same-model measure fan-out
# ---------------------------------------------------------------------------

def test_mode1_fires_on_dimension_fanout():
    eq = EnrichedQuery(
        model_name="customers",
        measures=[_measure("balance", "sum", "customers")],
        dimensions=[_dim("status", "orders")],
        resolved_joins=[("orders", "orders", "customers.id = orders.customer_id", "left")],
        fanning_join_aliases={"orders"},
    )
    with pytest.raises(FanoutError):
        check_fanout(eq)


def test_fanout_error_is_slayer_and_value_error():
    assert issubclass(FanoutError, SlayerError)
    assert issubclass(FanoutError, ValueError)


def test_mode1_error_message_names_owner_target_and_fix():
    eq = EnrichedQuery(
        model_name="customers",
        measures=[_measure("balance", "sum", "customers")],
        dimensions=[_dim("status", "orders")],
        resolved_joins=[("orders", "orders", "customers.id = orders.customer_id", "left")],
        fanning_join_aliases={"orders"},
    )
    with pytest.raises(FanoutError) as exc:
        check_fanout(eq)
    msg = str(exc.value)
    assert "orders" in msg          # the fanning target
    assert "customers" in msg       # the owner
    # suggests the invariant-aggregation escape hatch
    assert "count_distinct" in msg


def test_mode1_no_fire_when_join_is_safe():
    eq = EnrichedQuery(
        model_name="customers",
        measures=[_measure("balance", "sum", "customers")],
        dimensions=[_dim("status", "orders")],
        resolved_joins=[("orders", "orders", "customers.id = orders.customer_id", "left")],
        fanning_join_aliases=set(),  # join proved safe
    )
    check_fanout(eq)  # must not raise


def test_mode1_fires_on_multihop_when_only_later_hop_fans():
    """Path source -> customers (safe) -> notes (fanning). Only the second hop
    fans; the alias 'customers__notes' is in the fanning set. The base measure
    still fires because a prefix of the dimension's path is fanning."""
    eq = EnrichedQuery(
        model_name="orders",
        measures=[_measure("amount", "sum", "orders")],
        dimensions=[_dim("body", "customers__notes", alias="orders.customers.notes.body")],
        resolved_joins=[
            ("customers", "customers", "orders.customer_id = customers.id", "left"),
            ("notes", "customers__notes", "customers.id = customers__notes.customer_id", "left"),
        ],
        fanning_join_aliases={"customers__notes"},  # first hop safe, second fans
    )
    with pytest.raises(FanoutError):
        check_fanout(eq)


def test_mode1_no_fire_when_fanning_join_only_serves_isolated_cross_model_cte():
    # The fanning join is present in resolved_joins + fanning set, but NO base
    # dimension/filter references it — it exists only to reach a cross-model
    # measure target. The base measure must NOT fire on it.
    cm = _cm("avg", owner="orders_agg", alias="orders_agg.x_avg", rerooted=None)
    eq = EnrichedQuery(
        model_name="customers",
        measures=[_measure("balance", "sum", "customers")],
        dimensions=[],  # nothing pins the orders join into the base FROM
        cross_model_measures=[cm],
        resolved_joins=[("orders", "orders", "customers.id = orders.customer_id", "left")],
        fanning_join_aliases={"orders"},
    )
    check_fanout(eq)  # base measure must not fire


# ---------------------------------------------------------------------------
# Mode 2 — cross-model re-rooting grain shift (unified via _check_one)
# ---------------------------------------------------------------------------

def test_mode2_fires_when_rerooted_measure_fans_out():
    rerooted = EnrichedQuery(
        model_name="customers",  # owner
        measures=[_measure("score", "avg", "customers", alias="orders.customers.score_avg")],
        dimensions=[_dim("status", "orders")],
        resolved_joins=[("orders", "orders", "orders.customer_id = customers.id", "left")],
        fanning_join_aliases={"orders"},
    )
    cm = _cm("avg", owner="customers", alias="orders.customers.score_avg", rerooted=rerooted)
    eq = EnrichedQuery(model_name="orders", cross_model_measures=[cm])
    with pytest.raises(FanoutError):
        check_fanout(eq)


def test_mode2_no_fire_for_invariant_rerooted_measure():
    rerooted = EnrichedQuery(
        model_name="customers",
        measures=[_measure("score", "max", "customers", alias="orders.customers.score_max")],
        dimensions=[_dim("status", "orders")],
        resolved_joins=[("orders", "orders", "orders.customer_id = customers.id", "left")],
        fanning_join_aliases={"orders"},
    )
    cm = _cm("max", owner="customers", alias="orders.customers.score_max", rerooted=rerooted)
    eq = EnrichedQuery(model_name="orders", cross_model_measures=[cm])
    check_fanout(eq)  # must not raise


# ---------------------------------------------------------------------------
# Constructor compatibility
# ---------------------------------------------------------------------------

def test_fanning_join_aliases_defaults_to_empty_set():
    eq = EnrichedQuery(
        model_name="x",
        measures=[_measure("a", "sum", "x")],
    )
    assert eq.fanning_join_aliases == set()
    check_fanout(eq)  # empty fanning set → never fires
