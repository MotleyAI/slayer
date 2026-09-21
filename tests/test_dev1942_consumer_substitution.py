"""DEV-1942 task 1.4 — ``substitute_consumer_keys`` mirrors ``walk_consumer_keys``.

The planner's re-aggregation pre-substitution must replace a root only where root
discovery looks: opaque below an attach-owning aggregate's inputs (source/args/
kwargs), still traversing its partition keys. The deep ``substitute_value_keys``
erases a nested re-aggregation before row-attach discovery runs; the consumer-scoped
walk keeps it. Red until ``substitute_consumer_keys`` lands (ImportError today).

Spec: openspec …/specs/queries/partitioned-aggregates — "Mixed sources carry the
full expression-source surface" (discovery-opacity clause).
"""

from __future__ import annotations

from decimal import Decimal

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    Grain,
    LiteralKey,
    TransformKey,
    substitute_consumer_keys,
    substitute_value_keys,
    walk_value_keys,
)

QTY = ColumnKey(leaf="quantity")
UP = ColumnKey(leaf="unit_price")
AMOUNT = ColumnKey(leaf="amount")
PRODUCT = ColumnKey(leaf="product")
CITY = ColumnKey(leaf="city")
REGION = ColumnKey(leaf="region")
ORDERED_AT = ColumnKey(leaf="ordered_at")
PH = ColumnKey(leaf="__regroup__0__min")
PH2 = ColumnKey(leaf="__regroup__1__band")

AVG_UP_PRODUCT = AggregateKey(source=UP, agg="avg", partition_keys=Grain.of([PRODUCT]))
REGION_SUM = AggregateKey(source=AMOUNT, agg="sum", partition_keys=Grain.of([REGION]))
MIXED_SRC = ArithmeticKey(op="*", operands=(QTY, AVG_UP_PRODUCT))
#: sum(quantity * avg(unit_price, partition_by=product)) — a mixed source.
MIXED = AggregateKey(source=MIXED_SRC, agg="sum")
#: mixed source AND an attached parameter — both attached by one mechanism.
MIXED_PARAM = AggregateKey(source=MIXED_SRC, agg="weighted_avg",
                           kwargs=(("weight", REGION_SUM),))

# The DEV-1942 issue shape: min(amount:sum(partition_by=[region, ordered_at]),
# partition_by=region) used inside sum(amount * min(...)).
X = AggregateKey(source=AMOUNT, agg="sum", partition_keys=Grain.of([REGION, ORDERED_AT]))
MIN_X = AggregateKey(source=X, agg="min", partition_keys=Grain.of([REGION]))
MIXED_MIN = AggregateKey(source=ArithmeticKey(op="*", operands=(AMOUNT, MIN_X)), agg="sum")


def test_replaced_at_match() -> None:
    assert substitute_consumer_keys(AVG_UP_PRODUCT, {AVG_UP_PRODUCT: PH}) is PH


def test_opaque_below_a_roots_source() -> None:
    # AVG_UP_PRODUCT lives below MIXED's source (an attached input): consumer-scoped
    # substitution leaves it, deep substitution rewrites it.
    assert substitute_consumer_keys(MIXED, {AVG_UP_PRODUCT: PH}) is MIXED
    assert substitute_value_keys(MIXED, {AVG_UP_PRODUCT: PH}) != MIXED


def test_parameter_input_is_opaque() -> None:
    assert substitute_consumer_keys(MIXED_PARAM, {REGION_SUM: PH}) is MIXED_PARAM


def test_partition_keys_are_traversed() -> None:
    pb_inner = AggregateKey(source=AMOUNT, agg="sum", partition_keys=Grain.of([CITY]))
    band = ArithmeticKey(op=">", operands=(pb_inner, LiteralKey(value=Decimal(100))))
    root = AggregateKey(source=MIXED_SRC, agg="sum", partition_keys=Grain.of([band]))
    got = substitute_consumer_keys(root, {pb_inner: PH2})
    seen = set(walk_value_keys(got))
    assert PH2 in seen             # partition key rewritten
    assert pb_inner not in seen
    assert AVG_UP_PRODUCT in seen  # source still opaque, unchanged


def test_plain_composite_is_traversed() -> None:
    expr = ArithmeticKey(op="+", operands=(AMOUNT, QTY))
    got = substitute_consumer_keys(expr, {AMOUNT: PH})
    assert got.operands == (PH, QTY)


def test_plain_aggregate_source_is_traversed_and_scalars_pass_through() -> None:
    agg = AggregateKey(source=AMOUNT, agg="quantile", args=(Decimal("0.5"),))
    got = substitute_consumer_keys(agg, {AMOUNT: PH})
    assert got.source is PH               # not attach-owning → source rewritten
    assert got.args == (Decimal("0.5"),)  # scalar arg untouched


def test_transform_input_is_traversed() -> None:
    t = TransformKey(op="rank", input=AVG_UP_PRODUCT)
    assert substitute_consumer_keys(t, {AVG_UP_PRODUCT: PH}).input is PH


def test_bare_scalars_pass_through() -> None:
    assert substitute_consumer_keys(Decimal("0.5"), {}) == Decimal("0.5")
    assert substitute_consumer_keys("foo", {}) == "foo"
    assert substitute_consumer_keys(None, {}) is None


def test_identity_object_when_nothing_matches() -> None:
    assert substitute_consumer_keys(MIXED, {REGION_SUM: PH}) is MIXED
    expr = ArithmeticKey(op="+", operands=(AMOUNT, QTY))
    assert substitute_consumer_keys(expr, {REGION_SUM: PH}) is expr


def test_deep_erases_the_nested_root_consumer_keeps_it() -> None:
    # The crux: substituting the shared re-aggregation on the mixed root.
    deep = substitute_value_keys(MIXED_MIN, {MIN_X: PH})
    consumer = substitute_consumer_keys(MIXED_MIN, {MIN_X: PH})
    assert MIN_X not in set(walk_value_keys(deep))       # deep erases it
    assert MIN_X in set(walk_value_keys(consumer))       # consumer keeps it
    assert consumer is MIXED_MIN
    # A standalone occurrence (a top-level consumer) is still replaced.
    assert substitute_consumer_keys(MIN_X, {MIN_X: PH}) is PH
