"""DEV-1859 task 4.1 — the one classifier over an aggregation's full input set
(design decisions 8/9). ``attached_inputs`` spans source/args/kwargs deduped;
``is_reaggregation_key`` is pure (attached source only); ``is_row_attach_root``
owns a row/literal source with attached inputs; ``attached_operand_keys`` is the
lenient partition-key set; ``walk_consumer_keys`` is opaque below a root's
inputs but still walks its partition keys.

These reference the §5.1 API (renamed/added in spec-implement), so the module is
red until the structural re-cut lands.

Spec: openspec …/specs/queries/partitioned-aggregates — "Mixed sources carry
the full expression-source surface" (discovery opacity clause).
"""

from __future__ import annotations

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    Grain,
    LiteralKey,
    attached_inputs,
    attached_operand_keys,
    is_reaggregation_key,
    is_row_attach_root,
    walk_consumer_keys,
    walk_value_keys,
)

QTY = ColumnKey(leaf="quantity")
UP = ColumnKey(leaf="unit_price")
AMOUNT = ColumnKey(leaf="amount")
PRODUCT = ColumnKey(leaf="product")
CITY = ColumnKey(leaf="city")
REGION = ColumnKey(leaf="region")

AVG_UP_PRODUCT = AggregateKey(source=UP, agg="avg", partition_keys=Grain.of([PRODUCT]))
AVG_UP_CITY = AggregateKey(source=UP, agg="avg", partition_keys=Grain.of([CITY]))
REGION_SUM = AggregateKey(source=AMOUNT, agg="sum", partition_keys=Grain.of([REGION]))

MIXED_SRC = ArithmeticKey(op="*", operands=(QTY, AVG_UP_PRODUCT))
#: sum(quantity * avg(unit_price, partition_by=product)) — a mixed source.
MIXED = AggregateKey(source=MIXED_SRC, agg="sum")
#: avg(sum(amount, partition_by=[…])) — a pure re-aggregation.
PURE = AggregateKey(source=AVG_UP_PRODUCT, agg="avg")
#: weighted_avg(amount, weight=sum(amount, partition_by=region)) — row + param.
ROW_PARAM = AggregateKey(source=AMOUNT, agg="weighted_avg",
                         kwargs=(("weight", REGION_SUM),))
#: the same, parameter passed POSITIONALLY — attached inputs span args too.
POS_PARAM = AggregateKey(source=AMOUNT, agg="weighted_avg", args=(REGION_SUM,))
#: wsum(1, weight=sum(amount, partition_by=region)) — literal + param.
LIT_PARAM = AggregateKey(source=LiteralKey(value=1), agg="wsum",
                         kwargs=(("weight", REGION_SUM),))
#: mixed source AND an attached parameter — both attached by one mechanism.
MIXED_PARAM = AggregateKey(source=MIXED_SRC, agg="weighted_avg",
                           kwargs=(("weight", REGION_SUM),))
#: a plain row aggregation — no attached inputs at all.
PLAIN = AggregateKey(source=AMOUNT, agg="sum")


class TestAttachedInputs:
    def test_over_source(self):
        assert attached_inputs(MIXED) == [AVG_UP_PRODUCT]

    def test_over_kwargs(self):
        assert attached_inputs(ROW_PARAM) == [REGION_SUM]

    def test_over_positional_args(self):
        assert attached_inputs(POS_PARAM) == [REGION_SUM]

    def test_source_and_parameter_deduped(self):
        """Source constituent + attached parameter — both, order-stable."""
        assert attached_inputs(MIXED_PARAM) == [AVG_UP_PRODUCT, REGION_SUM]

    def test_repeated_input_appears_once(self):
        root = AggregateKey(source=AVG_UP_PRODUCT, agg="weighted_avg",
                            kwargs=(("weight", AVG_UP_PRODUCT),))
        assert attached_inputs(root) == [AVG_UP_PRODUCT]

    def test_plain_source_has_none(self):
        assert attached_inputs(PLAIN) == []


class TestReaggregationIsPure:
    def test_attached_source_is_reaggregation(self):
        assert is_reaggregation_key(PURE) is True

    def test_mixed_source_is_not_reaggregation(self):
        assert is_reaggregation_key(MIXED) is False

    def test_row_source_with_attached_param_is_not_reaggregation(self):
        assert is_reaggregation_key(ROW_PARAM) is False

    def test_literal_source_with_param_is_not_reaggregation(self):
        assert is_reaggregation_key(LIT_PARAM) is False


class TestRowAttachRoot:
    def test_mixed_source(self):
        assert is_row_attach_root(MIXED) is True

    def test_row_source_with_attached_parameter(self):
        assert is_row_attach_root(ROW_PARAM) is True

    def test_literal_source_with_attached_parameter(self):
        assert is_row_attach_root(LIT_PARAM) is True

    def test_row_source_with_positional_attached_parameter(self):
        assert is_row_attach_root(POS_PARAM) is True

    def test_pure_reaggregation_is_not_a_row_attach_root(self):
        assert is_row_attach_root(PURE) is False

    def test_plain_aggregation_is_not_a_row_attach_root(self):
        assert is_row_attach_root(PLAIN) is False


class TestAttachedOperandKeys:
    def test_covers_source_and_parameter_inners(self):
        keys = attached_operand_keys([MIXED_PARAM])
        assert AVG_UP_PRODUCT in keys
        assert REGION_SUM in keys

    def test_covers_pure_reaggregation_operands(self):
        assert AVG_UP_PRODUCT in attached_operand_keys([PURE])

    def test_empty_for_a_plain_aggregation(self):
        assert attached_operand_keys([PLAIN]) == frozenset()


class TestWalkConsumerKeys:
    def test_opaque_below_a_roots_inputs(self):
        """The root is yielded; its source constituent is not (it belongs to the
        root) — unlike ``walk_value_keys``, which descends into it."""
        seen = set(walk_consumer_keys(MIXED))
        assert MIXED in seen
        assert AVG_UP_PRODUCT not in seen
        assert AVG_UP_PRODUCT in set(walk_value_keys(MIXED))

    def test_parameter_input_is_opaque(self):
        seen = set(walk_consumer_keys(MIXED_PARAM))
        assert REGION_SUM not in seen
        assert AVG_UP_PRODUCT not in seen

    def test_partition_keys_stay_visible(self):
        """An attach-carrying computed dimension in partition_by= is NOT an
        input — it stays visible so its own attach is still planned."""
        pb_inner = AggregateKey(source=AMOUNT, agg="sum",
                                partition_keys=Grain.of([CITY]))
        band = ArithmeticKey(op=">", operands=(pb_inner, LiteralKey(value=100)))
        root = AggregateKey(source=MIXED_SRC, agg="sum",
                            partition_keys=Grain.of([band]))
        seen = set(walk_consumer_keys(root))
        assert pb_inner in seen           # reached through the partition key
        assert AVG_UP_PRODUCT not in seen  # still opaque below the source
