"""DEV-1859 task 4.2 — the recursive determination arm (design decision 12).
``grain_determines`` on an aggregate is True iff the grain determines EACH of
its partition keys: a grain member, a to-one-reached column, or an
aggregate-valued key whose own grain is determined; an expression-valued
partition key is determined only as an exact grain member.

The aggregate arm today tests partition-key MEMBERSHIP only, so the recursive
cases are red until §6.1 lands; the negatives are stable regression pins.

Spec: openspec …/specs/queries/partitioned-aggregates — "Attached parameters on
row-level sources" (determination rule); queries/semantics — "Ungrained
aggregate parameters type at the query grain".
"""

from __future__ import annotations

from typing import List

from decimal import Decimal

from slayer.core.keys import AggregateKey, ArithmeticKey, ColumnKey, Grain, LiteralKey
from slayer.core.models import SlayerModel
from slayer.engine.join_safety import grain_determines

from tests._dev1840_fixtures import dev1840_models
from tests._dev1847_fixtures import dev1847_models


def _mbn(models: List[SlayerModel]) -> dict:
    return {m.name: m for m in models}


class TestRecursiveAggregateArm:
    """An aggregate parameter grained by a determined key is determined."""

    def test_to_one_reached_partition_key_over_entity_seed(self):
        """`sum(amount, partition_by=customers.regions.name)` against
        `{status, customers.id}` — the entity key seeds the to-one region."""
        models = dev1840_models()
        mbn = _mbn(models)
        key = AggregateKey(
            source=ColumnKey(leaf="amount"), agg="sum",
            partition_keys=Grain.of(
                [ColumnKey(path=("customers", "regions"), leaf="name")]))
        grain = Grain.of(
            [ColumnKey(leaf="status"), ColumnKey(path=("customers",), leaf="id")])
        assert grain_determines(
            key=key, grain=grain, host_model=mbn["orders"],
            models_by_name=mbn) is True

    def test_aggregate_valued_partition_key_within_grain(self):
        """A partition key that is itself `sum(amount, partition_by=region)` is
        determined when the grain determines region."""
        models = dev1847_models()
        mbn = _mbn(models)
        inner = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum",
                             partition_keys=Grain.of([ColumnKey(leaf="region")]))
        outer = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum",
                             partition_keys=Grain.of([inner]))
        grain = Grain.of([ColumnKey(leaf="region")])
        assert grain_determines(
            key=outer, grain=grain, host_model=mbn["sales"],
            models_by_name=mbn) is True

    def test_exact_expression_partition_key_is_a_member(self):
        """An expression-valued partition key is determined only as an exact
        grain member (conservative arm)."""
        band = ArithmeticKey(
            op=">",
            operands=(AggregateKey(source=ColumnKey(leaf="amount"), agg="sum",
                                   partition_keys=Grain.of([ColumnKey(leaf="city")])),
                      LiteralKey(value=Decimal(100))))
        models = dev1847_models()
        mbn = _mbn(models)
        key = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum",
                           partition_keys=Grain.of([band]))
        assert grain_determines(
            key=key, grain=Grain.of([band]), host_model=mbn["sales"],
            models_by_name=mbn) is True


class TestUndeterminedStaysConservative:
    """Stable regression pins — these must stay False before and after §6.1."""

    def test_plain_partition_key_outside_grain(self):
        """`sum(x, partition_by=product)` against `[city, region]` — product is
        not determined."""
        models = dev1847_models()
        mbn = _mbn(models)
        key = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum",
                           partition_keys=Grain.of([ColumnKey(leaf="product")]))
        grain = Grain.of([ColumnKey(leaf="city"), ColumnKey(leaf="region")])
        assert grain_determines(
            key=key, grain=grain, host_model=mbn["sales"],
            models_by_name=mbn) is False

    def test_aggregate_valued_partition_key_outside_grain(self):
        models = dev1847_models()
        mbn = _mbn(models)
        inner = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum",
                             partition_keys=Grain.of([ColumnKey(leaf="region")]))
        outer = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum",
                             partition_keys=Grain.of([inner]))
        assert grain_determines(
            key=outer, grain=Grain.of([ColumnKey(leaf="city")]),
            host_model=mbn["sales"], models_by_name=mbn) is False

    def test_non_member_expression_partition_key(self):
        band = ArithmeticKey(
            op=">",
            operands=(AggregateKey(source=ColumnKey(leaf="amount"), agg="sum",
                                   partition_keys=Grain.of([ColumnKey(leaf="city")])),
                      LiteralKey(value=Decimal(100))))
        models = dev1847_models()
        mbn = _mbn(models)
        key = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum",
                           partition_keys=Grain.of([band]))
        assert grain_determines(
            key=key, grain=Grain.of([ColumnKey(leaf="region")]),
            host_model=mbn["sales"], models_by_name=mbn) is False


class TestColumnArmControl:
    """The column arm already determines a to-one reach today — a positive
    control that must stay green (the aggregate arm builds on it)."""

    def test_to_one_column_over_entity_seed(self):
        models = dev1840_models()
        mbn = _mbn(models)
        name = ColumnKey(path=("customers", "regions"), leaf="name")
        grain = Grain.of(
            [ColumnKey(leaf="status"), ColumnKey(path=("customers",), leaf="id")])
        assert grain_determines(
            key=name, grain=grain, host_model=mbn["orders"],
            models_by_name=mbn) is True
