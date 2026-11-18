"""DEV-1871 — typed terms in ``slayer.ir.terms`` (design D1–D3).

Terms annotate keys: they add only the resolved home dataset and resolved
total grain; ``Aggregate.recipe`` references the ``AggregateKey``. The
``Dataset`` protocol is the aggregate-is-a-dataset axiom as subtyping.
"""

from __future__ import annotations

import pytest

from slayer.core.keys import AggregateKey, FieldKey, TransformKey
from slayer.core.keys import Grain

from slayer.ir.terms import (
    Aggregate,
    Broadcast,
    Dataset,
    Field,
    StoredDataset,
    StageDataset,
    Transform,
)

_STATUS = FieldKey(leaf="status")
_TIER = FieldKey(leaf="tier")


def _dataset_ds() -> "StoredDataset":
    return StoredDataset(data_source="ds", dataset_name="orders")


def _recipe() -> AggregateKey:
    return AggregateKey(source=FieldKey(leaf="amount"), agg="sum")


def _aggregate(*, grain: Grain | None = None) -> "Aggregate":
    return Aggregate(
        home=_dataset_ds(), recipe=_recipe(),
        grain=Grain.of({_STATUS}) if grain is None else grain,
    )


class TestDatasetIdentity:
    def test_model_dataset_identity(self) -> None:
        a = StoredDataset(data_source="ds", dataset_name="orders")
        b = StoredDataset(data_source="ds", dataset_name="orders")
        assert a == b
        assert hash(a) == hash(b)
        assert a != StoredDataset(data_source="ds", dataset_name="customers")
        assert a != StoredDataset(data_source="other", dataset_name="orders")
        assert len({a, b}) == 1

    def test_stage_dataset_identity(self) -> None:
        a = StageDataset(stage_name="s1")
        assert a == StageDataset(stage_name="s1")
        assert a != StageDataset(stage_name="s2")

    def test_aggregate_is_a_dataset(self) -> None:
        """The aggregate-is-a-dataset axiom as subtyping."""
        assert isinstance(_dataset_ds(), Dataset)
        assert isinstance(StageDataset(stage_name="s1"), Dataset)
        assert isinstance(_aggregate(), Dataset)
        # The protocol must be non-vacuous: arbitrary objects don't satisfy it.
        assert not isinstance(object(), Dataset)
        assert not isinstance(Grain.EMPTY, Dataset)

    def test_aggregate_identity(self) -> None:
        a = _aggregate()
        b = _aggregate()
        assert a == b
        assert hash(a) == hash(b)
        assert a != _aggregate(grain=Grain.of({_TIER}))
        assert a != Aggregate(
            home=StageDataset(stage_name="s1"), recipe=_recipe(),
            grain=Grain.of({_STATUS}),
        )

    def test_transform_identity_and_grain(self) -> None:
        home = _aggregate()
        a = Transform(input=home, recipe=TransformKey(op="rank", input=_recipe()))
        b = Transform(input=home, recipe=TransformKey(op="rank", input=_recipe()))
        assert a == b
        assert hash(a) == hash(b)
        assert a != Transform(
            input=home, recipe=TransformKey(op="dense_rank", input=_recipe()),
        )
        assert a.grain == home.grain


class TestConstructionInvariants:
    def test_aggregate_recipe_is_a_reference(self) -> None:
        recipe = _recipe()
        term = Aggregate(home=_dataset_ds(), recipe=recipe, grain=Grain.EMPTY)
        assert term.recipe is recipe

    def test_aggregate_grain_totality(self) -> None:
        home = _dataset_ds()
        recipe = _recipe()
        raw_set = frozenset()
        with pytest.raises(ValueError):
            Aggregate(home=home, recipe=recipe, grain=None)  # pyright: ignore[reportArgumentType] — the None rejection under test
        with pytest.raises(ValueError):
            Aggregate(home=home, recipe=recipe, grain=raw_set)  # pyright: ignore[reportArgumentType] — the raw-set rejection under test

    def test_transform_time_axis_checked_at_construction(self) -> None:
        home = _aggregate()
        timeless = TransformKey(op="rank", input=_recipe())
        Transform(input=home, recipe=timeless)
        with_axis = TransformKey(
            op="cumsum", input=_recipe(), time_key=FieldKey(leaf="ordered_at"),
        )
        Transform(input=home, recipe=with_axis)
        axisless = TransformKey(op="cumsum", input=_recipe())
        with pytest.raises(ValueError):
            Transform(input=home, recipe=axisless)

    def test_broadcast_direction(self) -> None:
        """Broadcast is coarse → fine only; the reverse needs re-aggregation."""
        scalar = _aggregate(grain=Grain.EMPTY)
        fine = Grain.of({_STATUS})
        broadcast = Broadcast(source=scalar, into=fine)
        assert broadcast.grain == fine
        fine_agg = _aggregate(grain=fine)
        with pytest.raises(ValueError):
            Broadcast(source=fine_agg, into=Grain.EMPTY)
        tier_agg = _aggregate(grain=Grain.of({_TIER}))
        with pytest.raises(ValueError):
            Broadcast(source=tier_agg, into=fine)

    def test_field_carries_home_and_key(self) -> None:
        field = Field(home=_dataset_ds(), key=_STATUS)
        assert field.home == _dataset_ds()
        assert field.key == _STATUS

    def test_terms_are_frozen_and_hashable(self) -> None:
        term = _aggregate()
        hash(term)
        with pytest.raises(ValueError):
            term.grain = Grain.EMPTY  # pyright: ignore[reportAttributeAccessIssue] — the frozen violation under test
        dataset = _dataset_ds()
        with pytest.raises(ValueError):
            dataset.dataset_name = "other"  # pyright: ignore[reportAttributeAccessIssue] — the frozen violation under test
