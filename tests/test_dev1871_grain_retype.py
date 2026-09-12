"""DEV-1871 — ``partition_keys`` retyped to ``Grain``; the locus rename.

Design D6/D7: ``AggregateKey.partition_keys: Optional[Grain]`` (None = inherit
context, ``Grain.EMPTY`` = explicitly scalar), ``TransformKey.partition_keys:
Grain``, strict (no set-like coercion); the ``"target" | "host"`` literal moves
from ``AggregateKey.grain`` to ``AggregateKey.locus``.
"""

from __future__ import annotations

import pytest

from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    TransformKey,
    reroot_value_key,
    substitute_value_keys,
)
from slayer.ir.grain import Grain

_COL_A = ColumnKey(leaf="a")
_COL_B = ColumnKey(leaf="b")


def _agg(**kw) -> AggregateKey:
    return AggregateKey(source=ColumnKey(leaf="amount"), agg="sum", **kw)


class TestLocusRename:
    def test_locus_replaces_the_grain_field(self) -> None:
        key = _agg()
        assert key.locus == "target"
        assert not hasattr(key, "grain")

    def test_locus_participates_in_identity(self) -> None:
        assert _agg(locus="host") != _agg()


class TestGrainTypedPartitionKeys:
    def test_aggregate_key_carries_a_grain(self) -> None:
        key = _agg(partition_keys=Grain.of({_COL_A}))
        assert isinstance(key.partition_keys, Grain)
        assert key.partition_keys == Grain.of({_COL_A})

    def test_raw_set_likes_are_rejected(self) -> None:
        with pytest.raises(ValueError):
            _agg(partition_keys=frozenset({_COL_A}))
        with pytest.raises(ValueError):
            _agg(partition_keys=[_COL_A])
        with pytest.raises(ValueError):
            TransformKey(op="rank", input=_COL_A, partition_keys=frozenset({_COL_B}))

    def test_none_stays_distinct_from_explicit_scalar(self) -> None:
        inherited = _agg(partition_keys=None)
        scalar = _agg(partition_keys=Grain.EMPTY)
        assert inherited.partition_keys is None
        assert scalar.partition_keys == Grain.EMPTY
        assert inherited != scalar

    def test_transform_key_grain_is_total(self) -> None:
        key = TransformKey(op="rank", input=_COL_A)
        assert key.partition_keys == Grain.EMPTY
        with pytest.raises(ValueError):
            TransformKey(op="rank", input=_COL_A, partition_keys=None)


class TestTraversalLaw:
    def test_reroot_preserves_the_grain_type(self) -> None:
        part = ColumnKey(path=("customers",), leaf="tier")
        key = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="spend"), agg="sum",
            partition_keys=Grain.of({part}),
        )
        rerooted = reroot_value_key(key, target_path=("customers",))
        assert isinstance(rerooted.partition_keys, Grain)
        assert rerooted.partition_keys == Grain.of({ColumnKey(leaf="tier")})

    def test_substitute_preserves_the_grain_type(self) -> None:
        key = _agg(partition_keys=Grain.of({_COL_A}))
        swapped = substitute_value_keys(key, {_COL_A: _COL_B})
        assert isinstance(swapped.partition_keys, Grain)
        assert swapped.partition_keys == Grain.of({_COL_B})

    def test_map_children_preserves_the_grain_type(self) -> None:
        key = _agg(partition_keys=Grain.of({_COL_A}))
        assert key.map_children(lambda c: c) is key
        mapped = key.map_children(lambda c: _COL_B if c == _COL_A else c)
        assert isinstance(mapped.partition_keys, Grain)
        assert mapped.partition_keys == Grain.of({_COL_B})

    def test_transform_traversal_preserves_the_grain_type(self) -> None:
        key = TransformKey(
            op="rank", input=_COL_A, partition_keys=Grain.of({_COL_A}),
        )
        swapped = substitute_value_keys(key, {_COL_A: _COL_B})
        assert isinstance(swapped.partition_keys, Grain)
        assert swapped.partition_keys == Grain.of({_COL_B})
        assert swapped.input == _COL_B

    def test_partition_members_stay_children(self) -> None:
        key = _agg(partition_keys=Grain.of({_COL_A}))
        assert isinstance(key.partition_keys, Grain)
        assert _COL_A in key.children()
        tkey = TransformKey(op="rank", input=_COL_B, partition_keys=Grain.of({_COL_A}))
        assert isinstance(tkey.partition_keys, Grain)
        assert _COL_A in tkey.children()


class TestMemoStability:
    def test_equal_contents_intern_to_one_slot(self) -> None:
        k1 = _agg(partition_keys=Grain.of({_COL_A, _COL_B}))
        k2 = _agg(partition_keys=Grain.of({_COL_B, _COL_A}))
        assert isinstance(k1.partition_keys, Grain)
        assert k1 == k2
        assert hash(k1) == hash(k2)
        assert len({k1, k2}) == 1
        assert k1 != _agg(partition_keys=Grain.of({_COL_A}))

    def test_grain_hashes_like_its_key_set(self) -> None:
        keys = frozenset({_COL_A, _COL_B})
        key = _agg(partition_keys=Grain.of(keys))
        assert isinstance(key.partition_keys, Grain)
        assert hash(key.partition_keys) == hash(keys)
        tkey = TransformKey(op="rank", input=_COL_A, partition_keys=Grain.of(keys))
        assert isinstance(tkey.partition_keys, Grain)
        assert hash(tkey.partition_keys) == hash(keys)
