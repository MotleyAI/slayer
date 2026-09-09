"""LAW split-invariance (semantics.arc42.md §3 law 3, single-measure splits):
a term's denotation never depends on sibling measures — removing any one
measure leaves the group set and every shared cell value unchanged."""

from __future__ import annotations

import pytest

from tests._law_harness import (
    DEFERRAL_SITES,
    EXPECTED_RAISES,
    PAIRS_BY_GRAIN,
    SHAPES,
    execute_shape,
    keyed_rows,
    law_assert,
    law_params,
    make_law_engine,
    sample_pairs,
    sample_shapes,
    values_equal,
)


@pytest.fixture(params=law_params())
async def law_case(request):
    dialect, shape = request.param
    async for engine, _db_path in make_law_engine(dialect):
        yield engine, shape


async def test_removing_any_measure_preserves_groups_and_shared_cells(law_case):
    engine, shape = law_case
    full = await execute_shape(engine, shape)
    if full is None:
        return
    full_rows = keyed_rows(full, shape=shape)
    for drop in shape.measure_keys:
        sub = await execute_shape(engine, shape, drop=drop)
        if sub is None:
            continue
        sub_rows = keyed_rows(sub, shape=shape)
        law_assert(
            set(sub_rows) == set(full_rows),
            law="split-invariance",
            detail=f"group set moved when dropping {drop!r}: "
                   f"only-full={sorted(map(str, set(full_rows) - set(sub_rows)))}, "
                   f"only-sub={sorted(map(str, set(sub_rows) - set(full_rows)))}",
            shape=shape,
        )
        for key, row in sub_rows.items():
            for kept in shape.measure_keys:
                if kept == drop:
                    continue
                col = f"orders.m_{kept}"
                law_assert(
                    values_equal(a=row[col], b=full_rows[key][col]),
                    law="split-invariance",
                    detail=f"cell {key} measure {kept!r} changed when dropping "
                           f"{drop!r}: {row[col]!r} != {full_rows[key][col]!r}",
                    shape=shape,
                )


class TestHarnessSelfChecks:
    def test_law_assert_failures_name_the_law(self) -> None:
        with pytest.raises(AssertionError, match=r"LAW split-invariance violated"):
            law_assert(
                False, law="split-invariance", detail="detail", shape="shape",
            )

    def test_shape_and_pair_ids_are_stable_across_collections(self) -> None:
        assert sample_shapes() == SHAPES
        assert sample_pairs() == PAIRS_BY_GRAIN["rc"]
        assert sample_pairs(exclude=frozenset({"part_city"})) == PAIRS_BY_GRAIN["rm"]

    async def test_registry_expectation_is_strict(self, monkeypatch) -> None:
        """A registered expected raise that stops raising fails loudly — the
        registry can never silently outlive a lifted guard."""
        shape = SHAPES[0]
        monkeypatch.setitem(
            EXPECTED_RAISES, (shape.shape_id, None), DEFERRAL_SITES[0].fragment,
        )
        async for engine, _db_path in make_law_engine("sqlite"):
            with pytest.raises(pytest.fail.Exception, match="DID NOT RAISE"):
                await execute_shape(engine, shape)
