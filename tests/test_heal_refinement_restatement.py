"""A refinement measure restating a saved measure by its bare name (spec: queries/saved-query-refinement)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.errors import RefinementConflictError
from slayer.core.query import QueryRefinement, SlayerQuery, refine_query
from tests._query_heal_fixtures import BACKENDS, table
from tests._saved_query_refinement_fixtures import REVENUE, refine_engine

RESTATEMENTS = {
    "named": {"formula": "revenue", "name": "revenue"},
    "unnamed": {"formula": "revenue"},
    "shorthand": "revenue",
}
FIELD_VALUES = {"label": "Rev", "type": "DOUBLE", "description": "Paid revenue", "meta": {"unit": "usd"}}
OTHER_VALUES = {"label": "Revenue", "type": "INT", "description": "All revenue", "meta": {"unit": "eur"}}


def saved(*measures: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", "measures": list(measures)})


def merge(base: SlayerQuery, *measures: Any) -> SlayerQuery:
    return refine_query(saved=base, refinement=QueryRefinement.model_validate({"measures": list(measures)}))


def conflict(base: SlayerQuery, *measures: Any) -> str:
    with pytest.raises(RefinementConflictError) as info:
        merge(base, *measures)
    return str(info.value)


class TestRestatementCollapses:
    @pytest.mark.parametrize("form", sorted(RESTATEMENTS))
    def test_equals_refinement_without_it(self, form: str) -> None:
        base = saved(REVENUE)
        assert merge(base, RESTATEMENTS[form], "count(*)") == merge(base, "count(*)")

    @pytest.mark.parametrize("form", sorted(RESTATEMENTS))
    def test_picks_the_named_entry_among_several(self, form: str) -> None:
        base = saved({"formula": "count(*)", "name": "orders_n"}, REVENUE)
        assert merge(base, RESTATEMENTS[form]) == base

    @pytest.mark.parametrize("field", sorted(FIELD_VALUES))
    def test_equal_explicit_field_collapses(self, field: str) -> None:
        base = saved({**REVENUE, field: FIELD_VALUES[field]})
        assert merge(base, {"formula": "revenue", field: FIELD_VALUES[field]}) == base

    @pytest.mark.parametrize("field", sorted(FIELD_VALUES))
    def test_explicit_null_equal_to_unset_saved_field_collapses(self, field: str) -> None:
        base = saved(REVENUE)
        assert merge(base, {"formula": "revenue", "name": "revenue", field: None}) == base


class TestRestatementConflicts:
    @pytest.mark.parametrize("field", sorted(FIELD_VALUES))
    def test_differing_field(self, field: str) -> None:
        message = conflict(saved({**REVENUE, field: FIELD_VALUES[field]}), {"formula": "revenue", field: OTHER_VALUES[field]})
        assert "measures" in message
        assert "revenue" in message

    @pytest.mark.parametrize("field", sorted(FIELD_VALUES))
    def test_explicit_null_against_set_saved_field(self, field: str) -> None:
        message = conflict(saved({**REVENUE, field: FIELD_VALUES[field]}), {"formula": "revenue", field: None})
        assert "revenue" in message

    def test_another_entry_named_like_the_saved_one_still_conflicts(self) -> None:
        message = conflict(saved(REVENUE), RESTATEMENTS["named"], {"formula": "count(*)", "name": "revenue"})
        assert "revenue" in message

    def test_same_name_other_formula_still_conflicts(self) -> None:
        assert "revenue" in conflict(saved(REVENUE), {"formula": "count(*)", "name": "revenue"})


class TestNotARestatement:
    def test_other_name_appends(self) -> None:
        merged = merge(saved(REVENUE), {"formula": "revenue", "name": "r2"})
        assert [(m.name, m.formula) for m in merged.measures or []] == [("revenue", "sum(amount)"), ("r2", "revenue")]

    def test_bare_name_of_no_saved_entry_appends(self) -> None:
        merged = merge(saved(REVENUE), "amount_total")
        assert [(m.name, m.formula) for m in merged.measures or []] == [("revenue", "sum(amount)"), (None, "amount_total")]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("form", sorted(RESTATEMENTS))
async def test_executed_restatement_equals_refinement_without_it(backend: str, form: str) -> None:
    async with refine_engine(backend) as (eng, _):
        healed = await eng.execute("monthly_revenue", refine={"measures": [RESTATEMENTS[form], "count(*)"]})
        expected = await eng.execute("monthly_revenue", refine={"measures": ["count(*)"]})
    assert healed.columns == expected.columns
    assert table(healed, ordered=False) == table(expected, ordered=False)
    assert healed.sql == expected.sql


@pytest.mark.parametrize("backend", BACKENDS)
async def test_executed_restatement_with_differing_label_conflicts(backend: str) -> None:
    async with refine_engine(backend) as (eng, _):
        with pytest.raises(RefinementConflictError) as info:
            await eng.execute("monthly_revenue", refine={"measures": [{"formula": "revenue", "label": "Rev"}]})
    assert "measures" in str(info.value)
    assert "revenue" in str(info.value)
