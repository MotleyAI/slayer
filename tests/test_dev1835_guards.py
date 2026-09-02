"""DEV-1835 guard surface (design D7) — both directions.

Deleted: the DEV-1837 windowed/ranked coexistence arm, DEV-1504 G4/G5/G6/G7 +
the post-projection mixed-filter twin, ``time_shift``-over-ranked, and the
residual DEV-1839 windowed/first-last union-grain guard — asserted absent from
the package sources. Preserved verbatim: G1 (sum/avg only), G8 (duration
syntax), G2 (time resolution), and the ranked no-ranking-column error.
Resolved: G3 (windowed cross-model) is now a precise DEV-1836 attributability
error naming the unreachable time dimension, no longer a DEV-1504 deferral.

Scenario coverage map (spec: openspec …/specs/queries/computed-dimensions):
  The lifted windowed/ranked guard leaves no residue . TestDeletedGuardResidue
  Cross-model measures still guarded ................. (tests/test_dev1837_guards.py)
"""

from __future__ import annotations

import pytest

from tests._dev1835_fixtures import ModelMeasure, month_td, q
from tests._dev1739_fixtures import dev1739_models
from tests._engine_helpers import _engine_generate
from tests.test_dev1837_guards import ARM_WINDOWED_RANKED, _sources_containing

#: Messages the migration deletes — no source may still carry them.
DELETED_MESSAGE_FRAGMENTS = {
    "coexistence-arm": ARM_WINDOWED_RANKED,
    "G4": (
        "Windowed measures (window='…') combined with transforms are not yet "
        "supported (DEV-1504)."
    ),
    "G5": (
        "Windowed measures (window='…') inside arithmetic / composite / "
        "scalar expressions are not yet supported (DEV-1504)."
    ),
    "G6": (
        "Filtering on a windowed measure (window='…') requires that "
        "measure to also be selected (DEV-1504)."
    ),
    "G7": (
        "A single filter that mixes a windowed measure (window='…') with "
        "a plain aggregate is not yet supported (DEV-1504)."
    ),
    "post-projection-twin": (
        "with another predicate (a row column or a plain aggregate) "
        "is not yet supported (DEV-1504)."
    ),
    "time-shift-over-ranked": "time_shift over a ranked",
    # Kept within one source line — the guard is an f-string, whose per-line
    # ``f`` prefixes survive ``_normalized`` and break longer needles.
    "residual-union-guard": (
        "broadcasting a windowed / first / last aggregate across the"
    ),
}

G1_MESSAGE = (
    "Aggregation parameter 'window' is only supported for sum and avg, "
    "not '{agg}'."
)
G8_MALFORMED = "Invalid window duration '90x'. Use syntax like '1y2m3w5d6h7min8s'."
G2_MESSAGE = (
    "Windowed measure could not resolve its time dimension. Add a single "
    "time_dimensions entry, or set main_time_dimension to select among "
    "multiple time dimensions."
)
NO_RANKING_COLUMN = (
    "first/last aggregation requires a ranking time column "
    "(a time_dimension, a DATE/TIMESTAMP dimension, or the "
    "model's default_time_dimension); none is resolvable for "
    "model 'orders'."
)


async def _gen(query, *, models=None) -> str:
    models = models or dev1739_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:],
        dialect="duckdb", validate=False,
    )


class TestDeletedGuardResidue:
    @pytest.mark.parametrize(
        "name", sorted(DELETED_MESSAGE_FRAGMENTS), ids=str,
    )
    def test_deleted_message_has_no_remaining_references(self, name: str) -> None:
        offenders = _sources_containing(DELETED_MESSAGE_FRAGMENTS[name])
        assert not offenders, offenders


class TestPreservedGuardsVerbatim:
    @pytest.mark.parametrize("agg", ["max", "count"])
    async def test_g1_windowed_sum_avg_only(self, agg: str) -> None:
        query = q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula=f"amount:{agg}(window='90d')", name="w")],
        )
        with pytest.raises(ValueError) as ei:
            await _gen(query)
        assert str(ei.value) == G1_MESSAGE.format(agg=agg)

    async def test_g8_malformed_duration(self) -> None:
        query = q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum(window='90x')", name="w")],
        )
        with pytest.raises(ValueError) as ei:
            await _gen(query)
        assert str(ei.value) == G8_MALFORMED

    async def test_g2_no_resolvable_time_dimension(self) -> None:
        query = q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum(window='90d')", name="w")],
        )
        with pytest.raises(ValueError) as ei:
            await _gen(query)
        assert str(ei.value) == G2_MESSAGE

    async def test_ranked_no_ranking_column(self) -> None:
        models = dev1739_models()
        models[0] = models[0].model_copy(update={"default_time_dimension": None})
        query = q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:last", name="l")],
        )
        with pytest.raises(ValueError) as ei:
            await _gen(query, models=models)
        assert str(ei.value) == NO_RANKING_COLUMN


class TestRepointedGuards:
    async def test_g3_windowed_cross_model_needs_attributable_time_dimension(
        self,
    ) -> None:
        # DEV-1836 resolves the former blanket G3 deferral into a precise
        # attributability error: the active time dimension is a host column,
        # unreachable from the customers root.
        query = q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="customers.spend:sum(window='90d')", name="w",
            )],
        )
        with pytest.raises(ValueError, match=r"(?i)cross-model") as ei:
            await _gen(query)
        msg = str(ei.value)
        assert "ordered_at_month" in msg
        assert "DEV-1504" not in msg
