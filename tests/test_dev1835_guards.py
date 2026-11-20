"""Guard surface (design D7) — both directions.

Deleted: the windowed/ranked coexistence arm, G4/G5/G6/G7 +
the post-projection mixed-filter twin, ``time_shift``-over-ranked, and the
residual windowed/first-last union-grain guard — asserted absent from
the package sources. Preserved verbatim: G8 (duration syntax), G2 (time
resolution), and the ranked no-ranking-column error.
Resolved: G3 (windowed cross-dataset) is now a precise attributability
error naming the unreachable time dimension, no longer a deferral.

Scenario coverage map (spec: openspec …/specs/queries/computed-dimensions):
  The lifted windowed/ranked guard leaves no residue . TestDeletedGuardResidue
  Cross-dataset metrics still guarded ................ (tests/test_dev1837_guards.py)
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField

from slayer.core.query import QueryMetric

from tests._dev1835_fixtures import month_td, q
from tests._dev1739_fixtures import dev1739_datasets
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

G8_MALFORMED = (
    "WindowDurationError: Invalid window duration '90x'.\n"
    "  suggestion: Use syntax like '1y2m3w5d6h7min8s'."
)
G2_MESSAGE = (
    "TimeAxisError: Windowed metric could not resolve its time dimension.\n"
    "  suggestion: Add a single time_dimensions entry, or set main_time_dimension "
    "to select among multiple time dimensions."
)
NO_RANKING_COLUMN = (
    "first/last aggregation requires a ranking time field "
    "(a time_dimension, a DATE/TIMESTAMP dimension, or the "
    "dataset's default_time_dimension); none is resolvable for "
    "dataset 'orders'."
)


async def _gen(query, *, datasets=None) -> str:
    datasets = datasets or dev1739_datasets()
    return await _engine_generate(
        query=query, dataset=datasets[0], extra_datasets=datasets[1:],
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
    async def test_g8_malformed_duration(self) -> None:
        query = q(
            dimensions=["orders.region"], time_dimensions=month_td(),
            metrics=[QueryMetric(metric="sum(orders.amount, window='90x')", alias="w")],
        )
        with pytest.raises(ValueError) as ei:
            await _gen(query)
        assert str(ei.value) == G8_MALFORMED

    async def test_g2_no_resolvable_time_dimension(self) -> None:
        query = q(
            dimensions=["orders.region"],
            metrics=[QueryMetric(metric="sum(orders.amount, window='90d')", alias="w")],
        )
        with pytest.raises(ValueError) as ei:
            await _gen(query)
        assert str(ei.value) == G2_MESSAGE

    async def test_ranked_no_ranking_column(self) -> None:
        models = dev1739_datasets()
        # A second temporal field leaves no sole-field default either.
        models[0] = models[0].model_copy(update={
            "default_time_dimension": None,
            "fields": [*models[0].fields, DatasetField(name="updated_at", sql="ordered_at", datatype=DataType.DATETIME)],
        })
        query = q(
            dimensions=["orders.region"],
            metrics=[QueryMetric(metric="last(orders.amount)", alias="l")],
        )
        with pytest.raises(ValueError) as ei:
            await _gen(query, datasets=models)
        assert str(ei.value) == NO_RANKING_COLUMN


class TestRepointedGuards:
    async def test_g3_windowed_cross_model_needs_attributable_time_dimension(
        self,
    ) -> None:
        # The former blanket G3 deferral resolves into a precise
        # attributability error: the active time dimension is a host field,
        # unreachable from the customers root.
        query = q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            metrics=[QueryMetric(
                metric="sum(customers.spend, window='90d')", alias="w",
            )],
            to_many_handling="broadcast",
        )
        with pytest.raises(ValueError, match=r"attributable from") as ei:
            await _gen(query)
        msg = str(ei.value)
        assert "date_trunc('month', ordered_at)" in msg
        assert "DEV-1504" not in msg
