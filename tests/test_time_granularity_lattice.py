"""DEV-1471 task 1.1 — the ``TimeGranularity.nests_into`` lattice.

The full 9×9 matrix: reflexive, the four nesting chains, transitive closure,
and week/month + week/week_sunday rejected in both directions. ``a.nests_into(b)``
is True iff ``a``'s buckets tile ``b`` exactly (``a`` finer-or-equal, aligned).
Fails until 2.1 adds the method.
"""

from __future__ import annotations

import itertools

from slayer.core.enums import TimeGranularity

# Direct "nests into" edges (finer → the coarser bucket it tiles).
_EDGES = [
    (TimeGranularity.SECOND, TimeGranularity.MINUTE),
    (TimeGranularity.MINUTE, TimeGranularity.HOUR),
    (TimeGranularity.HOUR, TimeGranularity.DAY),
    (TimeGranularity.DAY, TimeGranularity.WEEK),
    (TimeGranularity.DAY, TimeGranularity.WEEK_SUNDAY),
    (TimeGranularity.DAY, TimeGranularity.MONTH),
    (TimeGranularity.MONTH, TimeGranularity.QUARTER),
    (TimeGranularity.QUARTER, TimeGranularity.YEAR),
]


def _expected_nesting() -> set[tuple[TimeGranularity, TimeGranularity]]:
    """Reflexive-transitive closure of ``_EDGES`` — the ground truth, computed
    independently of the implementation."""
    reach = {(g, g) for g in TimeGranularity}
    reach.update(_EDGES)
    changed = True
    while changed:
        changed = False
        for (a, b), (c, d) in itertools.product(list(reach), repeat=2):
            if b == c and (a, d) not in reach:
                reach.add((a, d))
                changed = True
    return reach


def test_full_9x9_matrix_matches_closure() -> None:
    expected = _expected_nesting()
    for a, b in itertools.product(TimeGranularity, repeat=2):
        assert a.nests_into(b) is ((a, b) in expected), (
            f"{a.value}.nests_into({b.value}) should be {(a, b) in expected}"
        )


def test_reflexive() -> None:
    for g in TimeGranularity:
        assert g.nests_into(g) is True


def test_chains() -> None:
    assert TimeGranularity.SECOND.nests_into(TimeGranularity.DAY) is True  # via minute→hour
    assert TimeGranularity.SECOND.nests_into(TimeGranularity.YEAR) is True  # full chain
    assert TimeGranularity.DAY.nests_into(TimeGranularity.WEEK) is True
    assert TimeGranularity.DAY.nests_into(TimeGranularity.WEEK_SUNDAY) is True
    assert TimeGranularity.MONTH.nests_into(TimeGranularity.YEAR) is True  # via quarter
    assert TimeGranularity.QUARTER.nests_into(TimeGranularity.YEAR) is True


def test_finer_rejected() -> None:
    assert TimeGranularity.MONTH.nests_into(TimeGranularity.DAY) is False
    assert TimeGranularity.YEAR.nests_into(TimeGranularity.MONTH) is False
    assert TimeGranularity.DAY.nests_into(TimeGranularity.HOUR) is False


def test_week_month_rejected_both_ways() -> None:
    assert TimeGranularity.WEEK.nests_into(TimeGranularity.MONTH) is False
    assert TimeGranularity.MONTH.nests_into(TimeGranularity.WEEK) is False
    assert TimeGranularity.WEEK.nests_into(TimeGranularity.YEAR) is False
    assert TimeGranularity.WEEK.nests_into(TimeGranularity.QUARTER) is False


def test_week_week_sunday_rejected_both_ways() -> None:
    assert TimeGranularity.WEEK.nests_into(TimeGranularity.WEEK_SUNDAY) is False
    assert TimeGranularity.WEEK_SUNDAY.nests_into(TimeGranularity.WEEK) is False
