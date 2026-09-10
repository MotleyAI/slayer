"""Carrier warnings must survive cross-process transport.

pytest-xdist forwards each worker's warnings to the controller by
reconstructing the category via ``cls(*message.args)``. A carrier whose
``args`` don't mirror its constructor params crashes the controller
(uncaught ``AttributeError``) or degrades lossily (caught ``TypeError``),
so the whole parallel run aborts. These tests pin the ``args`` contract
and the real xdist serialize/unserialize path.
"""

import warnings

import pytest
from xdist.remote import serialize_warning_message
from xdist.workermanage import unserialize_warning_message

from slayer.core.errors import (
    AssociatedGrainWarning,
    BroadcastGrainWarning,
    UnreachableFilterDroppedWarning,
)
from slayer.core.warnings import NormalizationWarning, SlayerNormalizationWarning


def _carriers() -> list[UserWarning]:
    nw = NormalizationWarning(
        rule_id="FUNC_STYLE_AGG",
        original="sum(revenue)",
        normalized="revenue:sum",
        location="measures[0].formula",
    )
    return [
        BroadcastGrainWarning(measure="orders.revenue_sum", reason="no join path"),
        AssociatedGrainWarning(
            measure="orders.revenue_sum", dimensions="customers.region"
        ),
        UnreachableFilterDroppedWarning(
            filter_text="customers.score > 5", reason="unreachable from CTE root"
        ),
        SlayerNormalizationWarning(nw),
    ]


@pytest.mark.parametrize("w", _carriers(), ids=lambda w: type(w).__name__)
def test_carrier_reconstructs_from_its_args(w: UserWarning) -> None:
    """Exception contract: ``type(w)(*w.args)`` rebuilds an equivalent instance."""
    clone = type(w)(*w.args)
    assert str(clone) == str(w)


@pytest.mark.parametrize("w", _carriers(), ids=lambda w: type(w).__name__)
def test_carrier_survives_xdist_roundtrip(w: UserWarning) -> None:
    """The pytest-xdist worker→controller path must not raise, and must keep
    the category and the human-readable text."""
    wm = warnings.WarningMessage(
        message=w, category=type(w), filename="f.py", lineno=1
    )
    restored = unserialize_warning_message(serialize_warning_message(wm))
    assert restored.category is type(w)
    assert str(w) in str(restored.message)
