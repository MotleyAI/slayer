"""Every document literal the vocabulary rename respelled in the tests: ``main``'s spelling converts to exactly the
respelled literal, with a deprecation, and the respelled one converts silently.

Spec: compatibility/ossie-vocabulary › Pre-2.0 spellings in fresh input convert with a deprecation.
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from slayer.core.deprecation import DeprecatedInputFutureWarning
from slayer.core.models import Aggregation, DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.query import (
    DatasetExtension, OrderItem, QueryDimension, QueryMetric, QueryRefinement, SlayerQuery, TimeDimension,
)

CORPUS = Path(__file__).parent / "fixtures" / "vocabulary_corpus"
INDEX = json.loads((CORPUS / "_index.json").read_text())["files"]
KINDS: dict[str, type[BaseModel]] = {
    "dataset": SlayerDataset, "field": DatasetField, "metric": DatasetMetric, "relationship": DatasetRelationship,
    "aggregation": Aggregation, "query": SlayerQuery, "refinement": QueryRefinement, "time_dimension": TimeDimension,
    "order_item": OrderItem, "extension": DatasetExtension, "query_metric": QueryMetric, "dimension": QueryDimension,
}


def converted(kind: str, value: Any) -> tuple[BaseModel, set[tuple[str, str]]]:
    """``value`` validated as ``kind``, and the ``(old, new)`` spellings its conversion reported."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        model = KINDS[kind].model_validate(value)
    reported = {(w.message.payload.old, w.message.payload.new) for w in caught
                if isinstance(w.message, DeprecatedInputFutureWarning)}
    return model, reported


def problem(pair: dict[str, Any]) -> str | None:
    old, old_reported = converted(pair["kind"], pair["old"])
    new, new_reported = converted(pair["kind"], pair["new"])
    if old != new:
        return "converts to something else"
    if new_reported:
        return f"the respelled literal reports {sorted(new_reported)}"
    if not old_reported:
        return "converts without a deprecation"
    unspelled = sorted(o for o, _ in old_reported if f'"{o}"' not in json.dumps(pair["old"]))
    return f"reports spellings it does not contain: {unspelled}" if unspelled else None


@pytest.mark.parametrize("name", sorted(e["corpus"] for e in INDEX if "corpus" in e), ids=lambda n: n.removesuffix(".json"))
def test_each_old_literal_converts_to_its_respelled_counterpart(name: str) -> None:
    pairs = json.loads((CORPUS / name).read_text())["pairs"]
    problems = [(i, p["kind"], reason) for i, p in enumerate(pairs) if (reason := problem(p))]
    assert not problems, problems


def test_the_index_accounts_for_every_corpus_file() -> None:
    listed = {e["corpus"] for e in INDEX if "corpus" in e}
    assert listed == {p.name for p in CORPUS.glob("*.json") if p.name != "_index.json"}
    assert all(("corpus" in e) != ("reason" in e) for e in INDEX)
