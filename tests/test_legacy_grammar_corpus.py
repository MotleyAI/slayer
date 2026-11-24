"""The stored-text gate agrees with 1.9.1's reader on the recorded corpus (spec: models/stored-upgrade)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from slayer.storage.legacy_grammar import is_old_grammar

_CORPUS = yaml.safe_load((Path(__file__).parent / "fixtures" / "legacy_grammar_v1_9_1.yaml").read_text(encoding="utf-8"))
_CASES = _CORPUS["cases"]


def _id(case: dict) -> str:
    return f"{case['surface']}:{case['text']!r}"


def test_recorded_from_1_9_1() -> None:
    assert _CORPUS["provenance"]["version"] == "1.9.1"


@pytest.mark.parametrize("case", _CASES, ids=_id)
def test_gate_verdict(case: dict) -> None:
    assert is_old_grammar(case["text"], filter=case["surface"] == "filter") is case["gate"]


@pytest.mark.parametrize("case", [c for c in _CASES if c["v191"] == "accepted"], ids=_id)
def test_text_1_9_1_parsed_is_held(case: dict) -> None:
    assert case["gate"] is True


def test_corpus_covers_both_semantic_rejection_verdicts() -> None:
    semantic = [c for c in _CASES if c["v191"] == "refused" and c["gate"]]
    refused = [c for c in _CASES if c["v191"] == "refused" and not c["gate"]]
    assert {c["surface"] for c in semantic} == {"filter", "expression"}
    assert {c["surface"] for c in refused} == {"filter", "expression"}
