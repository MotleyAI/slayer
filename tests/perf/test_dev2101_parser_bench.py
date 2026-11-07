"""Mode-B parser benchmark over the longest formulas in the tests and docs (recorded, not gating).

Run with: poetry run pytest tests/perf/test_dev2101_parser_bench.py --benchmark-only
``dev2101_old_parser_baseline.json`` holds the pre-grammar parser's µs per parse on the same formulas.
"""

import json
from pathlib import Path

import pytest

from slayer.core.syntax import parse_expr

BASELINE_US: dict[str, float] = json.loads((Path(__file__).parent / "dev2101_old_parser_baseline.json").read_text())
FORMULAS = list(BASELINE_US)


@pytest.mark.parametrize("formula", FORMULAS, ids=[f"f{i}" for i in range(len(FORMULAS))])
def test_parse(benchmark, formula: str) -> None:
    uncached = getattr(parse_expr, "__wrapped__", parse_expr)
    benchmark(uncached, formula)
    benchmark.extra_info["old_parser_us"] = BASELINE_US[formula]
    benchmark.extra_info["ratio_vs_old"] = benchmark.stats.stats.mean * 1e6 / BASELINE_US[formula]
