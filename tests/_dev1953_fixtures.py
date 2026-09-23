"""Shared fixtures for DEV-1953 — transform ``partition_by=`` binding and
operand-grain membership over the DEV-1847 sales graph."""

from __future__ import annotations

from collections import defaultdict
from typing import Callable, Dict, Optional, Tuple

import pytest

from tests._dev1847_fixtures import SPEND_BAND_EXPR, ModelMeasure, _SALES_ROWS_WIDE

UREG = {"expression": "upper(region)", "name": "ureg"}
BAND = {"expression": SPEND_BAND_EXPR, "name": "spend_band"}
AMOUNT = ModelMeasure(formula="amount:sum", name="a")


def cell_totals(key: Callable[[tuple], Tuple]) -> Dict[Tuple, Optional[float]]:
    """sum(amount) per ``key(row)``; an all-NULL cell is NULL."""
    acc: Dict[Tuple, Optional[float]] = {}
    for row in _SALES_ROWS_WIDE:
        k, a = key(row), row[4]
        prev = acc.get(k)
        acc[k] = prev if a is None else (prev or 0.0) + a
    return acc


def rank_within(totals: Dict[Tuple, Optional[float]]) -> Dict[Tuple, int]:
    """Descending competition rank within ``k[0]``; NULL ranks last."""
    groups: Dict = defaultdict(dict)
    for k, v in totals.items():
        groups[k[0]][k] = v
    out = {}
    for cells in groups.values():
        nonnull = [v for v in cells.values() if v is not None]
        for k, v in cells.items():
            out[k] = (1 + len(nonnull)) if v is None else 1 + sum(1 for x in nonnull if x > v)
    return out


def band_of() -> Dict[Tuple, str]:
    """spend_band per (city, region) cell."""
    return {k: "hi" if (v is not None and v > 45) else "lo"
            for k, v in cell_totals(lambda r: (r[2], r[1])).items()}


def approx_map(got: Dict, want: Dict) -> None:
    assert set(got) == set(want), (set(got), set(want))
    for k, v in want.items():
        if v is None:
            assert got[k] is None, k
        else:
            assert float(got[k]) == pytest.approx(v), k
