"""Re-derive every outer-grain oracle from the raw fixture rows."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

import pytest

from tests._reagg_outer_grain_fixtures import (
    ACCOUNT_TOTALS,
    ASSOCIATED_MAX_Q_BY_MONTH,
    BAND_THRESHOLD,
    BROADCAST_MAX_Q_BY_MONTH,
    COUNT_MAX_Q_BY_ACCOUNT,
    CUSTOMERS_ROWS,
    DEPTH3_MAX_ACCOUNT_TOTAL_BY_NAME,
    KEYLESS_SUM_MAX_Q,
    RANK_OF_ACCOUNT_MAX,
    SNAPSHOT_ROWS,
    SUM_LAST_P,
    SUM_MAX_P,
    SUM_MAX_Q_BY_ACCOUNT,
    SUM_MAX_Q_BY_BAND,
    SUM_SUM_P,
    SUM_SUM_P_BY_MONTH,
    SUM_WINDOWED_Q,
    WAVG_BY_ACCOUNT_MAX,
)

NAME = dict(CUSTOMERS_ROWS)
ROWS = [
    {"account": a, "name": NAME[c], "date": date.fromisoformat(d), "month": d[:7], "balance": b}
    for _id, a, c, d, b in SNAPSHOT_ROWS
]
ACCOUNT_MAX = {a: max(r["balance"] for r in ROWS if r["account"] == a) for a in ACCOUNT_TOTALS}
OWNER = {r["account"]: r["name"] for r in ROWS}


def _cells_p():
    out = defaultdict(list)
    for r in sorted(ROWS, key=lambda r: r["date"]):
        out[(r["account"], r["name"], r["month"])].append(r["balance"])
    return out


def _outer_sum(pick):
    out = defaultdict(float)
    for (_a, name, mon), values in _cells_p().items():
        out[(name, mon)] += pick(values)
    return dict(out)


@pytest.mark.parametrize("pick,oracle", [
    (sum, SUM_SUM_P), (max, SUM_MAX_P), (lambda v: v[-1], SUM_LAST_P),
])
def test_bucket_inners(pick, oracle):
    assert _outer_sum(pick) == oracle


def test_windowed_inner():
    month_ends = {"2024-01": date(2024, 1, 31), "2024-02": date(2024, 2, 29)}
    out = defaultdict(float)
    for a in ACCOUNT_TOTALS:
        months = {r["month"] for r in ROWS if r["account"] == a}
        for mon in months:
            end = month_ends[mon]
            out[(OWNER[a], mon)] += sum(
                r["balance"] for r in ROWS
                if r["account"] == a and end - timedelta(days=60) < r["date"] <= end)
    assert dict(out) == SUM_WINDOWED_Q


CHILDLESS = set(NAME.values()) - set(OWNER.values())


def test_to_many_account_and_keyless():
    assert {**{(OWNER[a], a): v for a, v in ACCOUNT_MAX.items()},
            **{(c, None): None for c in CHILDLESS}} == SUM_MAX_Q_BY_ACCOUNT
    assert sum(ACCOUNT_MAX.values()) == KEYLESS_SUM_MAX_Q
    assert {**{a: 1 for a in ACCOUNT_MAX}, **({None: 0} if CHILDLESS else {})} \
        == COUNT_MAX_Q_BY_ACCOUNT


def test_month_only():
    by_month = defaultdict(float)
    for (_name, mon), v in SUM_SUM_P.items():
        by_month[mon] += v
    assert {**by_month, **({None: None} if CHILDLESS else {})} == SUM_SUM_P_BY_MONTH


def test_associated_and_broadcast_across_months():
    per_customer = defaultdict(float)
    for a, v in ACCOUNT_MAX.items():
        per_customer[OWNER[a]] += v
    cells_ = {(r["name"], r["month"]) for r in ROWS}
    associated = {
        (name, mon): sum(ACCOUNT_MAX[a] for a in {
            r["account"] for r in ROWS if r["name"] == name and r["month"] == mon})
        for name, mon in cells_
    }
    assert associated == ASSOCIATED_MAX_Q_BY_MONTH
    assert {k: per_customer[k[0]] for k in cells_} == BROADCAST_MAX_Q_BY_MONTH
    assert associated != BROADCAST_MAX_Q_BY_MONTH


def test_band():
    out = defaultdict(float)
    for a, v in ACCOUNT_MAX.items():
        out[(OWNER[a], "hi" if v > BAND_THRESHOLD else "lo")] += v
    assert dict(out) == SUM_MAX_Q_BY_BAND


def test_account_totals_depth_three_rank_and_wavg():
    assert {a: sum(r["balance"] for r in ROWS if r["account"] == a) for a in ACCOUNT_MAX} \
        == ACCOUNT_TOTALS
    best = defaultdict(float)
    for a, v in ACCOUNT_TOTALS.items():
        best[OWNER[a]] = max(best[OWNER[a]], v)
    assert {**best, **{c: None for c in CHILDLESS}} == DEPTH3_MAX_ACCOUNT_TOTAL_BY_NAME
    ranked = sorted(ACCOUNT_MAX, key=lambda a: ACCOUNT_MAX[a], reverse=True)
    assert {(OWNER[a], a): i + 1 for i, a in enumerate(ranked)} == RANK_OF_ACCOUNT_MAX
    for name, want in WAVG_BY_ACCOUNT_MAX.items():
        rows = [r for r in ROWS if r["name"] == name]
        weights = [ACCOUNT_MAX[r["account"]] for r in rows]
        got = sum(r["balance"] * w for r, w in zip(rows, weights)) / sum(weights)
        assert got == pytest.approx(want)
