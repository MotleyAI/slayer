"""DEV-1869 law harness — seeded-deterministic query shapes + shared helpers for
the ``tests/test_law_*.py`` files (the generative backend of the laws in
``architecture/semantics.arc42.md`` §3).

Builds on the DEV-1739/1824/1837 fixture stack: orders → customers → regions,
the 7-row dataset, ``DIM_FAMILY_DIMS`` and the attach + transform measure pool.
Everything is seeded (``LAW_SEED``) — no Hypothesis, no run-to-run drift; shape
ids are stable across collections.

Execution-count arithmetic (SQLite runs all shapes, DuckDB the ``[::5]`` slice
of 8): split-invariance ≤ (1 + 4)×(40 + 8) = 240 engine runs; grain-union
2×(40 + 8) = 96; broadcast coherence 2 runs × (24 + 6 slice) pairs per grain
× 2 grains = 120, plus the named chain/refusal cases; lowering soundness ~12.
≈ 480 total, all on tmpfile-backed engines.
"""

from __future__ import annotations

import os
import random
import re
import shutil
import sqlite3
import tempfile
from typing import Any, AsyncIterator, Optional, Sequence, Tuple

import pytest
from pydantic import BaseModel

from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1739_fixtures import _engine_for, _seed_duckdb, _seed_sqlite
from tests._dev1837_fixtures import (
    DIM_FAMILY_DIMS,
    TD_TRANSFORM_OPS,
    TRANSFORM_FORMULAS,
    ModelMeasure,
    SlayerQuery,
    dim_key,
    month_key,
    month_td,
    q,
)
from tests.test_dev1837_dimension_measure_matrix import ATTACH_MEASURES, TD_ATTACH

LAW_SEED = 1869
N_SHAPES = 40
N_DUCKDB = 8
N_PAIRS = 24
DUCKDB_PAIR_STRIDE = 4

#: The full measure pool: every attach family + every transform op.
MEASURE_POOL = {**ATTACH_MEASURES, **TRANSFORM_FORMULAS}

#: Row-level filter pool (tag → predicate; ``f0`` = unfiltered).
FILTERS = {
    "f0": None,
    "fr": "region = 'North'",
    "fs": "status = 'ok'",
    "fc": "customers.tier = 'gold'",
}

_MEASURE_ABBREV = {
    "plain": "pl", "part": "pt", "win_part": "wp", "last_part": "lp",
    "wm": "wm", "rk": "rk", "cm": "cm", "time_shift": "ts", "lag": "lg",
    "lead": "ld", "change": "ch", "change_pct": "cp", "cumsum": "cs",
    "consecutive_periods": "sp", "rank": "rn",
}

#: Explicit semantic grain of the broadcast-attach operands (grain-union law):
#: partition keys, plus the month bucket for ``window=`` operands; ``cm`` is the
#: grand cross-model total — the empty grain.
OPERAND_GRAIN = {
    "part": ("region",),
    "last_part": ("region",),
    "win_part": ("region", "month"),
    "cm": (),
}

#: Broadcast-coherence operand families (design D4) × arithmetic ops.
OPERANDS = {
    "plain": "amount:sum",
    "part_region": "amount:sum(partition_by=region)",
    "part_city": "amount:sum(partition_by=city)",
    "last_part": "amount:last(partition_by=region)",
    "win_part": "amount:sum(window='90d', partition_by=region)",
    "cm": "customers.spend:sum",
}
OPS = ("+", "-", "*")

#: Registry key marking the measure-free (dims-only) variant of a shape.
DIMS_ONLY = "<dims-only>"


class DeferralSite(BaseModel):
    """One enumerated fail-closed guard: a unique, ref-free message fragment
    plus the live issue its message must reference."""

    fragment: str
    issue: str


#: The exact fail-closed deferral list. The guard ratchet
#: (tests/test_law_guard_ratchet.py) pins this to ``guards.baseline`` in
#: architecture/index.yaml — the list may only ever shrink.
DEFERRAL_SITES: Tuple[DeferralSite, ...] = (
    DeferralSite(
        fragment="partition_by on a cross-model first/last aggregation is not yet supported",
        issue="DEV-1868"),
    DeferralSite(
        fragment="A cross-model partition_by aggregate nested inside a transform is not yet supported",
        issue="DEV-1868"),
    DeferralSite(
        fragment="cross-model aggregate operand inside an AGGREGATE-phase composite",
        issue="DEV-1868"),
    DeferralSite(
        fragment="must wrap an explicitly-grained aggregate",
        issue="DEV-1868"),
    DeferralSite(
        fragment="would require a nested attach",
        issue="DEV-1847"),
    DeferralSite(
        fragment="nested attach itself needs a further regroup producer CTE",
        issue="DEV-1847"),
    DeferralSite(
        fragment="nested attach grain is not a subset of the producer grain",
        issue="DEV-1847"),
    DeferralSite(
        fragment="query-backed models (source_queries) deferred",
        issue="DEV-1878"),
)

_FRAGMENTS = frozenset(site.fragment for site in DEFERRAL_SITES)


class LawShape(BaseModel):
    """One generated query shape: a dimension family, 2–4 pool measures, and a
    filter; the month time dimension is auto-forced by the FULL measure set so
    the grain never moves between a shape's variants."""

    shape_id: str
    dim_family: str
    measure_keys: Tuple[str, ...]
    filter: Optional[str]

    @property
    def with_month(self) -> bool:
        return any(
            k in TD_ATTACH or k in TD_TRANSFORM_OPS for k in self.measure_keys
        )

    def __str__(self) -> str:
        return (
            f"{self.shape_id}(family={self.dim_family}, "
            f"measures={list(self.measure_keys)}, filter={self.filter!r})"
        )


def _shape(idx: int, family: str, keys: Sequence[str], ftag: str) -> LawShape:
    pool_order = tuple(MEASURE_POOL)
    ordered = tuple(sorted(keys, key=pool_order.index))
    abbrev = "".join(_MEASURE_ABBREV[k] for k in ordered)
    return LawShape(
        shape_id=f"s{idx:02d}-{family}-{abbrev}-{ftag}",
        dim_family=family, measure_keys=ordered, filter=FILTERS[ftag],
    )


def sample_shapes() -> Tuple[LawShape, ...]:
    """Deterministic covering core (every family, measure, and filter; ≥1
    cross-model, ≥1 windowed), then seeded random fill to ``N_SHAPES``."""
    rng = random.Random(LAW_SEED)
    families = tuple(DIM_FAMILY_DIMS)
    measures = tuple(MEASURE_POOL)
    ftags = tuple(FILTERS)
    cores: list[tuple[str, Sequence[str], str]] = [
        (families[i % len(families)], measures[i * 3:i * 3 + 3], ftags[i % len(ftags)])
        for i in range(len(measures) // 3)
    ]
    for j in range(len(cores), len(families)):
        cores.append((families[j], ("plain", "cm"), ftags[j % len(ftags)]))
    shapes = [_shape(i, *core) for i, core in enumerate(cores)]
    for i in range(len(shapes), N_SHAPES):
        keys = rng.sample(measures, rng.randint(2, 4))
        shapes.append(_shape(i, rng.choice(families), keys, rng.choice(ftags)))
    _assert_covering(shapes)
    return tuple(shapes)


def _assert_covering(shapes: Sequence[LawShape]) -> None:
    assert {s.dim_family for s in shapes} == set(DIM_FAMILY_DIMS)
    assert {k for s in shapes for k in s.measure_keys} == set(MEASURE_POOL)
    assert {s.filter for s in shapes} == set(FILTERS.values())
    assert any("cm" in s.measure_keys for s in shapes)
    assert any(set(s.measure_keys) & {"wm", "win_part"} for s in shapes)


SHAPES = sample_shapes()
DUCKDB_SHAPE_IDS = frozenset(s.shape_id for s in SHAPES[::5])
assert len(DUCKDB_SHAPE_IDS) == N_DUCKDB


class CoherencePair(BaseModel):
    """One (a, b, op) broadcast-coherence triple over ``OPERANDS`` × ``OPS``."""

    pair_id: str
    a: str
    b: str
    op: str

    @property
    def with_month(self) -> bool:
        return "win_part" in (self.a, self.b)

    def __str__(self) -> str:
        return f"{self.pair_id}({OPERANDS[self.a]!r} {self.op} {OPERANDS[self.b]!r})"


def sample_pairs(
    *, exclude: frozenset = frozenset(),
) -> Tuple[CoherencePair, ...]:
    """Covering core (every operand family and every op), then seeded fill to
    ``N_PAIRS`` distinct ordered a ≠ b triples. ``exclude`` drops operand
    families ill-typed at the target grain — the law quantifies over
    well-typed terms only."""
    rng = random.Random(LAW_SEED)
    names = tuple(n for n in OPERANDS if n not in exclude)
    triples: list[tuple[str, str, str]] = [
        (names[i], names[(i + 1) % len(names)], OPS[i % len(OPS)])
        for i in range(len(names))
    ]
    seen = set(triples)
    while len(triples) < N_PAIRS:
        cand = (rng.choice(names), rng.choice(names), rng.choice(OPS))
        if cand[0] != cand[1] and cand not in seen:
            seen.add(cand)
            triples.append(cand)
    return tuple(
        CoherencePair(pair_id=f"p{i:02d}-{a}.{op}.{b}", a=a, b=b, op=op)
        for i, (a, b, op) in enumerate(triples)
    )


#: Per-grain pair samples: ``rc`` = (region, city) takes the full operand pool;
#: ``rm`` = (region, month) excludes part_city (city outside the query grain is
#: the axiom-6 typed refusal, pinned by a named coherence test instead).
PAIRS_BY_GRAIN = {
    "rc": sample_pairs(),
    "rm": sample_pairs(exclude=frozenset({"part_city"})),
}
DUCKDB_PAIR_IDS = {
    grain: frozenset(p.pair_id for p in pairs[::DUCKDB_PAIR_STRIDE])
    for grain, pairs in PAIRS_BY_GRAIN.items()
}


def law_params() -> list:
    """``(dialect, shape)`` params: SQLite over every shape, DuckDB over the
    explicit ``DUCKDB_SHAPE_IDS`` slice."""
    return [
        pytest.param((dialect, s), id=f"{dialect}-{s.shape_id}")
        for dialect in ("sqlite", "duckdb")
        for s in SHAPES
        if dialect == "sqlite" or s.shape_id in DUCKDB_SHAPE_IDS
    ]


def pair_params() -> list:
    """``(dialect, pair, grain)`` params; grains: ``rc`` = (region, city),
    ``rm`` = (region, month)."""
    return [
        pytest.param((dialect, p, grain), id=f"{dialect}-{grain}-{p.pair_id}")
        for dialect in ("sqlite", "duckdb")
        for grain, pairs in PAIRS_BY_GRAIN.items()
        for p in pairs
        if dialect == "sqlite" or p.pair_id in DUCKDB_PAIR_IDS[grain]
    ]


async def make_law_engine(
    dialect: str,
) -> AsyncIterator[Tuple[SlayerQueryEngine, str]]:
    """Seeded engine + the path of a pristine copy of the database for
    raw-oracle SQL (a separate file: DuckDB rejects a second in-process
    connection configuration on the engine's file)."""
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, f"data.{dialect}")
        (_seed_sqlite if dialect == "sqlite" else _seed_duckdb)(db_path)
        oracle_path = os.path.join(d, f"oracle.{dialect}")
        shutil.copyfile(db_path, oracle_path)
        yield await _engine_for(dialect=dialect, db_path=db_path), oracle_path


def raw_rows(*, dialect: str, db_path: str, sql: str) -> list[tuple]:
    """Execute raw oracle SQL against the seeded database file."""
    if dialect == "sqlite":
        con = sqlite3.connect(db_path)
    else:
        con = pytest.importorskip("duckdb").connect(db_path)
    try:
        return list(con.execute(sql).fetchall())
    finally:
        con.close()


def build_query(
    shape: LawShape, *, drop: Optional[str] = None, dims_only: bool = False,
) -> SlayerQuery:
    kwargs: dict[str, Any] = {"dimensions": DIM_FAMILY_DIMS[shape.dim_family]}
    if not dims_only:
        kwargs["measures"] = [
            ModelMeasure(formula=MEASURE_POOL[k], name=f"m_{k}")
            for k in shape.measure_keys if k != drop
        ]
    if shape.with_month:
        kwargs["time_dimensions"] = month_td()
    if shape.filter:
        kwargs["filters"] = [shape.filter]
    return q(**kwargs)


#: Shape variants that hit an enumerated fail-closed guard, keyed by
#: (shape_id, dropped-measure-key | DIMS_ONLY | None) → a DEFERRAL_SITES
#: fragment. Never a bare NotImplementedError catch; no silent skips — an
#: unregistered raise fails the test, a registered one must match its site.
EXPECTED_RAISES: dict[Tuple[str, Optional[str]], str] = {}

assert set(EXPECTED_RAISES.values()) <= _FRAGMENTS


async def execute_shape(
    engine: SlayerQueryEngine,
    shape: LawShape,
    *,
    drop: Optional[str] = None,
    dims_only: bool = False,
):
    """Run one shape variant; returns the response, or None after asserting a
    registered expected raise."""
    variant = DIMS_ONLY if dims_only else drop
    query = build_query(shape, drop=drop, dims_only=dims_only)
    expected = EXPECTED_RAISES.get((shape.shape_id, variant))
    if expected is None:
        return await engine.execute(query)
    with pytest.raises(NotImplementedError, match=re.escape(expected)):
        await engine.execute(query)
    return None


def keyed_rows(resp, *, shape: LawShape) -> dict[tuple, dict]:
    """Rows keyed by the shape's group key; duplicate keys are themselves a
    grain violation, asserted before any value comparison."""
    out = {
        dim_key(r, family=shape.dim_family, with_month=shape.with_month): r
        for r in resp.data
    }
    law_assert(
        len(out) == len(resp.data),
        law="grain guarantee",
        detail=f"duplicate rows for one group key ({len(resp.data)} rows, "
               f"{len(out)} keys)",
        shape=shape,
    )
    return out


def canon(value):
    """NULL-safe canonical value for set-style comparisons."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return round(float(value), 6)
    return value


def values_equal(a, b) -> bool:
    """NULL-safe per-cell equality, approx for floats."""
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return bool(float(a) == pytest.approx(float(b)))
    return bool(a == b)


def law_assert(condition: bool, *, law: str, detail: str, shape) -> None:
    """The assertion convention: failures name the law, not just the diff."""
    assert condition, f"LAW {law} violated — {detail}; shape={shape}"


__all__ = [
    "DEFERRAL_SITES", "DIMS_ONLY", "DUCKDB_PAIR_IDS", "DUCKDB_SHAPE_IDS",
    "EXPECTED_RAISES", "FILTERS", "LAW_SEED", "MEASURE_POOL", "N_DUCKDB",
    "N_PAIRS", "N_SHAPES", "OPERANDS", "OPERAND_GRAIN", "OPS",
    "PAIRS_BY_GRAIN", "SHAPES", "CoherencePair", "DeferralSite", "LawShape",
    "ModelMeasure",
    "build_query", "canon", "execute_shape", "keyed_rows", "law_assert",
    "law_params", "make_law_engine", "month_key", "month_td", "pair_params",
    "q", "raw_rows", "sample_pairs", "sample_shapes", "values_equal",
]
