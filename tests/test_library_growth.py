"""Library growth never invalidates a stored artefact (spec: models/stored-upgrade; storage principle 3.2)."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Iterable, Mapping
from itertools import combinations
from pathlib import Path

import pytest

from slayer.core.enums import (
    ALL_TRANSFORMS,
    BUILTIN_AGGREGATIONS,
    GRANULARITY_NAMES,
    RESERVED_DATE_SYNTAX_NAMES,
    RESERVED_NAMES,
)
from slayer.core.keys import SCALAR_FUNCTIONS, SCALAR_SPELLINGS
from slayer.core.models import reserved_granularity_names
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage import granularity_rename, migrations
from tests._granularity_rename_fixtures import DS as GRANULARITY_DS
from tests._granularity_rename_fixtures import EXPECTED_BUCKETS, bucket_totals, granularity_store, renamed
from tests._library_names_fixtures import SUM_A, agg, metric, stored_engine, t_doc, value
from tests._stored_upgrade_fixtures import DS

#: Every name the library reserved when this guard was written. Never extended: a later reservation is shadowable or
#: goes in ``MIGRATED``.
PINNED: dict[str, frozenset[str]] = {
    "functions": frozenset({
        "abs", "acos", "ascii", "asin", "atan", "atan2", "cbrt", "ceil", "ceiling", "chr", "coalesce", "concat",
        "concat_ws", "contains", "contains_all", "contains_any", "cos", "current_date", "date_add", "date_diff",
        "date_part", "div", "ends_with", "exp", "floor", "greatest", "ifnull", "iif", "ilike", "instr", "interval",
        "json_value", "least", "left", "length", "like", "ln", "log", "log10", "log2", "lower", "lpad", "ltrim", "mod",
        "now", "nullif", "pi", "pow", "power", "regexp_extract", "regexp_like", "regexp_replace", "repeat", "replace",
        "reverse", "right", "round", "rpad", "rtrim", "sign", "sin", "split_part", "sqrt", "starts_with", "substr",
        "substring", "tan", "trim", "trunc", "upper",
    }),
    "spellings": frozenset({
        "date_trunc", "json_extract", "json_extract_path_text", "json_extract_scalar", "jsonb_extract_path_text",
        "strpos",
    }),
    "date_syntax": frozenset({
        "day", "day_of_month", "day_of_week", "day_of_year", "dayofmonth", "dayofyear", "doy", "extract", "hour",
        "iso_week", "iso_year", "isodow", "isoweek", "isoyear", "minute", "month", "quarter", "second", "week",
        "weekofyear", "year",
    }),
    "transforms": frozenset({
        "change", "change_pct", "consecutive_periods", "cumsum", "dense_rank", "first", "lag", "last", "lead",
        "ntile", "percent_rank", "rank", "time_shift",
    }),
    "granularities": frozenset({"day", "hour", "minute", "month", "quarter", "second", "week", "week_sunday", "year"}),
    "aggregations": frozenset({
        "avg", "collect", "corr", "count", "count_distinct", "count_distinct_approx", "covar_pop", "covar_samp",
        "first", "last", "max", "median", "min", "percentile", "stddev_pop", "stddev_samp", "sum", "var_pop",
        "var_samp", "weighted_avg",
    }),
    "reserved": frozenset(),
}

#: A dataset's own definition shadows these, so a new name in them needs no migration.
SHADOWABLE = frozenset({"functions", "spellings"})
#: Ossie Layer 1 functions and spellings reserved after ``PINNED``.
OSSIE_NAMES = frozenset({
    "if", "nvl", "nvl2", "startswith", "endswith", "charindex", "truncate", "to_date", "to_timestamp",
    "zeroifnull", "nullifzero", "radians", "degrees", "regexp_count",
})
#: Date-part words with no function form; a stored aggregation of the name keeps working.
PART_ONLY_NAMES = frozenset({"millisecond", "dayofweek"})

#: A newly reserved name outside ``SHADOWABLE`` → the ``(entity, source_version)`` migration that renames it in stores.
MIGRATED: dict[str, tuple[str, int]] = {
    "agg": ("SlayerModel", 18),
    **dict.fromkeys(("sum_distinct", "avg_distinct", "percentile_disc", "percentile_approx"), ("SlayerModel", 31)),
}


def live_vocabularies() -> dict[str, frozenset[str]]:
    return {
        "functions": SCALAR_FUNCTIONS, "spellings": SCALAR_SPELLINGS, "date_syntax": RESERVED_DATE_SYNTAX_NAMES,
        "transforms": frozenset(ALL_TRANSFORMS), "granularities": frozenset(GRANULARITY_NAMES),
        "aggregations": BUILTIN_AGGREGATIONS, "reserved": RESERVED_NAMES,
    }


def unpinned(
    *, live: Mapping[str, frozenset[str]], pinned: Mapping[str, frozenset[str]],
    migrated: Mapping[str, tuple[str, int]],
) -> list[str]:
    """``vocabulary:name`` for each newly reserved name that neither yields to a stored definition nor is migrated."""
    out: list[str] = []
    for vocabulary, names in live.items():
        if vocabulary in SHADOWABLE:
            continue
        for name in sorted(names - pinned.get(vocabulary, frozenset())):
            if migrated.get(name) not in migrations._REGISTRY:
                out.append(f"{vocabulary}:{name}")
    return out


class TestGuard:
    def test_every_reservation_is_pinned_or_migrated(self) -> None:
        assert unpinned(live=live_vocabularies(), pinned=PINNED, migrated=MIGRATED) == []

    def test_every_vocabulary_is_pinned(self) -> None:
        assert set(live_vocabularies()) == set(PINNED)

    def test_ossie_names_are_shadowable_reservations(self) -> None:
        assert sorted(OSSIE_NAMES - (SCALAR_FUNCTIONS | SCALAR_SPELLINGS)) == []

    def test_part_only_names_are_not_functions(self) -> None:
        assert PART_ONLY_NAMES & (SCALAR_FUNCTIONS | SCALAR_SPELLINGS) == set()

    def test_agg_is_reserved_and_migrated(self) -> None:
        assert "agg" in live_vocabularies()["reserved"]
        assert MIGRATED["agg"] in migrations._REGISTRY

    def test_an_unpinned_reservation_is_caught(self) -> None:
        live = {**live_vocabularies(), "transforms": ALL_TRANSFORMS | {"new_window"}}
        assert unpinned(live=live, pinned=PINNED, migrated=MIGRATED) == ["transforms:new_window"]

    def test_a_new_function_needs_no_migration(self) -> None:
        live = {**live_vocabularies(), "functions": SCALAR_FUNCTIONS | {"new_fn"}}
        assert unpinned(live=live, pinned=PINNED, migrated=MIGRATED) == []

    def test_a_migrated_reservation_passes(self) -> None:
        live = {**live_vocabularies(), "transforms": ALL_TRANSFORMS | {"new_window"}}
        key = next(iter(migrations._REGISTRY))
        assert unpinned(live=live, pinned=PINNED, migrated={**MIGRATED, "new_window": key}) == []

    def test_an_unregistered_migration_does_not_count(self) -> None:
        live = {**live_vocabularies(), "transforms": ALL_TRANSFORMS | {"new_window"}}
        assert unpinned(live=live, pinned=PINNED, migrated={**MIGRATED, "new_window": ("SlayerModel", 10_000)}) == [
            "transforms:new_window"]


#: The rename gate of every name reserved when gates were introduced.
INITIAL_GATE: dict[str, int] = {"SlayerModel": 30, "SlayerQuery": 19, "Memory": 14}
#: Reserved scalar spellings ``PINNED`` does not list.
_RESERVED_SPELLINGS = frozenset({
    "array_contains", "array_length", "cardinality", "current_timestamp", "dateadd", "datediff", "iff",
})
#: ``MIGRATED`` when gates were introduced; a later entry needs a stepped gate.
_INITIAL_MIGRATED = frozenset({"agg"})
INITIAL_NAMES = frozenset().union(*PINNED.values()) | _INITIAL_MIGRATED | _RESERVED_SPELLINGS
Gates = Mapping[str, Mapping[str, int]]


def initial_gate_findings(*, reserved: Iterable[str], gates: Gates) -> list[str]:
    """Each reserved initial name whose gate is no longer the initial one."""
    return sorted(n for n in set(reserved) & INITIAL_NAMES if dict(gates.get(n) or {}) != INITIAL_GATE)


def _stepped(low: Mapping[str, int], high: Mapping[str, int]) -> bool:
    return all(high[entity] > low[entity] for entity in INITIAL_GATE)


def gate_findings(*, reserved: Iterable[str], gates: Gates) -> list[str]:
    """Each reserved name without a gate, or whose gate does not step the versions above every other gate."""
    out: set[str] = set()
    later: list[str] = []
    for name in reserved:
        gate = gates.get(name)
        if gate is None or (dict(gate) != INITIAL_GATE if name in INITIAL_NAMES else not _stepped(INITIAL_GATE, gate)):
            out.add(name)
        elif name not in INITIAL_NAMES:
            later.append(name)
    for a, b in combinations(later, 2):
        if dict(gates[a]) != dict(gates[b]) and not (_stepped(gates[a], gates[b]) or _stepped(gates[b], gates[a])):
            out |= {a, b}
    return sorted(out)


def _with(name: str, gate: Mapping[str, int] | None = None) -> tuple[set[str], Gates]:
    """Today's reservations and gates plus ``name`` (gated by ``gate`` when given)."""
    gates = dict(granularity_rename.RENAME_GATES)
    if gate is not None:
        gates[name] = gate
    return set(reserved_granularity_names()) | {name}, gates


class TestRenameGates:
    def test_every_reserved_name_has_its_gate(self) -> None:
        assert gate_findings(reserved=reserved_granularity_names(), gates=granularity_rename.RENAME_GATES) == []

    def test_every_initial_name_keeps_the_initial_gate(self) -> None:
        assert _INITIAL_MIGRATED <= set(MIGRATED)
        assert initial_gate_findings(reserved=reserved_granularity_names(), gates=granularity_rename.RENAME_GATES) == []

    def test_a_stepped_later_reservation_keeps_the_initial_gates(self) -> None:
        reserved, gates = _with("new_window", {"SlayerModel": 33, "SlayerQuery": 21, "Memory": 16})
        assert initial_gate_findings(reserved=reserved, gates=gates) == []

    def test_a_changed_initial_gate_is_caught(self) -> None:
        reserved, gates = _with("left", {"SlayerModel": 33, "SlayerQuery": 21, "Memory": 16})
        assert initial_gate_findings(reserved=reserved, gates=gates) == ["left"]

    @pytest.mark.parametrize("name", ["new_window", "new_fn"], ids=["transform", "function"])
    def test_a_reservation_without_a_gate_is_caught(self, name: str) -> None:
        reserved, gates = _with(name)
        assert gate_findings(reserved=reserved, gates=gates) == [name]

    @pytest.mark.parametrize("gate", [
        INITIAL_GATE, {"SlayerModel": 31, "SlayerQuery": 19, "Memory": 15},
        {"SlayerModel": 31, "SlayerQuery": 20, "Memory": 14}, {"SlayerModel": 29, "SlayerQuery": 20, "Memory": 15},
    ], ids=["initial", "query-not-stepped", "memory-not-stepped", "dataset-lower"])
    def test_a_gate_that_does_not_step_the_versions_is_caught(self, gate: dict[str, int]) -> None:
        reserved, gates = _with("new_window", gate)
        assert gate_findings(reserved=reserved, gates=gates) == ["new_window"]

    def test_a_stepped_gate_passes(self) -> None:
        reserved, gates = _with("new_window", {"SlayerModel": 33, "SlayerQuery": 21, "Memory": 16})
        assert gate_findings(reserved=reserved, gates=gates) == []

    @pytest.mark.parametrize(("second", "caught"), [
        ({"SlayerModel": 34, "SlayerQuery": 21, "Memory": 17}, ["new_a", "new_b"]),
        ({"SlayerModel": 34, "SlayerQuery": 22, "Memory": 17}, []),
        ({"SlayerModel": 33, "SlayerQuery": 21, "Memory": 16}, []),
    ], ids=["not-stepped", "stepped", "same-release"])
    def test_later_gates_step_each_other(self, second: dict[str, int], caught: list[str]) -> None:
        reserved, gates = _with("new_a", {"SlayerModel": 33, "SlayerQuery": 21, "Memory": 16})
        gates = {**gates, "new_b": second}
        assert gate_findings(reserved=reserved | {"new_b"}, gates=gates) == caught


_FUNCTION_NAMES = sorted(SCALAR_FUNCTIONS | SCALAR_SPELLINGS | OSSIE_NAMES | PART_ONLY_NAMES)


@pytest.fixture(scope="module")
async def aggregation_engine() -> AsyncGenerator[SlayerQueryEngine]:
    docs = [t_doc(name=f"t_{name}", aggregations=[agg(name)]) for name in _FUNCTION_NAMES]
    async with stored_engine(datasets=docs) as engine:
        yield engine


class TestStoredAggregations:
    @pytest.mark.parametrize("name", _FUNCTION_NAMES)
    async def test_named_after_a_function(self, aggregation_engine: SlayerQueryEngine, name: str) -> None:
        assert await aggregation_engine.storage.get_dataset(f"t_{name}", data_source=DS) is not None
        resp = await aggregation_engine.execute({"population": f"t_{name}", "metrics": [metric(f"{name}(t_{name}.a)")]})
        assert float(value(resp)) == SUM_A


#: Built-in granularities were never storable as custom ones.
_GRANULARITY_CASES = sorted(frozenset().union(*live_vocabularies().values()) - GRANULARITY_NAMES)


class TestStoredGranularities:
    @pytest.mark.parametrize("name", _GRANULARITY_CASES)
    async def test_named_after_a_reserved_name(self, tmp_path: Path, name: str) -> None:
        storage = await granularity_store(backend="yaml", base=str(tmp_path), name=name)
        ds = await storage.get_datasource(GRANULARITY_DS)
        assert ds is not None
        assert [g.name for g in ds.granularities] == [renamed(name)]
        assert await bucket_totals(storage) == EXPECTED_BUCKETS
