"""Library growth never invalidates a stored artefact (spec: models/stored-upgrade; system principle 11.2)."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping
from pathlib import Path

import pytest

from slayer.core.enums import ALL_TRANSFORMS, BUILTIN_AGGREGATIONS, GRANULARITY_NAMES, RESERVED_DATE_SYNTAX_NAMES
from slayer.core.keys import SCALAR_FUNCTIONS, SCALAR_SPELLINGS
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage import migrations
from tests._granularity_rename_fixtures import DS as GRANULARITY_DS
from tests._granularity_rename_fixtures import EXPECTED_BUCKETS, bucket_totals, granularity_store, renamed
from tests._library_names_fixtures import SUM_A, agg, measure, stored_engine, t_doc, value
from tests._stored_upgrade_fixtures import DS

#: Every name the library reserved when this guard was written. Extend it only together with the rule below.
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
        "day_of_month", "day_of_week", "day_of_year", "dayofmonth", "dayofyear", "doy", "extract", "iso_week",
        "iso_year", "isodow", "isoweek", "isoyear", "weekofyear",
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
}

#: A model's own definition shadows these, so a new name in them needs no migration.
SHADOWABLE = frozenset({"functions", "spellings"})

#: A newly reserved name outside ``SHADOWABLE`` → the ``(entity, source_version)`` migration that renames it in stores.
MIGRATED: dict[str, tuple[str, int]] = {}


def live_vocabularies() -> dict[str, frozenset[str]]:
    return {
        "functions": SCALAR_FUNCTIONS, "spellings": SCALAR_SPELLINGS, "date_syntax": RESERVED_DATE_SYNTAX_NAMES,
        "transforms": frozenset(ALL_TRANSFORMS), "granularities": frozenset(GRANULARITY_NAMES),
        "aggregations": BUILTIN_AGGREGATIONS,
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

    def test_an_unpinned_reservation_is_caught(self) -> None:
        live = {**live_vocabularies(), "transforms": ALL_TRANSFORMS | {"new_window"}}
        assert unpinned(live=live, pinned=PINNED, migrated=MIGRATED) == ["transforms:new_window"]

    def test_a_new_function_needs_no_migration(self) -> None:
        live = {**live_vocabularies(), "functions": SCALAR_FUNCTIONS | {"new_fn"}}
        assert unpinned(live=live, pinned=PINNED, migrated=MIGRATED) == []

    def test_a_migrated_reservation_passes(self) -> None:
        live = {**live_vocabularies(), "transforms": ALL_TRANSFORMS | {"new_window"}}
        key = next(iter(migrations._REGISTRY))
        assert unpinned(live=live, pinned=PINNED, migrated={"new_window": key}) == []

    def test_an_unregistered_migration_does_not_count(self) -> None:
        live = {**live_vocabularies(), "transforms": ALL_TRANSFORMS | {"new_window"}}
        assert unpinned(live=live, pinned=PINNED, migrated={"new_window": ("SlayerModel", 10_000)}) == [
            "transforms:new_window"]


_FUNCTION_NAMES = sorted(SCALAR_FUNCTIONS | SCALAR_SPELLINGS)


@pytest.fixture(scope="module")
async def aggregation_engine() -> AsyncGenerator[SlayerQueryEngine]:
    docs = [t_doc(name=f"t_{name}", aggregations=[agg(name)]) for name in _FUNCTION_NAMES]
    async with stored_engine(models=docs) as engine:
        yield engine


class TestStoredAggregations:
    @pytest.mark.parametrize("name", _FUNCTION_NAMES)
    async def test_named_after_a_function(self, aggregation_engine: SlayerQueryEngine, name: str) -> None:
        assert await aggregation_engine.storage.get_model(f"t_{name}", data_source=DS) is not None
        resp = await aggregation_engine.execute({"source_model": f"t_{name}", "measures": [measure(f"{name}(a)")]})
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
