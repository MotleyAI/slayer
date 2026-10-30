"""Usage-reporting query features from the parse tree, over every Mode B position, with per-name function
counts and four new flags; a newer version's spool keeps what this version can name
(spec: usage-reporting/collection, usage-reporting/delivery)."""

from __future__ import annotations

import json
import time
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, get_args

import pytest

from slayer.core.keys import SCALAR_FUNCTIONS
from slayer import usage_reporting
from slayer.usage_reporting import payload, settings, spool
from slayer.usage_reporting.features import QueryFeatures, query_features
from tests._usage_reporting_helpers import (
    SENT,
    assert_no_sentinel,
    build_sentinel_storage,
    build_storage,
    cli,
    end_process,
    make_due,
    quiet_config,
    show,
    spool_files,
)


def _counts(found: QueryFeatures) -> dict[str, Counter[str]]:
    return {"aggregations": Counter(found.aggregations), "transforms": Counter(found.transforms),
            "functions": Counter(found.functions), "flags": Counter(found.flags)}


def _measures(*formulas: str) -> QueryFeatures:
    return query_features({"source_model": "m", "measures": list(formulas)})


def _filters(*filters: str) -> QueryFeatures:
    return query_features({"source_model": "m", "measures": ["count(*)"], "filters": list(filters)})


# --------------------------------------------------------------------------- #
# Formula → counts
# --------------------------------------------------------------------------- #
#: ``(measure formula, aggregations, transforms, functions, flags)``.
MEASURE_TABLE: list[tuple[str, list[str], list[str], list[str], list[str]]] = [
    ("sum(x)::int", ["sum"], [], [], ["cast"]),
    ("CAST(avg(y) AS DOUBLE)", ["avg"], [], [], ["cast"]),
    ("TRY_CAST(sum(y) AS INT)", ["sum"], [], [], ["try_cast"]),
    ("count(*) FILTER (WHERE z > 0)", ["count"], [], [], ["aggregate_filter"]),
    ("sum(x) FILTER (WHERE lower(y) = 'a')", ["sum"], [], ["lower"], ["aggregate_filter"]),
    ("count(DISTINCT x)", ["count_distinct"], [], [], []),
    ("array_length(tags)", ["custom"], [], [], []),
    ("dsum(amount)", ["custom"], [], [], []),
    ("amount:sum", ["sum"], [], [], []),
    ("time_shift(sum(amount), -1)", ["sum"], ["time_shift"], [], []),
    ("cumsum(sum(x)) / count(*)", ["sum", "count"], ["cumsum"], [], []),
    ("collect(city)", ["collect"], [], [], []),
    ("sum(round(price, 2))", ["sum"], [], ["round"], []),
    ("max(coalesce(a, 0)) + min(abs(b))", ["max", "min"], [], ["coalesce", "abs"], []),
    ("sum(iif(status = 'x(', 1, 0))", ["sum"], [], ["iif"], []),
    ("sum(CASE WHEN a > 1 THEN 1 ELSE 0 END)", ["sum"], [], ["iif"], []),
    ("max(strpos(name, 'b'))", ["max"], [], ["instr"], []),
    ("max(EXTRACT(YEAR FROM ordered_at))", ["max"], [], ["date_part"], []),
    ("max(lower(name) || upper(name))", ["max"], [], ["concat", "lower", "upper"], []),
]


@pytest.mark.parametrize(("formula", "aggregations", "transforms", "functions", "flags"), MEASURE_TABLE,
                         ids=[row[0] for row in MEASURE_TABLE])
def test_measure_formula_counts(formula: str, aggregations: list[str], transforms: list[str],
                                functions: list[str], flags: list[str]) -> None:
    assert _counts(_measures(formula)) == {
        "aggregations": Counter(aggregations), "transforms": Counter(transforms),
        "functions": Counter(functions), "flags": Counter(flags)}


#: ``(filter, functions, flags beyond "filters")`` — one entry per library family.
FILTER_TABLE: list[tuple[str, list[str], list[str]]] = [
    ("lower(name) = 'a'", ["lower"], []),
    ("length(trim(name)) > 3", ["length", "trim"], []),
    ("split_part(name, ',', 1) = 'a'", ["split_part"], []),
    ("replace(name, 'a', 'b') = 'c'", ["replace"], []),
    ("name LIKE 'a%'", ["like"], []),
    ("name ILIKE 'a%'", ["ilike"], []),
    ("regexp_like(name, 'a')", ["regexp_like"], []),
    ("name ~ 'a'", ["regexp_like"], []),
    ("regexp_extract(name, 'a(b)', 1) = 'b'", ["regexp_extract"], []),
    ("payload ->> 'k' = 'v'", ["json_value"], []),
    ("json_value(payload, '$.a') = 'v'", ["json_value"], []),
    ("contains(skills, 'go')", ["contains"], []),
    ("contains_any(skills, ['go', 'sql'])", ["contains_any"], ["list_literal"]),
    ("contains_all(skills, ['go'])", ["contains_all"], ["list_literal"]),
    ("round(sqrt(abs(rate)), 1) > 2", ["round", "sqrt", "abs"], []),
    ("pow(rate, 2) > ln(rate)", ["pow", "ln"], []),
    ("date_diff('day', a, b) > 3", ["date_diff"], []),
    ("ordered_at + INTERVAL '1 day' < now()", ["date_add", "now"], []),
    ("greatest(a, b) > least(a, b)", ["greatest", "least"], []),
    ("nullif(a, 0) IS NULL", ["nullif"], []),
    ("CAST(code AS INT) > 1", [], ["cast"]),
    ("TRY_CAST(code AS INT) > 1", [], ["try_cast"]),
    ("x IN (1, 2)", [], []),
]


@pytest.mark.parametrize(("text", "functions", "flags"), FILTER_TABLE, ids=[row[0] for row in FILTER_TABLE])
def test_filter_counts(text: str, functions: list[str], flags: list[str]) -> None:
    counts = _counts(_filters(text))
    assert counts["functions"] == Counter(functions)
    assert counts["flags"] == Counter(["filters", *flags])
    assert counts["aggregations"] == Counter(["count"])


def test_library_functions_by_name_scenario() -> None:
    found = query_features({"source_model": "m", "measures": ["count(*)"], "filters": [
        "lower(name) = 'a'", "regexp_like(name, 'a')", "payload ->> 'k' = 'v'", "contains_any(skills, ['go'])"]})
    assert Counter(found.functions) == Counter(["lower", "regexp_like", "json_value", "contains_any"])
    assert "list_literal" in found.flags


def test_grammar_forms_scenario() -> None:
    found = _measures("sum(x)::int", "CAST(avg(y) AS DOUBLE)", "count(*) FILTER (WHERE z > 0)", "count(DISTINCT x)")
    assert Counter(found.aggregations) == Counter(["sum", "avg", "count", "count_distinct"])
    assert Counter(found.flags) == Counter({"cast": 2, "aggregate_filter": 1})


def test_unparseable_expression_contributes_no_names() -> None:
    found = _filters("lower(")
    assert found.functions == []
    assert Counter(found.aggregations) == Counter(["count"])


# --------------------------------------------------------------------------- #
# Every position
# --------------------------------------------------------------------------- #
def test_every_position_is_read() -> None:
    found = query_features({
        "source_model": "orders",
        "dimensions": [{"name": "c", "expression": "upper(city)"}],
        "time_dimensions": [{"dimension": "coalesce(shipped_at, ordered_at)", "granularity": "month"}],
        "measures": ["sum(amount)"],
        "filters": ["length(name) > 3"],
        "order": [{"column": "abs(rate)", "direction": "asc"}],
    })
    assert Counter(found.functions) == Counter(["upper", "coalesce", "length", "abs"])


def test_plain_dimension_expression_is_read() -> None:
    found = query_features({"source_model": "orders", "dimensions": ["lower(city)"], "measures": ["count(*)"]})
    assert Counter(found.functions) == Counter(["lower"])


def test_every_stage_is_read() -> None:
    found = query_features([
        {"name": "s1", "source_model": "orders", "dimensions": [{"name": "c", "expression": "upper(city)"}],
         "measures": [{"formula": "sum(amount)", "name": "rev"}]},
        {"source_model": "s1", "measures": ["max(abs(rev))"]},
    ])
    assert Counter(found.functions) == Counter(["upper", "abs"])
    assert Counter(found.aggregations) == Counter(["sum", "max"])
    assert Counter(found.flags)["multistage"] == 1


@pytest.mark.parametrize("body", [
    {"dimensions": [{"name": "m", "expression": "month(ordered_at)"}], "measures": ["count(*)"]},
    {"measures": ["count(*)"], "filters": ["year(ordered_at) = 2024"]},
    {"measures": ["count(*)"], "filters": ["date_trunc('month', ordered_at) = '2024-01-01'"]},
])
def test_time_granularity_calls_are_not_counted(body: dict[str, Any]) -> None:
    found = query_features({"source_model": "orders", **body})
    assert Counter(found.aggregations) == Counter(["count"])
    assert found.functions == []


def test_list_children_are_not_counted() -> None:
    found = _filters("skills.value = 'go'", "orders.value > 1")
    assert found.functions == []
    assert Counter(found.aggregations) == Counter(["count"])


@pytest.mark.parametrize("source_model", [
    {"source_name": "orders", "measures": [{"name": "m2", "formula": "max(round(amount))"}]},
    {"name": "orders_inline", "data_source": "test", "sql_table": "orders",
     "columns": [{"name": "id", "type": "INT", "primary_key": True}, {"name": "amount", "type": "DOUBLE"}],
     "measures": [{"name": "m2", "formula": "max(round(amount))"}]},
])
def test_inline_model_measure_formulas_are_read(source_model: dict[str, Any]) -> None:
    found = query_features({"source_model": source_model, "measures": ["count(*)"]})
    assert Counter(found.functions) == Counter(["round"])
    assert Counter(found.aggregations) == Counter(["count", "max"])


# --------------------------------------------------------------------------- #
# Vocabulary and privacy
# --------------------------------------------------------------------------- #
def test_function_vocabulary_is_the_library() -> None:
    assert frozenset(get_args(payload.FunctionToken)) == SCALAR_FUNCTIONS


def test_new_flags_are_vocabulary() -> None:
    assert {"cast", "try_cast", "aggregate_filter", "list_literal"} <= payload.FLAGS


@pytest.mark.parametrize("text", [
    f"{SENT}_fn(x) > 1", "fünf(x) > 1", f'"{SENT}"(x) > 1', f"lower({SENT}_col) = 'a'",
    f"{SENT}(lower(x)) > 1", "lower(", f"{SENT}.value = 'x'", f"x::{SENT}",
])
def test_only_vocabulary_function_names_are_counted(text: str) -> None:
    found = _filters(text)
    assert set(found.functions) <= SCALAR_FUNCTIONS
    assert SENT not in json.dumps(found.model_dump(mode="json"), default=str)


def test_merge_sums_function_counters() -> None:
    a = payload.Batch.model_validate({"features": {"functions": {"lower": 1}, "flags": {"cast": 2}}})
    b = payload.Batch.model_validate({"features": {"functions": {"lower": 2, "abs": 1}, "flags": {"cast": 1}}})
    merged = payload.merge([a, b])
    assert merged.features.functions == {"lower": 3, "abs": 1}
    assert merged.features.flags == {"cast": 3}


# --------------------------------------------------------------------------- #
# End to end through the CLI
# --------------------------------------------------------------------------- #
@pytest.fixture
def store(usage_reporting_env) -> str:
    quiet_config()
    return build_storage(usage_reporting_env.root)


def test_report_counts_functions_and_new_flags(store: str) -> None:
    cli(["query", json.dumps({"source_model": "orders", "measures": ["sum(amount)::int"],
                              "filters": ["lower(status) = 'paid'"]}), "--storage", store])
    features = show()["features"]
    assert features["functions"] == {"lower": 1}
    assert features["flags"]["cast"] == 1


def test_sent_report_carries_no_function_shaped_sentinel(usage_reporting_env) -> None:
    quiet_config()
    sentinel_store = build_sentinel_storage(usage_reporting_env.root)
    query = {"source_model": f"{SENT}_model", "measures": [f"{SENT}_agg({SENT}_col)"],
             "filters": [f"{SENT}_fn({SENT}_col) > 0", f"lower({SENT}_col) = '{SENT}'", f"x::{SENT}"]}
    cli(["query", json.dumps(query), "--storage", sentinel_store])
    make_due()
    cli(["models", "--storage", sentinel_store, "list"])
    (request,) = usage_reporting_env.capture.wait_for(count=1)
    assert_no_sentinel(request.body)
    assert set(request.json()["properties"]["features"].get("functions", {})) <= SCALAR_FUNCTIONS


# --------------------------------------------------------------------------- #
# Spool files from other versions
# --------------------------------------------------------------------------- #
def _spool_raw(directory: Path, batch: dict[str, Any]) -> None:
    path = directory / f"spool-{time.time_ns():020d}-{uuid.uuid4().hex[:12]}.json"
    path.write_text(json.dumps({"period": {"first": "2026-10-01", "last": "2026-10-02"}, **batch}))


def _load_one(directory: Path) -> payload.Batch:
    batches = spool.load(spool.pending_files(directory))
    assert len(batches) == 1
    return batches[0]


def test_newer_spool_keeps_what_this_version_names(tmp_path: Path) -> None:
    _spool_raw(tmp_path, {
        "usage": {"cli:query": {"ok": 2}},
        "features": {"aggregations": {"sum": 3, "future_agg": 1}, "flags": {"future_flag": 4, "cast": 1},
                     "functions": {"lower": 2, "future_fn": 5}, "future_field": {"x": 1}},
        "future_section": {"y": 1},
    })
    batch = _load_one(tmp_path)
    assert batch.features.aggregations == {"sum": 3}
    assert batch.features.flags == {"cast": 1}
    assert batch.features.functions == {"lower": 2}
    assert batch.usage["cli:query"].ok == 2
    dumped = batch.model_dump_json()
    assert "future" not in dumped


def test_spool_never_keeps_a_hand_crafted_sentinel(tmp_path: Path) -> None:
    _spool_raw(tmp_path, {"features": {"functions": {SENT: 9, "abs": 1}, "flags": {SENT: 1}}})
    batch = _load_one(tmp_path)
    assert batch.features.functions == {"abs": 1}
    assert_no_sentinel(batch.model_dump_json())


def test_older_spool_without_function_counters_is_kept_whole(tmp_path: Path) -> None:
    _spool_raw(tmp_path, {"usage": {"cli:query": {"ok": 1}},
                          "features": {"aggregations": {"sum": 3}, "transforms": {"cumsum": 1},
                                       "flags": {"filters": 2}}})
    batch = _load_one(tmp_path)
    assert batch.features.aggregations == {"sum": 3}
    assert batch.features.transforms == {"cumsum": 1}
    assert batch.features.flags == {"filters": 2}
    assert batch.features.functions == {}
    assert batch.usage["cli:query"].ok == 1


def test_newer_spool_reaches_the_report_without_its_unknown_parts(usage_reporting_env) -> None:
    make_due()
    directory = settings.spool_dir()
    directory.mkdir(parents=True, exist_ok=True)
    _spool_raw(directory, {"features": {"aggregations": {"sum": 3}, "flags": {"future_flag": 4},
                                        "future_field": {"x": 1}}})
    usage_reporting.start()
    (request,) = usage_reporting_env.capture.wait_for(count=1)
    end_process()
    report = request.json()["properties"]
    assert report["features"]["aggregations"]["sum"] == 3
    assert "future" not in json.dumps(report)
    assert all("future" not in path.read_text() for path in spool_files())


def test_send_model_stays_strict() -> None:
    with pytest.raises(ValueError, match="future_flag"):
        payload.Batch.model_validate({"features": {"flags": {"future_flag": 1}}})
