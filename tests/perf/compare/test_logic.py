"""Unit tests for the engine-comparison tooling (classify, oracle, corpus sanity).

Run manually (tests/perf is CI-ignored):
    poetry run pytest tests/perf/compare/test_logic.py -o addopts=""
"""

import datetime as dt
import math
from decimal import Decimal

import pandas as pd
import pytest

import classify
import corpus
import oracle
from classify import (
    PerfFlag,
    Verdict,
    canonical_rows,
    cells_equal,
    classify_entry,
    decode_cell,
    encode_cell,
    flag_perf,
    pool_abba,
    row_sort_key,
    warning_drift,
)

# ---------------------------------------------------------------------------
# Cell encode/decode (typed-tagged JSON representation)
# ---------------------------------------------------------------------------

ROUNDTRIP_VALUES = [
    None,
    True,
    False,
    0,
    -17,
    3.5,
    "text",
    "2024-01-01",  # a string that looks like a date stays a string
    Decimal("123.456"),
    Decimal("-0.0000000001"),
    Decimal("1E+2"),
    dt.datetime(2024, 6, 1, 12, 30, 45),
    dt.datetime(2024, 6, 1, 12, 30, 45, 123456),
    dt.datetime(2024, 6, 1, 12, 0, 0, tzinfo=dt.timezone.utc),
    dt.date(2024, 6, 1),
]


@pytest.mark.parametrize("value", ROUNDTRIP_VALUES, ids=repr)
def test_cell_roundtrip(value):
    encoded = encode_cell(value)
    decoded = decode_cell(encoded)
    assert decoded == value
    assert type(decoded) is type(value)


def test_encode_cell_floats_stay_numbers():
    assert encode_cell(3.5) == 3.5
    assert encode_cell(7) == 7


def test_encode_cell_tags_are_strict_json_safe():
    import json

    for value in ROUNDTRIP_VALUES:
        json.dumps(encode_cell(value), allow_nan=False)


def test_encode_cell_nan_and_inf_tagged():
    import json
    import math

    for value in [float("nan"), float("inf"), float("-inf")]:
        encoded = encode_cell(value)
        json.dumps(encoded, allow_nan=False)  # never bare NaN/Infinity in JSON
        decoded = decode_cell(encoded)
        assert math.isnan(decoded) if value != value else decoded == value


def test_cells_equal_nan_matches_nan():
    assert cells_equal(float("nan"), float("nan"))[0] is True
    assert cells_equal(float("nan"), 0.0)[0] is False


def test_decode_cell_passthrough_untagged():
    assert decode_cell("plain") == "plain"
    assert decode_cell(1.25) == 1.25
    assert decode_cell(None) is None


def test_decode_cell_rejects_malformed_dicts():
    with pytest.raises(ValueError):
        decode_cell({"unexpected": "dict"})
    with pytest.raises(ValueError):
        decode_cell({"__dec__": "not-a-decimal"})


# ---------------------------------------------------------------------------
# cells_equal: tolerance + cross-type coercion with drift note
# ---------------------------------------------------------------------------

def test_cells_equal_exact_and_none():
    assert cells_equal(None, None) == (True, None)
    assert cells_equal("a", "a") == (True, None)
    assert cells_equal(5, 5) == (True, None)
    assert cells_equal(None, 0)[0] is False
    assert cells_equal("a", "b")[0] is False


def test_cells_equal_float_tolerance():
    equal, drift = cells_equal(1.0, 1.0 + 1e-12)
    assert equal
    assert drift is None
    assert cells_equal(1.0, 1.0 + 1e-6)[0] is False
    # absolute floor near zero
    assert cells_equal(0.0, 1e-13)[0] is True
    assert cells_equal(0.0, 1e-9)[0] is False


def test_cells_equal_decimal_vs_float():
    assert cells_equal(Decimal("2.5"), 2.5)[0] is True


def test_cells_equal_bool_as_number_notes_drift():
    equal, drift = cells_equal(True, 1)
    assert equal is True
    assert drift is not None
    equal, drift = cells_equal(False, 0)
    assert equal is True
    assert drift is not None
    assert cells_equal(True, 0)[0] is False
    assert cells_equal(True, True) == (True, None)


def test_cells_equal_datetime_string_coercion_notes_drift():
    equal, drift = cells_equal("2024-06-01T00:00:00", dt.datetime(2024, 6, 1))
    assert equal is True
    assert drift is not None
    equal, drift = cells_equal(dt.date(2024, 6, 1), "2024-06-01")
    assert equal is True
    assert drift is not None


def test_cells_equal_numeric_string_coercion_notes_drift():
    equal, drift = cells_equal("42", 42)
    assert equal is True
    assert drift is not None
    assert cells_equal("42x", 42)[0] is False


# ---------------------------------------------------------------------------
# Sorting / canonicalization
# ---------------------------------------------------------------------------

def test_row_sort_key_heterogeneous_types():
    rows = [
        ["b", 2],
        [None, 1],
        ["a", 3],
        [1.5, 0],
        [dt.datetime(2024, 1, 1), 9],
    ]
    ordered = sorted(rows, key=row_sort_key)
    # deterministic: None first, then numbers, then strings, then datetimes
    assert ordered[0][0] is None
    assert ordered[1][0] == 1.5
    assert [r[0] for r in ordered[2:4]] == ["a", "b"]
    assert isinstance(ordered[4][0], dt.datetime)


def test_canonical_rows_unordered_sorts_and_keeps_duplicates():
    rows = [["b", 1], ["a", 2], ["a", 2]]
    result = canonical_rows(rows, ordered=False)
    assert result == [["a", 2], ["a", 2], ["b", 1]]


def test_canonical_rows_ordered_preserves_order():
    rows = [["b", 1], ["a", 2]]
    assert canonical_rows(rows, ordered=True) == rows


def test_sort_key_coercion_aligns_representations():
    # numeric strings must sort numerically, not lexicographically, so a side
    # returning strings aligns positionally with a side returning numbers
    string_side = canonical_rows([["10"], ["9"], ["100"]], ordered=False)
    number_side = canonical_rows([[10], [9], [100]], ordered=False)
    assert [r[0] for r in string_side] == ["9", "10", "100"]
    assert [r[0] for r in number_side] == [9, 10, 100]
    # ISO datetime strings sort in datetime rank alongside real datetimes
    iso_side = canonical_rows([["2024-02-01T00:00:00"], ["2024-01-15T00:00:00"]], ordered=False)
    dt_side = canonical_rows([[dt.datetime(2024, 2, 1)], [dt.datetime(2024, 1, 15)]], ordered=False)
    assert [r[0] for r in iso_side] == ["2024-01-15T00:00:00", "2024-02-01T00:00:00"]
    assert [r[0] for r in dt_side] == [dt.datetime(2024, 1, 15), dt.datetime(2024, 2, 1)]


def test_classify_multirow_cross_representation_match():
    # one side numeric strings, other side numbers: sorted-compare must align
    a = _ok(["c"], [["10"], ["9"], ["100"]])
    b = _ok(["c"], [[9], [100], [10]])
    verdict = classify_entry(ENTRY, a, b)
    assert verdict.status == "MATCH"
    assert verdict.type_drift is True


# ---------------------------------------------------------------------------
# classify_entry: full taxonomy
# ---------------------------------------------------------------------------

def _ok(columns, rows):
    return {"status": "ok", "error_type": None, "error_msg": None,
            "columns": columns, "rows": rows}


def _err(error_type="QueryError", msg="boom"):
    return {"status": "error", "error_type": error_type, "error_msg": msg,
            "columns": [], "rows": []}


ENTRY = {"id": "q1", "family": "aggs", "query": {}, "expect_error": False, "ordered": False}
ENTRY_ORDERED = {**ENTRY, "ordered": True}
ENTRY_ERR = {**ENTRY, "expect_error": True}


def test_classify_match_exact():
    a = _ok(["orders.x"], [[1], [2]])
    b = _ok(["orders.x"], [[2], [1]])  # unordered: sorted before compare
    verdict = classify_entry(ENTRY, a, b)
    assert verdict.status == "MATCH"


def test_classify_match_within_tolerance():
    a = _ok(["c"], [[1.0]])
    b = _ok(["c"], [[1.0 + 1e-12]])
    assert classify_entry(ENTRY, a, b).status == "MATCH"


def test_classify_name_drift():
    a = _ok(["orders.total_cost_sum"], [[10.0], [20.0]])
    b = _ok(["orders.cost_sum"], [[10.0], [20.0]])
    verdict = classify_entry(ENTRY, a, b)
    assert verdict.status == "NAME_DRIFT"
    assert "total_cost_sum" in verdict.detail


def test_classify_value_mismatch_cell():
    a = _ok(["c"], [[10.0]])
    b = _ok(["c"], [[11.0]])
    assert classify_entry(ENTRY, a, b).status == "VALUE_MISMATCH"


def test_classify_value_mismatch_row_count():
    a = _ok(["c"], [[1], [2]])
    b = _ok(["c"], [[1]])
    verdict = classify_entry(ENTRY, a, b)
    assert verdict.status == "VALUE_MISMATCH"
    assert "row count" in verdict.detail.lower()


def test_classify_order_mismatch_only_when_ordered():
    a = _ok(["c"], [[1], [2]])
    b = _ok(["c"], [[2], [1]])
    assert classify_entry(ENTRY_ORDERED, a, b).status == "ORDER_MISMATCH"
    assert classify_entry(ENTRY, a, b).status == "MATCH"


def test_classify_ordered_value_mismatch_beats_order():
    a = _ok(["c"], [[1], [2]])
    b = _ok(["c"], [[3], [1]])
    assert classify_entry(ENTRY_ORDERED, a, b).status == "VALUE_MISMATCH"


def test_classify_shape_mismatch():
    a = _ok(["c1", "c2"], [[1, 2]])
    b = _ok(["c1"], [[1]])
    assert classify_entry(ENTRY, a, b).status == "SHAPE_MISMATCH"


def test_classify_pypi_only_error():
    verdict = classify_entry(ENTRY, _err(), _ok(["c"], [[1]]))
    assert verdict.status == "PYPI_ONLY_ERROR"
    assert verdict.expected_error is False


def test_classify_branch_only_error():
    verdict = classify_entry(ENTRY, _ok(["c"], [[1]]), _err())
    assert verdict.status == "BRANCH_ONLY_ERROR"


def test_classify_one_side_error_on_expected_error_entry():
    verdict = classify_entry(ENTRY_ERR, _err(), _ok(["c"], [[1]]))
    assert verdict.status == "PYPI_ONLY_ERROR"
    assert verdict.expected_error is True


def test_classify_both_error_expected():
    verdict = classify_entry(ENTRY_ERR, _err("ValidationError"), _err("ValidationError"))
    assert verdict.status == "BOTH_ERROR"
    assert verdict.error_type_drift is False


def test_classify_both_error_type_drift():
    verdict = classify_entry(ENTRY_ERR, _err("ValidationError"), _err("QueryError"))
    assert verdict.status == "BOTH_ERROR"
    assert verdict.error_type_drift is True


def test_classify_both_error_unexpected():
    verdict = classify_entry(ENTRY, _err(), _err())
    assert verdict.status == "BOTH_ERROR_UNEXPECTED"


def test_classify_expect_error_match_checked_per_side():
    entry = {**ENTRY_ERR, "expect_error_match": "Validation"}
    ok_verdict = classify_entry(entry, _err("ValidationError"), _err("ValidationError"))
    assert ok_verdict.status == "BOTH_ERROR"
    assert ok_verdict.error_match_failed == []
    bad = classify_entry(entry, _err("ValidationError"), _err("TypeError", "oops"))
    assert bad.error_match_failed == ["branch"]


def test_classify_expect_error_match_searches_message_too():
    entry = {**ENTRY_ERR, "expect_error_match": "unknown column"}
    verdict = classify_entry(
        entry,
        _err("QueryError", "unknown column 'x'"),
        _err("SlayerError", "Unknown column 'x' on model orders"),
    )
    assert verdict.error_match_failed == []  # case-insensitive, type or message


def test_classify_match_with_type_drift_flag():
    a = _ok(["d"], [["2024-06-01T00:00:00"]])
    b = _ok(["d"], [[encode_cell(dt.datetime(2024, 6, 1))]])
    verdict = classify_entry(ENTRY, a, b)
    assert verdict.status == "MATCH"
    assert verdict.type_drift is True


def test_classify_decodes_tagged_cells():
    a = _ok(["c"], [[encode_cell(Decimal("2.5"))]])
    b = _ok(["c"], [[2.5]])
    assert classify_entry(ENTRY, a, b).status == "MATCH"


# ---------------------------------------------------------------------------
# Perf flagging
# ---------------------------------------------------------------------------

def test_flag_perf_requires_ratio_and_floor():
    # 2x slower and > 20ms absolute: flagged
    flag = flag_perf("q1", "exec", pypi_times=[0.100] * 7, branch_times=[0.200] * 7)
    assert isinstance(flag, PerfFlag)
    assert flag.flagged is True
    # 2x slower but sub-millisecond: not flagged
    flag = flag_perf("q1", "exec", pypi_times=[0.0001] * 7, branch_times=[0.0002] * 7)
    assert flag.flagged is False
    # large absolute delta but below ratio: not flagged
    flag = flag_perf("q1", "exec", pypi_times=[1.000] * 7, branch_times=[1.100] * 7)
    assert flag.flagged is False


def test_flag_perf_uses_median():
    # one slow outlier must not flag
    times = [0.010] * 6 + [10.0]
    flag = flag_perf("q1", "exec", pypi_times=[0.010] * 7, branch_times=times)
    assert flag.flagged is False
    assert flag.branch_median == pytest.approx(0.010)


def test_flag_perf_branch_faster_never_flagged():
    flag = flag_perf("q1", "gen", pypi_times=[0.200] * 7, branch_times=[0.050] * 7)
    assert flag.flagged is False
    assert flag.ratio < 1


def test_flag_perf_boundaries_are_strict():
    # exactly 1.3x: not flagged (strict >)
    flag = flag_perf("q1", "exec", pypi_times=[0.100] * 7, branch_times=[0.130] * 7)
    assert flag.flagged is False
    # exactly 20ms delta at high ratio: not flagged (strict >)
    flag = flag_perf("q1", "exec", pypi_times=[0.020] * 7, branch_times=[0.040] * 7)
    assert flag.flagged is False
    # just past both boundaries: flagged
    flag = flag_perf("q1", "exec", pypi_times=[0.020] * 7, branch_times=[0.0411] * 7)
    assert flag.flagged is True


def test_flag_perf_degenerate_inputs():
    with pytest.raises(ValueError):
        flag_perf("q1", "exec", pypi_times=[], branch_times=[0.1])
    with pytest.raises(ValueError):
        flag_perf("q1", "exec", pypi_times=[0.1], branch_times=[float("nan")])
    # zero baseline: flag iff absolute delta clears the floor
    flag = flag_perf("q1", "exec", pypi_times=[0.0] * 7, branch_times=[0.500] * 7)
    assert flag.flagged is True
    flag = flag_perf("q1", "exec", pypi_times=[0.0] * 7, branch_times=[0.001] * 7)
    assert flag.flagged is False


def test_resolve_scales_explicit_and_profile_all():
    import compare

    assert compare.resolve_scales(False, "10k,40k") == {"10k": 10_000, "40k": 40_000}
    assert compare.resolve_scales(False, "") == {}
    all_scales = compare.resolve_scales(True, "10k")  # scales_arg ignored under profile-all
    assert set(all_scales) == set(compare.ALL_SCALES)
    assert {"10k", "40k", "100k", "1m", "10m"} <= set(all_scales)


def test_pool_abba_concatenates_per_query():
    run1 = {"q1": {"exec": [0.1, 0.2], "gen": [0.01]}}
    run2 = {"q1": {"exec": [0.3], "gen": [0.02]}, "q2": {"exec": [0.5], "gen": [0.05]}}
    pooled = pool_abba(run1, run2)
    assert pooled["q1"]["exec"] == [0.1, 0.2, 0.3]
    assert pooled["q1"]["gen"] == [0.01, 0.02]
    assert pooled["q2"]["exec"] == [0.5]  # query present in only one run survives


def test_flag_perf_even_length_pooled_samples():
    flag = flag_perf("q1", "exec", pypi_times=[0.100] * 14, branch_times=[0.100] * 7 + [0.300] * 7)
    assert flag.branch_median == pytest.approx(0.200)


# ---------------------------------------------------------------------------
# Warning drift (informational)
# ---------------------------------------------------------------------------

def test_warning_drift_none_when_identical():
    warnings = [{"kind": "normalization", "detail": "x"}] * 2
    assert warning_drift(list(warnings), list(warnings)) is None


def test_warning_drift_reports_kind_and_multiplicity():
    drift = warning_drift(
        [{"kind": "normalization"}],
        [{"kind": "normalization"}, {"kind": "unreachable_filter_dropped"}],
    )
    assert drift is not None
    assert "unreachable_filter_dropped" in drift
    assert warning_drift([{"kind": "a"}], [{"kind": "a"}, {"kind": "a"}]) is not None


# ---------------------------------------------------------------------------
# Oracle: hand-computed micro-fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def frames():
    tables = {
        "orders": [
            # id, customer_id, category, created_at, cost
            {"id": 1, "customer_id": 1, "shop_id": 1, "category": "food",
             "created_at": "2024-01-10", "cost": 10.0},
            {"id": 2, "customer_id": 1, "shop_id": 1, "category": "food",
             "created_at": "2024-01-20", "cost": 30.0},
            {"id": 3, "customer_id": 2, "shop_id": 2, "category": "toys",
             "created_at": "2024-02-05", "cost": 100.0},
            {"id": 4, "customer_id": 2, "shop_id": 2, "category": "toys",
             "created_at": "2024-04-01", "cost": None},  # gap month March; null cost
            {"id": 5, "customer_id": 3, "shop_id": 1, "category": "food",
             "created_at": "2024-04-15", "cost": 60.0},
        ],
        "customers": [
            {"id": 1, "name": "Ann", "segment": "retail"},
            {"id": 2, "name": "Bob", "segment": "corp"},
            {"id": 3, "name": "Cid", "segment": "retail"},
        ],
    }
    return oracle.frames_from_tables(tables)


def test_oracle_group_agg(frames):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "groupby": ["category"],
        "aggs": [{"col": "cost", "op": "sum"}, {"op": "count_star"}],
    }}
    rows = oracle.expected(spec, frames)
    assert canonical_rows(rows, ordered=False) == [["food", 100.0, 3], ["toys", 100.0, 2]]


def test_oracle_ungrouped_agg_and_count_distinct(frames):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "aggs": [{"col": "customer_id", "op": "count_distinct"},
                 {"col": "cost", "op": "avg"}],
    }}
    rows = oracle.expected(spec, frames)
    # AVG ignores the NULL cost: (10+30+100+60)/4
    assert rows == [[3, 50.0]]


def test_oracle_filter_before_agg(frames):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "filter": "cost > 20",
        "aggs": [{"col": "cost", "op": "sum"}],
    }}
    assert oracle.expected(spec, frames) == [[190.0]]


def test_oracle_all_null_group_sums_to_none(frames):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "filter": "category == 'toys' and id == 4",
        "aggs": [{"col": "cost", "op": "sum"}],
    }}
    assert oracle.expected(spec, frames) == [[None]]


def test_oracle_join_group(frames):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "joins": [{"table": "customers", "left_on": "customer_id", "right_on": "id"}],
        "groupby": ["customers.segment"],
        "aggs": [{"col": "cost", "op": "sum"}],
    }}
    rows = oracle.expected(spec, frames)
    assert canonical_rows(rows, ordered=False) == [["corp", 100.0], ["retail", 100.0]]


def test_oracle_month_bucket_no_fill(frames):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "time_bucket": {"col": "created_at", "gran": "month"},
        "groupby": ["created_at_month"],
        "aggs": [{"col": "cost", "op": "sum"}],
        "order_by": [["created_at_month", "asc"]],
    }}
    rows = oracle.expected(spec, frames)
    # March absent (gap month, no fill); April sums non-null only
    assert [r[0] for r in rows] == ["2024-01-01", "2024-02-01", "2024-04-01"]
    assert [r[1] for r in rows] == [40.0, 100.0, 60.0]


def test_oracle_cumsum_post_op(frames):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "time_bucket": {"col": "created_at", "gran": "month"},
        "groupby": ["created_at_month"],
        "aggs": [{"col": "cost", "op": "sum"}],
        "order_by": [["created_at_month", "asc"]],
        "post": [{"op": "cumsum", "on": "cost_sum"}],
    }}
    rows = oracle.expected(spec, frames)
    assert [r[-1] for r in rows] == [40.0, 140.0, 200.0]


def test_oracle_change_post_op_is_period_aware(frames):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "time_bucket": {"col": "created_at", "gran": "month"},
        "groupby": ["created_at_month"],
        "aggs": [{"col": "cost", "op": "sum"}],
        "order_by": [["created_at_month", "asc"]],
        "post": [{"op": "change", "on": "cost_sum"}],
    }}
    rows = oracle.expected(spec, frames)
    # months are Jan, Feb, Apr: April's previous CALENDAR month (March) is
    # missing, so its change is None — matching engine semantics — not a
    # row-diff against February
    assert [r[-1] for r in rows] == [None, 60.0, None]


def test_oracle_having(frames):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "groupby": ["category"],
        "aggs": [{"op": "count_star"}],
        "having": "_count > 2",
    }}
    assert oracle.expected(spec, frames) == [["food", 3]]


def test_oracle_per_agg_where_keeps_all_groups(frames):
    # a filtered measure (Column.filter / CASE-WHEN) is scoped to its aggregate,
    # not the whole query: every group survives, non-matching ones aggregate to
    # NULL — unlike a query-level filter, which would drop empty groups
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "joins": [{"table": "customers", "left_on": "customer_id", "right_on": "id"}],
        "groupby": ["category"],
        "aggs": [{"col": "cost", "op": "sum", "where": "`customers.segment` == 'retail'"}],
    }}
    rows = canonical_rows(oracle.expected(spec, frames), ordered=False)
    # food keeps its retail orders (10+30+60); toys is all-corp → NULL, not dropped
    assert rows == [["food", 100.0], ["toys", None]]


def test_oracle_per_agg_where_ungrouped(frames):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "aggs": [{"col": "cost", "op": "sum", "where": "category == 'food'"},
                 {"op": "count_star"}],
    }}
    # masked sum (food only: 10+30+60) beside an unmasked count over all 5 rows
    assert oracle.expected(spec, frames) == [[100.0, 5]]


def test_oracle_per_agg_where_rejects_count_star(frames):
    spec = {"fn": "agg", "args": {
        "aggs": [{"op": "count_star", "where": "cost > 0"}],
    }}
    with pytest.raises(ValueError, match="count_star"):
        oracle.expected(spec, frames)


def test_oracle_order_limit_offset(frames):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "groupby": ["id"],
        "aggs": [{"col": "cost", "op": "sum"}],
        "order_by": [["id", "desc"]],
        "limit": 2,
        "offset": 1,
    }}
    rows = oracle.expected(spec, frames)
    assert [r[0] for r in rows] == [4, 3]


def test_oracle_output_is_json_native(frames):
    # no numpy scalars / NaT / NaN may leak into oracle output
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "groupby": ["category"],
        "aggs": [{"col": "cost", "op": "sum"}],
    }}
    for row in oracle.expected(spec, frames):
        for cell in row:
            assert cell is None or isinstance(cell, (str, int, float, bool))
            if isinstance(cell, float):
                assert not math.isnan(cell)


def test_oracle_frames_from_dataset_matches_seed():
    seed_mod = oracle.load_seed_module()  # importlib-by-path: tests/perf is a package

    dataset = seed_mod.generate_dataset(order_count=200, start_date="2024-01-01",
                                        end_date="2024-03-31", seed=7)
    frames = oracle.frames_from_dataset(dataset)
    assert set(frames) >= {"orders", "customers", "shops", "regions"}
    assert len(frames["orders"]) == 200
    assert isinstance(frames["orders"], pd.DataFrame)


def test_oracle_unknown_fn_raises():
    with pytest.raises(KeyError):
        oracle.expected({"fn": "nope", "args": {}}, {})


@pytest.fixture
def frames_edge():
    tables = {
        "orders": [
            {"id": 1, "customer_id": 1, "category": "food",
             "created_at": "2023-11-05", "cost": 10.0},
            {"id": 2, "customer_id": 99, "category": "food",  # unmatched FK
             "created_at": "2024-01-15", "cost": 20.0},
            {"id": 3, "customer_id": None, "category": "toys",  # null FK
             "created_at": "2024-02-10", "cost": None},
            {"id": 4, "customer_id": 1, "category": "toys",
             "created_at": "2024-02-20", "cost": 5.0},
        ],
        "customers": [
            {"id": 1, "name": "Ann", "segment": "retail"},
        ],
    }
    return oracle.frames_from_tables(tables)


def test_oracle_year_bucket(frames_edge):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "time_bucket": {"col": "created_at", "gran": "year"},
        "groupby": ["created_at_year"],
        "aggs": [{"col": "cost", "op": "sum"}],
        "order_by": [["created_at_year", "asc"]],
    }}
    assert oracle.expected(spec, frames_edge) == [["2023-01-01", 10.0], ["2024-01-01", 25.0]]


def test_oracle_min_max(frames_edge):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "aggs": [{"col": "cost", "op": "min"}, {"col": "cost", "op": "max"}],
    }}
    assert oracle.expected(spec, frames_edge) == [[5.0, 20.0]]


def test_oracle_count_ignores_nulls(frames_edge):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "aggs": [{"col": "cost", "op": "count"},
                 {"col": "customer_id", "op": "count_distinct"},
                 {"op": "count_star"}],
    }}
    # count(cost)=3 (one null), distinct customer_id={1,99}, count(*)=4
    assert oracle.expected(spec, frames_edge) == [[3, 2, 4]]


def test_oracle_left_join_unmatched_keys(frames_edge):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "joins": [{"table": "customers", "left_on": "customer_id", "right_on": "id"}],
        "groupby": ["customers.segment"],
        "aggs": [{"op": "count_star"}],
    }}
    rows = oracle.expected(spec, frames_edge)
    # unmatched + null FKs group under NULL segment (left join semantics)
    assert canonical_rows(rows, ordered=False) == [[None, 2], ["retail", 2]]


def test_oracle_multi_column_groupby(frames_edge):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "time_bucket": {"col": "created_at", "gran": "year"},
        "groupby": ["category", "created_at_year"],
        "aggs": [{"op": "count_star"}],
    }}
    rows = canonical_rows(oracle.expected(spec, frames_edge), ordered=False)
    assert rows == [["food", "2023-01-01", 1], ["food", "2024-01-01", 1],
                    ["toys", "2024-01-01", 2]]


def test_oracle_distinct_dims(frames_edge):
    spec = {"fn": "agg", "args": {"table": "orders", "groupby": ["category"], "aggs": []}}
    rows = canonical_rows(oracle.expected(spec, frames_edge), ordered=False)
    assert rows == [["food"], ["toys"]]
    spec_all = {"fn": "agg", "args": {"table": "orders", "groupby": ["category"],
                                      "aggs": [], "distinct": False}}
    assert len(oracle.expected(spec_all, frames_edge)) == 4


def test_oracle_in_filter(frames_edge):
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "filter": "category in ['toys']",
        "aggs": [{"op": "count_star"}],
    }}
    assert oracle.expected(spec, frames_edge) == [[2]]


# ---------------------------------------------------------------------------
# Corpus sanity
# ---------------------------------------------------------------------------

KNOWN_FAMILIES = {
    "bench", "aggs", "joins", "filters", "formulas", "time",
    "order_limit", "multi", "multi_stage", "behavior", "errors",
}


def test_corpus_size_and_unique_ids():
    ids = [e["id"] for e in corpus.ENTRIES]
    assert len(ids) == len(set(ids))
    assert len(ids) >= 90


def test_corpus_entry_shapes():
    for entry in corpus.ENTRIES:
        assert entry["family"] in KNOWN_FAMILIES, entry["id"]
        assert isinstance(entry["expect_error"], bool), entry["id"]
        assert isinstance(entry["ordered"], bool), entry["id"]
        assert isinstance(entry["query"], (dict, list)), entry["id"]
        if isinstance(entry["query"], list):
            assert all(isinstance(q, dict) for q in entry["query"]), entry["id"]


def test_corpus_family_coverage():
    families = {e["family"] for e in corpus.ENTRIES}
    assert families == KNOWN_FAMILIES
    multi_stage = [e for e in corpus.ENTRIES if e["family"] == "multi_stage"]
    assert len(multi_stage) >= 6
    assert all(isinstance(e["query"], list) for e in multi_stage)


def test_corpus_oracle_specs_valid():
    with_oracle = 0
    for entry in corpus.ENTRIES:
        spec = entry.get("oracle")
        if spec is None:
            continue
        with_oracle += 1
        assert spec["fn"] in oracle.ORACLE_FNS, entry["id"]
        assert isinstance(spec.get("args"), dict), entry["id"]
    assert with_oracle >= 30  # substantial arbitration coverage


def test_corpus_error_entries_have_no_oracle():
    for entry in corpus.ENTRIES:
        if entry["expect_error"]:
            assert entry.get("oracle") is None, entry["id"]


def test_corpus_subset_100k_marked_and_valid():
    subset = [e for e in corpus.ENTRIES if e.get("subset_100k")]
    assert 10 <= len(subset) <= 25
    assert all(not e["expect_error"] for e in subset)


def test_corpus_models_and_datasource():
    models = {m["name"]: m for m in corpus.MODELS}
    assert set(models) == {"orders", "customers", "shops", "regions"}
    join_targets = {j["target_model"] for j in models["orders"]["joins"]}
    assert {"customers", "shops"} <= join_targets
    shops_targets = {j["target_model"] for j in models["shops"]["joins"]}
    assert "regions" in shops_targets


def test_corpus_no_slayer_imports():
    import ast
    from pathlib import Path

    tree = ast.parse(Path(str(corpus.__file__)).read_text())
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        assert not any(n.split(".")[0] == "slayer" for n in names)


def test_corpus_adversarial_tables():
    tables = corpus.ADVERSARIAL_TABLES
    assert set(tables) == {"orders", "customers", "shops", "regions"}
    orders = tables["orders"]
    assert len(orders) >= 20
    assert sum(len(rows) for rows in tables.values()) >= 40
    # fact-side pathologies: null FK, unmatched FK, null measure
    assert any(r["customer_id"] is None for r in orders)
    customer_pks = {c["id"] for c in tables["customers"]}
    assert any(r["customer_id"] not in customer_pks
               for r in orders if r["customer_id"] is not None)
    assert any(r["cost"] is None for r in orders)
    # dimension-side pathologies: duplicate PK (join amplification), null join key
    cust_ids = [c["id"] for c in tables["customers"]]
    assert len(cust_ids) != len(set(cust_ids))
    assert any(s["region_id"] is None for s in tables["shops"])
    # ties: two orders in the same month with identical cost
    seen = set()
    tie_found = False
    for r in orders:
        if r["cost"] is None:
            continue
        key = (str(r["created_at"])[:7], r["cost"])
        tie_found = tie_found or key in seen
        seen.add(key)
    assert tie_found
    # gap: at least one empty calendar month strictly inside the date span
    months = sorted({str(r["created_at"])[:7] for r in orders})
    first_year, first_month = map(int, months[0].split("-"))
    last_year, last_month = map(int, months[-1].split("-"))
    span = (last_year - first_year) * 12 + (last_month - first_month) + 1
    assert span > len(months)


def test_corpus_is_strict_json_serializable():
    import json

    payload = {
        "entries": corpus.ENTRIES,
        "models": corpus.MODELS,
        "datasource_type_placeholder": True,
        "adversarial": corpus.ADVERSARIAL_TABLES,
    }
    json.dumps(payload, allow_nan=False)


def test_corpus_oracle_specs_execute_on_both_datasets():
    seed_mod = oracle.load_seed_module()
    dataset = seed_mod.generate_dataset(order_count=300, start_date="2023-01-01",
                                        end_date="2024-12-31", seed=42)
    gen_frames = oracle.frames_from_dataset(dataset)
    adv_frames = oracle.frames_from_tables(corpus.ADVERSARIAL_TABLES)
    for entry in corpus.ENTRIES:
        spec = entry.get("oracle")
        if spec is None:
            continue
        for frames_set in (gen_frames, adv_frames):
            rows = oracle.expected(spec, frames_set)
            assert isinstance(rows, list), entry["id"]
            for row in rows:
                for cell in row:
                    assert cell is None or isinstance(cell, (str, int, float, bool)), entry["id"]


def test_corpus_variables_entries():
    with_vars = [e for e in corpus.ENTRIES if e.get("variables")]
    assert len(with_vars) >= 3
    assert any(isinstance(v, list) for e in with_vars for v in e["variables"].values())
    assert any(v == [] for e in with_vars for v in e["variables"].values())


# ---------------------------------------------------------------------------
# Verdict model basics
# ---------------------------------------------------------------------------

def test_verdict_is_pydantic_model():
    from pydantic import BaseModel

    assert issubclass(Verdict, BaseModel)
    assert issubclass(PerfFlag, BaseModel)


def test_classify_module_is_slayer_free():
    import ast
    from pathlib import Path

    tree = ast.parse(Path(str(classify.__file__)).read_text())
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        assert not any(n.split(".")[0] in {"slayer", "pandas"} for n in names)


# ---------------------------------------------------------------------------
# Post-review hardening (Codex implementation review)
# ---------------------------------------------------------------------------

def test_is_problem_covers_error_match_failures():
    from classify import is_problem

    clean = Verdict(status="BOTH_ERROR")
    assert is_problem(clean) is False
    assert is_problem(Verdict(status="MATCH")) is False
    assert is_problem(Verdict(status="VALUE_MISMATCH")) is True
    assert is_problem(Verdict(status="BOTH_ERROR", error_match_failed=["branch"])) is True
    assert is_problem(Verdict(status="PYPI_ONLY_ERROR")) is True


def test_flag_perf_incomplete_samples_rejected():
    # ABBA pooling must produce 2*repeats samples per side; short samples
    # (one run missing) are a hard error, not a quiet verdict
    with pytest.raises(ValueError, match="expected 14"):
        flag_perf("q1", "exec", pypi_times=[0.1] * 7, branch_times=[0.1] * 14,
                  expected_samples=14)
    flag = flag_perf("q1", "exec", pypi_times=[0.1] * 14, branch_times=[0.1] * 14,
                     expected_samples=14)
    assert flag.flagged is False


def test_oracle_left_join_null_keys_never_match():
    # SQL semantics: NULL join keys match nothing, even a NULL on the right
    tables = {
        "orders": [
            {"id": 1, "customer_id": None, "cost": 10.0},
            {"id": 2, "customer_id": 5, "cost": 20.0},
        ],
        "customers": [
            {"id": None, "segment": "ghost"},
            {"id": 5, "segment": "real"},
        ],
    }
    frames = oracle.frames_from_tables(tables)
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "joins": [{"table": "customers", "left_on": "customer_id", "right_on": "id"}],
        "groupby": ["customers.segment"],
        "aggs": [{"op": "count_star"}],
    }}
    rows = canonical_rows(oracle.expected(spec, frames), ordered=False)
    # null-key order lands in the NULL segment group, NOT in 'ghost'
    assert rows == [[None, 1], ["real", 1]]


# ---------------------------------------------------------------------------
# Post-review hardening (round 2: CodeRabbit + Codex + Sonar)
# ---------------------------------------------------------------------------

def test_write_timings_csv_skips_error_rows(tmp_path):
    import compare

    flags = [
        {"backend": "sqlite", "scale": "10m", "entry": "q1", "metric": "exec",
         "pypi_median": 0.1, "branch_median": 0.2, "ratio": 2.0, "delta": 0.1, "flagged": False},
        {"backend": "sqlite", "scale": "10m", "entry": "q2", "metric": "exec",
         "flagged": False, "error": "flag_perf raised"},        # error row: no median fields
        {"backend": "sqlite", "scale": "10m", "entry": "q3", "metric": "n/a",
         "flagged": False, "error": "timed on only one side"},
    ]
    compare.write_timings_csv(tmp_path, flags)  # must not KeyError on the error rows
    lines = (tmp_path / "timings.csv").read_text().strip().splitlines()
    assert len(lines) == 2  # header + the one valid row; both error rows skipped
    assert lines[1].startswith("sqlite,10m,q1,exec")


def test_oracle_multi_key_order_by_keeps_first_key_primary(frames):
    # category asc, then cost_sum desc: a naive sequential sort would let the LAST
    # key (cost_sum) become primary and interleave categories
    spec = {"fn": "agg", "args": {
        "table": "orders",
        "groupby": ["category", "id"],
        "aggs": [{"col": "cost", "op": "sum"}],
        "order_by": [["category", "asc"], ["cost_sum", "desc"]],
    }}
    rows = oracle.expected(spec, frames)
    cats = [r[0] for r in rows]
    assert cats == sorted(cats)  # category (primary key) stays non-decreasing
    food = [r[-1] for r in rows if r[0] == "food"]
    assert food == sorted(food, reverse=True)  # cost_sum (secondary) desc within a category
