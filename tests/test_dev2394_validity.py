"""Spec: models/semantic-model-documents › The native validity check agrees with upstream's validator.

Cases and message texts follow upstream ``validation/tests/test_validate.py`` (commits ``1b1ebb6``, ``3719487``,
``d4dca4e``).
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from scripts import ossie_schema_drift as drift_check
from tests._dev2243_fixtures import CORPUS
from tests._dev2394_fixtures import (
    model_metric_over_order_revenue, orders_metrics, orders_with_field, renamed_model, validity_errors,
)

EXPRESSIONS = CORPUS / "compliance" / "expressions"
#: Query-shaped invalid.txt line upstream's validator still accepts.
DEFERRED_QUERY = "SELECT a FROM t"
DATASET_AGG = "AGG in a dataset-scoped metric takes a single source.measure reference"
CTX = "Metric 'orders.revenue_total' in model 'm' (OSSIE_SQL_2026)"


def sql_errors(errors: list[str]) -> list[str]:
    return [e for e in errors if e.startswith("[SQL]")]


# --------------------------------------------------------------------------- #
# AGG(source.measure) in a dataset-scoped metric (spec.md, Metric Scoping, rules 7–9)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("expression", [
    "AGG(source.revenue)",
    "agg(SOURCE.revenue)",
    'AGG(source."Revenue")',
    "AGG(source.revenue) / COUNT(DISTINCT order_id)",
    "COALESCE(AGG(source.revenue), 0) - AGG(source.refunds)",
    "CASE WHEN COUNT(*) > 0 THEN AGG(source.revenue) END",
])
def test_a_source_measure_call_is_accepted(expression: str) -> None:
    """Spec scenario: A source-measure call is accepted."""
    assert validity_errors(orders_metrics(revenue_total=expression)) == []


def test_a_model_may_evaluate_source_measures() -> None:
    doc = orders_metrics(revenue_total="AGG(source.revenue)",
                         revenue_per_order="AGG(source.revenue) / COUNT(DISTINCT order_id)")
    doc["metrics"] = [{"name": "revenue_per_customer",
                       "expression": {"sql": "AGG(orders.revenue_total) / COUNT(DISTINCT customers.id)"}}]
    assert validity_errors(doc) == []


@pytest.mark.parametrize(("expression", "expected"), [
    ("AGG(total_usd)", f"AGG(total_usd): {DATASET_AGG}"),
    ("AGG(order_count)", f"AGG(order_count): {DATASET_AGG}"),
    ("AGG(orders.order_count)", f"AGG(orders.order_count): {DATASET_AGG}"),
    ("AGG(source.revenue + 1)", f"AGG(source.revenue + 1): {DATASET_AGG}"),
    ("AGG(DISTINCT source.revenue)", f"AGG(DISTINCT source.revenue): {DATASET_AGG}"),
    ("AGG(source.*)", f"AGG(source.*): {DATASET_AGG}"),
    ("AGG(db.source.revenue)", f"AGG(db.source.revenue): {DATASET_AGG}"),
    ('AGG(source."")', f'AGG(source.""): {DATASET_AGG}'),
    ("AGG(source.revenue) FILTER (WHERE order_id = 1)",
     "AGG(source.revenue) has a FILTER (WHERE ...) clause; AGG does not take one"),
], ids=["field", "metric", "qualified-metric", "expression", "distinct", "star", "three-part", "empty-name", "filter"])
def test_an_invalid_dataset_scoped_agg_call_is_refused(expression: str, expected: str) -> None:
    """Spec scenario: AGG of a field in a dataset metric is refused (and every other invalid form)."""
    errors = validity_errors(orders_metrics(order_count="COUNT(*)", revenue_total=expression))
    assert len(errors) == 1, errors
    assert errors[0].startswith(f"[Scope] {CTX}: "), errors
    assert expected in errors[0]


def test_a_field_named_source_blocks_source_measures() -> None:
    """Spec scenario: A field named source blocks source measures."""
    doc = orders_metrics(revenue_total="AGG(source.revenue)")
    doc["datasets"][0]["fields"].append({"name": "source", "expression": {"sql": "src"}})
    errors = validity_errors(doc)
    assert len(errors) == 1, errors
    assert "declares a field named 'source'" in errors[0]


def test_agg_in_a_field_expression_is_refused_with_both_placements() -> None:
    errors = validity_errors(orders_with_field("AGG(source.revenue)"))
    assert errors == ["[Scope] Field 'orders.revenue' in model 'm' (OSSIE_SQL_2026) calls AGG in a field expression. "
                      "This version allows AGG only in metrics, as AGG(dataset.metric) in a model-scoped metric and "
                      "AGG(source.measure) in a dataset-scoped metric."]


def test_a_dataset_scoped_agg_is_reported_once() -> None:
    doc = renamed_model()
    doc["datasets"][0]["metrics"] = [{"name": "revenue", "expression": {"sql": "AGG(order_revenue)"}}]
    errors = validity_errors(doc)
    assert len(errors) == 1, errors
    assert DATASET_AGG in errors[0]


def test_agg_in_a_model_scoped_metric_is_accepted() -> None:
    assert validity_errors(model_metric_over_order_revenue("AGG(orders.order_revenue)")) == []


def test_a_superseded_portable_entry_is_not_checked() -> None:
    doc = renamed_model()
    doc["datasets"][0]["metrics"] = [{"name": "revenue_total", "expression": {
        "sql": "COUNT(*)", "dialects": [{"dialect": "OSSIE_SQL_2026", "expression": "AGG(total_usd)"}]}}]
    assert validity_errors(doc) == []


def test_an_unsuperseded_portable_entry_is_checked() -> None:
    doc = renamed_model()
    doc["datasets"][0]["metrics"] = [{"name": "revenue_total", "expression": {
        "dialects": [{"dialect": "OSSIE_SQL_2026", "expression": "AGG(total_usd)"}]}}]
    errors = validity_errors(doc)
    assert len(errors) == 1, errors
    assert DATASET_AGG in errors[0]


def test_vendor_dialects_are_unaffected() -> None:
    """Spec scenario: Vendor dialects are unaffected."""
    doc = renamed_model()
    doc["datasets"][0]["metrics"] = [{"name": "revenue_total", "expression": {
        "dialects": [{"dialect": "SNOWFLAKE", "expression": "AGG(total_usd)"}]}}]
    doc["datasets"][0]["fields"].append({"name": "due", "expression": {
        "dialects": [{"dialect": "POSTGRES", "expression": "order_date + INTERVAL '7 days'"}]}})
    assert validity_errors(doc) == []


# --------------------------------------------------------------------------- #
# Portable constructs OSSIE_SQL_2026 does not have (upstream 3719487)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("expression", "detail"), [
    ("order_date + INTERVAL '7 days'", "INTERVAL is not part of OSSIE_SQL_2026"),
    ("DATE_ADD(d, INTERVAL 7 DAY)", "INTERVAL is not part of OSSIE_SQL_2026"),
    ("INTERVAL '7 days'", "INTERVAL is not part of OSSIE_SQL_2026"),
    ("x IN ()", "IN list must contain at least one value"),
    ("x NOT IN ()", "IN list must contain at least one value"),
    ("a b", "expected an expression, found an alias"),
    ("a IS", "expected an expression, found an alias"),
    ("a AS b", "expected an expression, found an alias"),
    ("SUM(DISTINCT)", "DISTINCT requires an argument"),
    ("COUNT(DISTINCT)", "DISTINCT requires an argument"),
], ids=["interval-add", "date-add-interval", "bare-interval", "empty-in", "empty-not-in", "juxtaposition",
        "dangling-is", "explicit-alias", "sum-distinct", "count-distinct"])
def test_a_portable_construct_outside_the_language_is_refused(expression: str, detail: str) -> None:
    """Spec scenario: Portable INTERVAL is refused (and the other three constructs)."""
    errors = sql_errors(validity_errors(orders_with_field(expression)))
    assert len(errors) == 1, errors
    assert errors[0].startswith("[SQL] Field 'orders.revenue' in model 'm' (OSSIE_SQL_2026): "), errors
    assert detail in errors[0]


def test_a_construct_in_an_unsuperseded_portable_entry_is_refused() -> None:
    doc = orders_with_field({"dialects": [{"dialect": "OSSIE_SQL_2026", "expression": "x IN ()"}]})
    assert any("IN list must contain at least one value" in e for e in sql_errors(validity_errors(doc)))


def test_a_construct_in_a_superseded_portable_entry_is_not_checked() -> None:
    doc = orders_with_field({"sql": "total_cents",
                             "dialects": [{"dialect": "OSSIE_SQL_2026", "expression": "x IN ()"}]})
    assert validity_errors(doc) == []


@pytest.mark.parametrize("dialect", ["ANSI_SQL", "POSTGRES", "SNOWFLAKE"])
def test_other_dialects_keep_these_constructs(dialect: str) -> None:
    doc = orders_with_field({"dialects": [{"dialect": dialect, "expression": "order_date + INTERVAL '7 days'"}]})
    assert validity_errors(doc) == []


# --------------------------------------------------------------------------- #
# Aggregates in field expressions (upstream d4dca4e)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("expression", "names"), [
    ("SUM(total_cents)", "SUM"),
    ("COUNT(*)", "COUNT"),
    ("COUNT(DISTINCT o_custkey)", "COUNT"),
    ("SUM(total_cents) FILTER (WHERE total_cents > 0)", "SUM"),
    ("CASE WHEN AVG(total_cents) > 100 THEN 1 ELSE 0 END", "AVG"),
    ("total_cents / SUM(total_cents)", "SUM"),
    ("MIN(total_cents) + MAX(total_cents) + MIN(total_cents)", "MIN, MAX"),
    ("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_cents)", "PERCENTILE_CONT"),
    ("APPROX_PERCENTILE(total_cents, 0.95)", "APPROX_PERCENTILE"),
    ("SUM(SUM(total_cents)) OVER ()", "SUM"),
    ("ROW_NUMBER() OVER (ORDER BY SUM(total_cents))", "SUM"),
    ("ROW_NUMBER() OVER (PARTITION BY MAX(total_cents) ORDER BY o_orderkey)", "MAX"),
    ("SUM(total_cents) FILTER (WHERE total_cents > AVG(total_cents)) OVER ()", "AVG"),
])
def test_an_aggregate_in_a_field_is_refused(expression: str, names: str) -> None:
    """Spec scenario: An aggregate in a field is refused."""
    errors = validity_errors(orders_with_field(expression))
    assert len(errors) == 1, errors
    assert errors[0].startswith(f"[Scope] Field 'orders.revenue' in model 'm' (OSSIE_SQL_2026) calls {names} "
                                "without an OVER clause in a field expression."), errors


@pytest.mark.parametrize("call", [
    "SUM(x)", "COUNT(x)", "COUNT(*)", "COUNT(DISTINCT x)", "AVG(x)", "MIN(x)", "MAX(x)", "STDDEV(x)", "STDDEV_POP(x)",
    "STDDEV_SAMP(x)", "VARIANCE(x)", "VAR_POP(x)", "VAR_SAMP(x)", "MEDIAN(x)",
    "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)", "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY x)",
    "APPROX_COUNT_DISTINCT(x)", "APPROX_PERCENTILE(x, 0.95)",
])
def test_every_aggregate_of_the_language_is_refused_in_a_field(call: str) -> None:
    errors = validity_errors(orders_with_field(call))
    assert len(errors) == 1, errors
    assert "without an OVER clause in a field expression" in errors[0]


@pytest.mark.parametrize("expression", [
    "total_cents",
    "total_cents / 100.0",
    "COALESCE(total_cents, 0)",
    "GREATEST(total_cents, 0)",
    "total_cents / SUM(total_cents) OVER ()",
    "total_cents / SUM(total_cents) OVER (PARTITION BY o_custkey)",
    "SUM(total_cents) FILTER (WHERE total_cents > 0) OVER ()",
    "COUNT(DISTINCT o_custkey) OVER ()",
    "ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderkey)",
    "count",
    "sum",
    "(SELECT MAX(o.total_cents) FROM db.s.orders AS o)",
    "total_cents IN (SELECT MAX(o.total_cents) FROM db.s.orders AS o)",
])
def test_a_scalar_windowed_or_subquery_field_is_accepted(expression: str) -> None:
    assert not [e for e in validity_errors(orders_with_field(expression)) if "OVER clause" in e]


def test_the_scenarios_windowed_field_is_valid() -> None:
    """Spec scenario: An aggregate in a field is refused (its windowed half)."""
    assert validity_errors(orders_with_field("total_cents / SUM(total_cents) OVER ()")) == []


@pytest.mark.parametrize("dialect", ["ANSI_SQL", "SNOWFLAKE"])
def test_field_aggregates_skip_other_dialects(dialect: str) -> None:
    assert validity_errors(orders_with_field({"dialects": [{"dialect": dialect, "expression": "SUM(total_cents)"}]})) \
        == []


def test_field_aggregates_read_an_unsuperseded_portable_entry() -> None:
    doc = orders_with_field({"dialects": [{"dialect": "OSSIE_SQL_2026", "expression": "SUM(total_cents)"}]})
    assert len(validity_errors(doc)) == 1


def test_field_aggregates_check_the_top_level_sql_over_a_superseded_entry() -> None:
    superseded = {"dialects": [{"dialect": "OSSIE_SQL_2026", "expression": "SUM(total_cents)"}]}
    assert validity_errors(orders_with_field({"sql": "total_cents", **superseded})) == []
    errors = validity_errors(orders_with_field({"sql": "MAX(total_cents)", **superseded}))
    assert len(errors) == 1, errors
    assert "calls MAX without an OVER clause" in errors[0]


def test_field_aggregates_leave_metrics_alone() -> None:
    doc = renamed_model()
    doc["datasets"][0]["metrics"] = [{"name": "order_revenue", "expression": {"sql": "SUM(total_usd)"}}]
    doc["metrics"] = [{"name": "orders_seen", "expression": {"sql": "COUNT(orders.order_id)"}}]
    assert validity_errors(doc) == []


# --------------------------------------------------------------------------- #
# compliance/expressions parity, natively
# --------------------------------------------------------------------------- #
def fixture_lines(name: str) -> list[str]:
    """One expression per line; blank lines and '#' comments skipped."""
    text = (EXPRESSIONS / name).read_text(encoding="utf-8")
    return [line for line in text.splitlines() if line.strip() and not line.startswith("#")]


def test_every_valid_fixture_expression_passes_the_syntax_check() -> None:
    lines = fixture_lines("valid.txt")
    assert lines
    rejected = {line: sql_errors(validity_errors(orders_with_field(line))) for line in lines}
    assert {k: v for k, v in rejected.items() if v} == {}


def test_every_invalid_fixture_expression_fails_the_syntax_check() -> None:
    lines = [line for line in fixture_lines("invalid.txt") if line != DEFERRED_QUERY]
    assert lines
    accepted = [line for line in lines if not sql_errors(validity_errors(orders_with_field(line)))]
    assert accepted == []


def test_the_deferred_query_line_is_accepted_as_upstream_accepts_it() -> None:
    assert DEFERRED_QUERY in fixture_lines("invalid.txt")
    assert sql_errors(validity_errors(orders_with_field(DEFERRED_QUERY))) == []


#: Runs upstream ``validate_sql_expression`` on each line of a JSON list; prints the error-or-null list.
_UPSTREAM_DRIVER = """\
import importlib.util, json, sys
spec = importlib.util.spec_from_file_location("validate", sys.argv[1])
validate = importlib.util.module_from_spec(spec)
sys.modules["validate"] = validate
spec.loader.exec_module(validate)
lines = json.loads(open(sys.argv[2], encoding="utf-8").read())
print(json.dumps([validate.validate_sql_expression(line, "OSSIE_SQL_2026", "ctx") for line in lines]))
"""


def upstream_expression_errors(lines: list[str], *, tmp: Path) -> list[Any]:
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is not on PATH; upstream's validator runs through `uv run`")
    copy = tmp / "ossie"
    for part in ("validation", "core-spec", "python"):
        shutil.copytree(CORPUS / part, copy / part)
    validate = copy / "validation" / "validate.py"
    driver = tmp / "driver.py"
    driver.write_text(_UPSTREAM_DRIVER, encoding="utf-8")
    payload = tmp / "lines.json"
    payload.write_text(json.dumps(lines), encoding="utf-8")
    deps = drift_check.script_dependencies(validate)
    run = subprocess.run([uv, "run", "--no-project", *(a for d in deps for a in ("--with", d)), "python", str(driver),
                          str(validate), str(payload)], capture_output=True, text=True, check=False, timeout=600,
                         cwd=copy / "validation")
    assert run.returncode == 0, run.stderr[-4000:]
    return json.loads(run.stdout.strip().splitlines()[-1])


def test_the_syntax_check_agrees_with_upstream_on_every_fixture_line(tmp_path: Path) -> None:
    lines = fixture_lines("valid.txt") + fixture_lines("invalid.txt")
    upstream = upstream_expression_errors(lines, tmp=tmp_path)
    native = [bool(sql_errors(validity_errors(orders_with_field(line)))) for line in lines]
    disagreements = [line for line, theirs, ours in zip(lines, upstream, native, strict=True) if (theirs is not None) != ours]
    assert disagreements == []
