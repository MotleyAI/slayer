"""MetricFlow runner for the semantic-layer probe suite: build the dbt project, run every probe in ../probes.yaml."""

import argparse
import datetime as dt
import decimal
import logging
import math
import os
import re
import shutil
import sys
import tempfile
from importlib.metadata import version
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import duckdb
import yaml
from dbt.cli.main import dbtRunner
from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from pydantic import BaseModel, ConfigDict, Field

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
PASS, KNOWN, FIXED, FAIL = "PASS", "KNOWN-BUG", "FIXED", "FAIL"
STATUSES = (PASS, KNOWN, FIXED, FAIL)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]00:00:00(\.0+)?Z?)?$")
_NUM_RE = re.compile(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$")


class Compare(BaseModel):
    """How to line up engine output with truth_sql (truth columns are positional: keys, then values)."""

    model_config = ConfigDict(extra="forbid")
    keys: List[str] = Field(default_factory=list)
    values: List[str] = Field(default_factory=list)
    tolerance: float = 1e-6
    ordered: bool = False
    columns_exact: bool = False


class MfQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    metrics: Optional[List[str]] = None
    group_by: Optional[List[str]] = None
    where: Optional[List[str]] = None
    order: Optional[List[str]] = None
    limit: Optional[int] = None
    start: Optional[dt.datetime] = None
    end: Optional[dt.datetime] = None
    saved_query: Optional[str] = None


class MfExpect(BaseModel):
    model_config = ConfigDict(extra="forbid")
    error: Optional[str] = None
    known_bug: Optional[str] = None
    buggy_rows: List[List[Any]] = Field(default_factory=list)
    buggy_row_count: Optional[int] = None


class MfBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: MfQuery
    model_declared: bool = False
    keys: Optional[List[str]] = None
    values: Optional[List[str]] = None
    expect: Literal["match"] | MfExpect = "match"
    note: Optional[str] = None


class Probe(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: str
    row: str
    title: str
    truth_sql: Optional[str] = None
    contrast_sql: Optional[str] = None
    compare: Compare = Field(default_factory=Compare)
    metricflow: Optional[MfBlock] = None


class Result(BaseModel):
    columns: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    error: Optional[str] = None
    sql: Optional[str] = None


class Outcome(BaseModel):
    status: str
    detail: str


def sql_statements(text: str) -> List[str]:
    text = re.sub(r"--[^\n]*", "", text)
    return [s.strip() for s in text.split(";") if s.strip()]


def seed(db_path: Path) -> None:
    con = duckdb.connect(str(db_path))
    for stmt in sql_statements((ROOT / "dataset.sql").read_text()):
        con.execute(stmt)
    con.close()


def compute_truth(db_path: Path, probes: List[Probe]) -> Dict[Tuple[str, str], List[tuple]]:
    """All truth/contrast rows up front, before dbt and MetricFlow open the file."""
    con = duckdb.connect(str(db_path), read_only=True)
    out: Dict[Tuple[str, str], List[tuple]] = {}
    for p in probes:
        for kind in ("truth", "contrast"):
            sql = getattr(p, f"{kind}_sql")
            if sql:
                out[(p.id, kind)] = con.execute(sql).fetchall()
    con.close()
    return out


def build_engine(tmp: Path, db_path: Path) -> MetricFlowEngine:
    """Copy the dbt project into tmp (MetricFlow reads <project>/target), run it, and load MetricFlow over it."""
    project = tmp / "dbt_project"
    shutil.copytree(HERE / "dbt_project", project, ignore=shutil.ignore_patterns("target", "logs"))
    os.environ.update(
        MF_PROBE_DB=str(db_path), DO_NOT_TRACK="1", DBT_SEND_ANONYMOUS_USAGE_STATS="false",
        DBT_LOG_PATH=str(tmp / "logs"),
    )
    dirs = ["--project-dir", str(project), "--profiles-dir", str(project)]
    res = dbtRunner().invoke(["--quiet", "run", *dirs])
    if not res.success:
        raise SystemExit(f"dbt run failed: {res.exception}")
    cfg = CLIConfiguration()
    cfg.setup(dbt_profiles_path=project, dbt_project_path=project, configure_file_logging=False)
    logging.disable(logging.CRITICAL)
    return cfg.mf


def run_query(mf: MetricFlowEngine, q: MfQuery) -> Result:
    req = MetricFlowQueryRequest.create(
        saved_query_name=q.saved_query, metric_names=q.metrics, group_by_names=q.group_by,
        where_constraints=q.where, order_by_names=q.order, limit=q.limit,
        time_constraint_start=q.start, time_constraint_end=q.end,
    )
    try:
        res = mf.query(mf_request=req)
    except Exception as e:  # noqa: BLE001 - a probe's error is data
        return Result(error=f"{type(e).__name__}: {e}")
    table = res.result_df
    assert table is not None
    return Result(columns=list(table.column_names), rows=[list(r) for r in table.rows], sql=res.sql)


def canon(v: Any) -> Any:
    """Normalise a cell so MetricFlow and DuckDB spellings of one value compare equal."""
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, dt.datetime):
        return v.strftime("%Y-%m-%d") if v.time() == dt.time() else v.isoformat()
    if isinstance(v, dt.date):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, (int, float, decimal.Decimal)):
        f = float(v)
        return int(f) if f.is_integer() else f
    if isinstance(v, str):
        if _DATE_RE.match(v):
            return v[:10]
        if _NUM_RE.match(v):
            return canon(float(v))
    return v


def cell_eq(a: Any, b: Any, tol: float) -> bool:
    a, b = canon(a), canon(b)
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool):
        return math.isclose(a, b, rel_tol=tol, abs_tol=tol)
    return a == b


def _sort_key(row: List[Any], nkeys: int) -> tuple:
    part = row[:nkeys] if nkeys else row
    return tuple((x is None, str(x)) for x in (canon(c) for c in part))


def rows_match(got: List[List[Any]], exp: List[List[Any]], cmp: Compare, nkeys: int) -> Tuple[bool, str]:
    if len(got) != len(exp):
        return False, f"{len(got)} rows, expected {len(exp)}"
    if not cmp.ordered:
        got = sorted(got, key=lambda r: _sort_key(r, nkeys))
        exp = sorted(exp, key=lambda r: _sort_key(r, nkeys))
    for g, e in zip(got, exp):
        if len(g) != len(e) or not all(cell_eq(a, b, cmp.tolerance) for a, b in zip(g, e)):
            return False, f"row {[canon(x) for x in g]} != expected {[canon(x) for x in e]}"
    return True, f"{len(exp)} rows match"


def has_row(got: List[List[Any]], want: List[Any], tol: float) -> bool:
    return any(len(g) == len(want) and all(cell_eq(a, b, tol) for a, b in zip(g, want)) for g in got)


def first_line(s: str) -> str:
    return " ".join(s.split())[:200]


def _error_outcome(ex: MfExpect, res: Result) -> Optional[Outcome]:
    err = res.error or ""
    if ex.error:
        if not res.error:
            return Outcome(status=FAIL, detail=f"expected error {ex.error!r}, got {len(res.rows)} rows")
        if ex.error not in err:
            return Outcome(status=FAIL, detail=f"wrong error: {first_line(err)}")
        return Outcome(status=PASS, detail=f"errors as expected: {first_line(ex.error)}")
    if res.error:
        return Outcome(status=FAIL, detail=f"unexpected error: {first_line(err)}")
    return None


def _known_bug_outcome(ex: MfExpect, got: List[List[Any]], ok: bool, why: str, tol: float) -> Outcome:
    if ok:
        return Outcome(status=FIXED, detail=f"now matches truth ({why}); update the expectation")
    sig = all(has_row(got, b, tol) for b in ex.buggy_rows)
    if ex.buggy_row_count is not None:
        sig = sig and len(got) == ex.buggy_row_count
    if sig:
        return Outcome(status=KNOWN, detail=f"{ex.known_bug} ({why})")
    return Outcome(status=FAIL, detail=f"neither truth nor the known buggy value: {why}")


def evaluate(p: Probe, res: Result, truth: Dict) -> Outcome:
    block = p.metricflow
    assert block is not None
    ex = block.expect if isinstance(block.expect, MfExpect) else MfExpect()
    failed = _error_outcome(ex, res)
    if failed:
        return failed

    keys = block.keys if block.keys is not None else p.compare.keys
    values = block.values if block.values is not None else p.compare.values
    missing = [c for c in keys + values if c not in res.columns]
    if missing:
        return Outcome(status=FAIL, detail=f"columns {missing} not in {res.columns}")
    if p.compare.columns_exact and res.columns != keys + values:
        return Outcome(status=FAIL, detail=f"columns {res.columns} != {keys + values}")
    got = [[r[res.columns.index(c)] for c in keys + values] for r in res.rows]
    exp_rows = [list(r) for r in truth.get((p.id, "truth"), [])]
    ok, why = rows_match(got, exp_rows, p.compare, len(keys)) if p.truth_sql else (True, "no truth_sql")

    if ex.known_bug:
        return _known_bug_outcome(ex, got, ok, why, p.compare.tolerance)
    if not ok:
        return Outcome(status=FAIL, detail=why)
    if p.contrast_sql:
        contrast = [list(r) for r in truth[(p.id, "contrast")]]
        if rows_match(got, contrast, p.compare, len(keys))[0]:
            return Outcome(status=FAIL, detail="result also equals contrast_sql; the probe shows nothing")
    return Outcome(status=PASS, detail=why)


def row_order(r: str) -> tuple:
    return (r[0], int(re.sub(r"\D", "", r) or 0))


def print_summary(title: str, results: List[Tuple[Probe, Outcome]]) -> None:
    if not results:
        return
    print(f"\n{title}")
    print(f"{'row':<6}" + "".join(f"{s:>11}" for s in STATUSES))
    for r in sorted({p.row for p, _ in results}, key=row_order):
        counts = [sum(1 for p, o in results if p.row == r and o.status == s) for s in STATUSES]
        print(f"{r:<6}" + "".join(f"{c:>11}" for c in counts))
    totals = [sum(1 for _, o in results if o.status == s) for s in STATUSES]
    print(f"{'total':<6}" + "".join(f"{c:>11}" for c in totals) + f"   ({len(results)} probes)")


def run_probe(mf: Any, p: Probe, truth: Dict, verbose: bool) -> Outcome:
    assert p.metricflow is not None
    res = run_query(mf, p.metricflow.query)
    outcome = evaluate(p, res, truth)
    tag = " [model]" if p.metricflow.model_declared else ""
    print(f"{outcome.status:<9} {p.id:<28} {p.row:<4} {(p.title + tag)[:64]:<64}  {outcome.detail}")
    if verbose:
        print(f"    sql: {res.sql}\n    columns: {res.columns}\n    rows: {res.rows}")
        if res.error:
            print(f"    error: {res.error[:2000]}")
        if (p.id, "truth") in truth:
            print(f"    truth: {truth[(p.id, 'truth')]}")
    return outcome


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--row", help="only probes of this matrix row, e.g. Q4")
    ap.add_argument("--id", help="only probes whose id contains this substring")
    ap.add_argument("--verbose", action="store_true", help="show SQL, truth and returned values")
    args = ap.parse_args()

    doc = yaml.safe_load((ROOT / "probes.yaml").read_text())
    probes = [Probe.model_validate(p) for p in doc["probes"]]
    probes = [
        p for p in probes
        if p.metricflow and (not args.row or p.row == args.row) and (not args.id or args.id in p.id)
    ]
    vers = ", ".join(f"{n} {version(n)}" for n in ("metricflow", "dbt-metricflow", "dbt-core", "dbt-duckdb", "duckdb"))
    print(f"=== MetricFlow ({vers}) ===")
    with tempfile.TemporaryDirectory(prefix="metricflow-probes-") as tmp_name:
        tmp = Path(tmp_name)
        db_path = tmp / "probe.duckdb"
        seed(db_path)
        truth = compute_truth(db_path, probes)
        mf = build_engine(tmp, db_path)
        results = [(p, run_probe(mf, p, truth, args.verbose)) for p in probes]
    declared = {p.id for p in probes if p.metricflow and p.metricflow.model_declared}
    print_summary("Summary (MetricFlow, query-only)", [(p, o) for p, o in results if p.id not in declared])
    print_summary("Summary (MetricFlow, model-declared)", [(p, o) for p, o in results if p.id in declared])
    return 1 if any(o.status == FAIL for _, o in results) else 0


if __name__ == "__main__":
    sys.exit(main())
