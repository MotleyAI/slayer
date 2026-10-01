"""SLayer runner for the semantic-layer probe suite: run every probe in ../probes.yaml, check against its expectation."""

import argparse
import asyncio
import datetime as dt
import decimal
import math
import re
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import duckdb
import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from slayer.async_utils import run_sync
from slayer.core.granularity import CustomGranularity
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.policy import SessionPolicy
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.yaml_storage import YAMLStorage

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
DATASOURCE = "probe"
# Relative time tokens ("last 3 months") and the spine's default upper bound read this instant.
NOW = dt.datetime(2025, 7, 15, 12, 0)
GRANULARITIES = [
    CustomGranularity(name="fiscal_year", base="month", multiple=12, origin=dt.datetime(2024, 4, 1)),
    CustomGranularity(name="quarter_hour", base="minute", multiple=15),
]
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


class SlayerExpect(BaseModel):
    model_config = ConfigDict(extra="forbid")
    error: Optional[str] = None
    warning: Optional[str] = None
    known_bug: Optional[str] = None
    buggy_rows: List[List[Any]] = Field(default_factory=list)
    buggy_row_count: Optional[int] = None
    buggy_error: Optional[str] = None


class SetupModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    query: Union[Dict[str, Any], List[Dict[str, Any]]]


class SlayerBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None
    run_by_name: Optional[str] = None
    refine: Optional[Dict[str, Any]] = None
    options: Dict[str, Any] = Field(default_factory=dict)
    # Files in slayer/extra_models/ saved for this probe only (they would otherwise add join cycles).
    extra_models: List[str] = Field(default_factory=list)
    create_model_from_query: Optional[SetupModel] = None
    keys: Optional[List[str]] = None
    values: Optional[List[str]] = None
    expect: Union[Literal["match"], SlayerExpect] = "match"
    note: Optional[str] = None

    @model_validator(mode="after")
    def _one_source(self) -> "SlayerBlock":
        if (self.query is None) == (self.run_by_name is None):
            raise ValueError("a slayer block needs exactly one of query / run_by_name")
        if self.refine is not None and self.run_by_name is None:
            raise ValueError("refine applies only to run_by_name")
        return self


class Probe(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: str
    row: str
    title: str
    truth_sql: Optional[str] = None
    contrast_sql: Optional[str] = None
    compare: Compare = Field(default_factory=Compare)
    slayer: Optional[SlayerBlock] = None


class Outcome(BaseModel):
    status: str
    detail: str


def load_probes(path: Path) -> Tuple[List[Probe], Dict[str, Any]]:
    doc = yaml.safe_load(path.read_text())
    probes = [Probe.model_validate(p) for p in doc["probes"]]
    ids = [p.id for p in probes]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        raise SystemExit(f"duplicate probe ids: {dupes}")
    return probes, doc.get("slayer_policies", {})


def sql_statements(text: str) -> List[str]:
    text = re.sub(r"--[^\n]*", "", text)
    return [s.strip() for s in text.split(";") if s.strip()]


def seed(db_path: Path) -> None:
    con = duckdb.connect(str(db_path))
    for stmt in sql_statements((ROOT / "dataset.sql").read_text()):
        con.execute(stmt)
    con.close()


def compute_truth(db_path: Path, probes: List[Probe]) -> Dict[Tuple[str, str], List[tuple]]:
    """All truth/contrast rows up front, so no SQL connection is held while SLayer runs."""
    con = duckdb.connect(str(db_path), read_only=True)
    out: Dict[Tuple[str, str], List[tuple]] = {}
    for p in probes:
        for kind in ("truth", "contrast"):
            sql = getattr(p, f"{kind}_sql")
            if sql:
                out[(p.id, kind)] = con.execute(sql).fetchall()
    con.close()
    return out


async def build_engines(
    db_path: Path, store: Path, policies: Dict[str, Any]
) -> Tuple[SlayerQueryEngine, Dict[str, SlayerQueryEngine]]:
    storage = YAMLStorage(base_dir=str(store))
    await storage.save_datasource(
        DatasourceConfig(name=DATASOURCE, type="duckdb", database=str(db_path), granularities=GRANULARITIES)
    )
    engine = SlayerQueryEngine(storage=storage, clock=lambda: NOW)
    docs = [yaml.safe_load(f.read_text()) for f in sorted((HERE / "models").glob("*.yaml"))]
    # Query-backed models validate by dry-run, so their sources must exist first.
    for doc in sorted(docs, key=lambda d: bool(d.get("source_queries"))):
        await engine.save_model(SlayerModel.model_validate(doc))
    by_policy = {
        name: SlayerQueryEngine(storage=storage, policy=SessionPolicy.model_validate(spec), clock=lambda: NOW)
        for name, spec in policies.items()
    }
    return engine, by_policy


def canon(v: Any) -> Any:
    """Normalise a cell so SLayer, Malloy and DuckDB spellings of one value compare equal."""
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


def resolve(name: str, columns: List[str]) -> str:
    """Pick the output column named ``name`` exactly, else the one ending in ``.name``."""
    if name in columns:
        return name
    hits = [c for c in columns if c.endswith("." + name)]
    if len(hits) != 1:
        raise KeyError(f"column {name!r} -> {hits or 'no match'} in {columns}")
    return hits[0]


def project(resp: SlayerResponse, keys: List[str], values: List[str]) -> List[List[Any]]:
    cols = [resolve(c, resp.columns) for c in keys + values]
    return [[r[c] for c in cols] for r in resp.data]


def run_query(engine: SlayerQueryEngine, block: SlayerBlock) -> Tuple[Optional[SlayerResponse], Optional[Exception]]:
    handling = block.options.get("to_many_handling")
    extras = [SlayerModel.model_validate(yaml.safe_load((HERE / "extra_models" / f).read_text()))
              for f in block.extra_models]
    try:
        for model in extras:
            run_sync(engine.save_model(model))
        if block.create_model_from_query:
            engine.create_model_from_query_sync(
                query=block.create_model_from_query.query, name=block.create_model_from_query.name
            )
        if block.run_by_name is not None:
            refine = {**(block.refine or {}), **({"to_many_handling": handling} if handling else {})}
            return engine.execute_sync(block.run_by_name, refine=refine or None), None
        query: Any = block.query
        if handling:
            query = {**query, "to_many_handling": handling}
        return engine.execute_sync(query), None
    except Exception as e:  # noqa: BLE001 - a probe's error is data
        return None, e
    finally:
        for model in extras:
            run_sync(engine.delete_model_by_name(model_name=model.name, data_source=model.data_source))


def first_line(e: Exception) -> str:
    return f"{type(e).__name__}: {str(e).strip().splitlines()[0][:160]}"


def evaluate(p: Probe, resp: Optional[SlayerResponse], err: Optional[Exception], truth: Dict) -> Outcome:
    block = p.slayer
    assert block is not None
    expect = block.expect
    exp_obj = expect if isinstance(expect, SlayerExpect) else SlayerExpect()

    if exp_obj.error:
        if err is None:
            return Outcome(status=FAIL, detail=f"expected error {exp_obj.error!r}, got {resp.row_count} rows")
        if exp_obj.error == type(err).__name__ or exp_obj.error in str(err):
            return Outcome(status=PASS, detail=f"errors as expected: {first_line(err)}")
        return Outcome(status=FAIL, detail=f"wrong error: {first_line(err)}")
    if err is not None and exp_obj.known_bug and exp_obj.buggy_error:
        if exp_obj.buggy_error == type(err).__name__ or exp_obj.buggy_error in str(err):
            return Outcome(status=KNOWN, detail=f"{exp_obj.known_bug} ({first_line(err)})")
        return Outcome(status=FAIL, detail=f"neither truth nor the known buggy error: {first_line(err)}")
    if err is not None:
        return Outcome(status=FAIL, detail=f"unexpected error: {first_line(err)}")

    assert resp is not None
    keys = block.keys if block.keys is not None else p.compare.keys
    values = block.values if block.values is not None else p.compare.values
    kinds = [w.kind for w in resp.warnings]
    try:
        got = project(resp, keys, values)
    except KeyError as e:
        return Outcome(status=FAIL, detail=str(e))
    if p.compare.columns_exact:
        want_cols = [resolve(c, resp.columns) for c in keys + values]
        if resp.columns != want_cols:
            return Outcome(status=FAIL, detail=f"columns {resp.columns} != {want_cols}")
    exp_rows = [list(r) for r in truth.get((p.id, "truth"), [])]
    ok, why = rows_match(got, exp_rows, p.compare, len(keys)) if p.truth_sql else (True, "no truth_sql")

    if exp_obj.known_bug:
        if ok:
            return Outcome(status=FIXED, detail=f"now matches truth ({why}); update the expectation")
        # An error signature needs the error; a result can't reproduce it.
        sig = not exp_obj.buggy_error and all(has_row(got, b, p.compare.tolerance) for b in exp_obj.buggy_rows)
        if exp_obj.buggy_row_count is not None:
            sig = sig and len(got) == exp_obj.buggy_row_count
        if sig:
            return Outcome(status=KNOWN, detail=f"{exp_obj.known_bug} ({why})")
        return Outcome(status=FAIL, detail=f"neither truth nor the known buggy value: {why}")
    if not ok:
        return Outcome(status=FAIL, detail=f"{why}; warnings={kinds}")
    if p.contrast_sql:
        contrast = [list(r) for r in truth[(p.id, "contrast")]]
        if rows_match(got, contrast, p.compare, len(keys))[0]:
            return Outcome(status=FAIL, detail="result also equals contrast_sql; the probe shows nothing")
    if exp_obj.warning and exp_obj.warning not in kinds:
        return Outcome(status=FAIL, detail=f"{why}, but warning {exp_obj.warning!r} missing (got {kinds})")
    suffix = f" + warning {exp_obj.warning}" if exp_obj.warning else ""
    return Outcome(status=PASS, detail=f"{why}{suffix}")


def print_summary(results: List[Tuple[Probe, Outcome]]) -> None:
    rows = sorted({p.row for p, _ in results}, key=lambda r: (r[0], int(re.sub(r"\D", "", r) or 0)))
    print("\nSummary (SLayer)")
    print(f"{'row':<6}" + "".join(f"{s:>11}" for s in STATUSES))
    for r in rows:
        counts = [sum(1 for p, o in results if p.row == r and o.status == s) for s in STATUSES]
        print(f"{r:<6}" + "".join(f"{c:>11}" for c in counts))
    totals = [sum(1 for _, o in results if o.status == s) for s in STATUSES]
    print(f"{'total':<6}" + "".join(f"{c:>11}" for c in totals) + f"   ({len(results)} probes)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--row", help="only probes of this matrix row, e.g. Q4")
    ap.add_argument("--id", help="only probes whose id contains this substring")
    ap.add_argument("--verbose", action="store_true", help="show SQL, truth and returned values")
    args = ap.parse_args()
    warnings.simplefilter("ignore")  # responses carry their own typed warnings

    probes, policies = load_probes(ROOT / "probes.yaml")
    probes = [
        p for p in probes
        if p.slayer and (not args.row or p.row == args.row) and (not args.id or args.id in p.id)
    ]
    with tempfile.TemporaryDirectory(prefix="slayer-probes-") as tmp:
        db_path = Path(tmp) / "probe.duckdb"
        seed(db_path)
        truth = compute_truth(db_path, probes)
        engine, by_policy = asyncio.run(build_engines(db_path, Path(tmp) / "store", policies))
        results: List[Tuple[Probe, Outcome]] = []
        for p in probes:
            assert p.slayer is not None
            policy = p.slayer.options.get("policy")
            resp, err = run_query(by_policy[policy] if policy else engine, p.slayer)
            outcome = evaluate(p, resp, err, truth)
            results.append((p, outcome))
            print(f"{outcome.status:<9} {p.id:<22} {p.row:<4} {p.title[:60]:<60}  {outcome.detail}")
            if args.verbose:
                if resp is not None:
                    print(f"    sql: {resp.sql}\n    columns: {resp.columns}\n    data: {resp.data}")
                    print(f"    warnings: {[w.kind for w in resp.warnings]}")
                if err is not None:
                    print(f"    error: {err}")
                if (p.id, "truth") in truth:
                    print(f"    truth: {truth[(p.id, 'truth')]}")
    print_summary(results)
    return 1 if any(o.status == FAIL for _, o in results) else 0


if __name__ == "__main__":
    sys.exit(main())
