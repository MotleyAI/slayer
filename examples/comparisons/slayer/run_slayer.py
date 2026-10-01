"""SLayer runner for the semantic-layer probe suite: run every probe in ../probes.yaml, check against its expectation."""

import argparse
import asyncio
import contextlib
import datetime as dt
import decimal
import json
import math
import os
import re
import shutil
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import duckdb
import sqlalchemy as sa
import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from slayer.async_utils import run_sync
from slayer.core.granularity import CustomGranularity
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.policy import SessionPolicy
from slayer.embeddings.client import SLAYER_EMBEDDING_MODEL_ENV
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.mcp.server import create_mcp_server
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
    # Substrings the last step's text output must contain (steps probes only).
    contains: List[str] = Field(default_factory=list)


class SetupModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    query: Dict[str, Any] | List[Dict[str, Any]]


class Step(BaseModel):
    """An MCP tool call, or SQL run against the probe's own copy of the database."""

    model_config = ConfigDict(extra="forbid")
    tool: Optional[str] = None
    args: Dict[str, Any] = Field(default_factory=dict)
    sql: Optional[str] = None

    @model_validator(mode="after")
    def _one_kind(self) -> "Step":
        if (self.tool is None) == (self.sql is None):
            raise ValueError("a step needs exactly one of tool / sql")
        return self


class SlayerBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: Dict[str, Any] | List[Dict[str, Any]] | None = None
    run_by_name: Optional[str] = None
    # An MCP session on a private copy of the database; the last step must be a tool call.
    steps: List[Step] = Field(default_factory=list)
    # What the session's storage starts with: the probe models, only the datasource, or nothing.
    setup: Literal["models", "datasource", "empty"] = "models"
    refine: Optional[Dict[str, Any]] = None
    options: Dict[str, Any] = Field(default_factory=dict)
    # Files in slayer/extra_models/ saved for this probe only (they would otherwise add join cycles).
    extra_models: List[str] = Field(default_factory=list)
    create_model_from_query: Optional[SetupModel] = None
    keys: Optional[List[str]] = None
    values: Optional[List[str]] = None
    expect: Literal["match"] | SlayerExpect = "match"
    note: Optional[str] = None

    @model_validator(mode="after")
    def _one_source(self) -> "SlayerBlock":
        if sum(x is not None for x in (self.query, self.run_by_name, self.steps or None)) != 1:
            raise ValueError("a slayer block needs exactly one of query / run_by_name / steps")
        if self.refine is not None and self.run_by_name is None:
            raise ValueError("refine applies only to run_by_name")
        if self.steps and self.steps[-1].tool is None:
            raise ValueError("the last step must be a tool call")
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


async def fill_store(storage: YAMLStorage, db_path: Path, setup: str) -> SlayerQueryEngine:
    """Register the datasource and save the probe models, as far as ``setup`` asks."""
    engine = SlayerQueryEngine(storage=storage, clock=lambda: NOW)
    if setup == "empty":
        return engine
    await storage.save_datasource(
        DatasourceConfig(name=DATASOURCE, type="duckdb", database=str(db_path), granularities=GRANULARITIES)
    )
    if setup == "datasource":
        return engine
    docs = [yaml.safe_load(f.read_text()) for f in sorted((HERE / "models").glob("*.yaml"))]
    # Query-backed models validate by dry-run, so their sources must exist first.
    for doc in sorted(docs, key=lambda d: bool(d.get("source_queries"))):
        await engine.save_model(SlayerModel.model_validate(doc))
    return engine


async def build_engines(
    db_path: Path, store: Path, policies: Dict[str, Any]
) -> Tuple[SlayerQueryEngine, Dict[str, SlayerQueryEngine]]:
    storage = YAMLStorage(base_dir=str(store))
    engine = await fill_store(storage=storage, db_path=db_path, setup="models")
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
    saved: List[SlayerModel] = []
    try:
        for model in extras:
            run_sync(engine.save_model(model))
            saved.append(model)
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
        for model in saved:
            # A failed cleanup must not mask the probe's own outcome.
            with contextlib.suppress(Exception):
                run_sync(engine.delete_model_by_name(model_name=model.name, data_source=model.data_source))


class SessionResult(BaseModel):
    """The last step's text, its rows when it returned JSON rows, or the first error."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    text: str = ""
    resp: Optional[SlayerResponse] = None
    err: Optional[Exception] = None


def run_sql(db_path: Path, sql: str) -> None:
    # Through duckdb_engine, so the connection shares the MCP engine's open database instead of clashing with it.
    eng = sa.create_engine(f"duckdb:///{db_path}")
    try:
        with eng.begin() as con:
            for stmt in sql_statements(sql):
                con.exec_driver_sql(stmt)
    finally:
        eng.dispose()


def as_response(text: str) -> Optional[SlayerResponse]:
    """The rows of a ``query`` tool call with ``format: json``, else None."""
    try:
        payload = json.loads(text)
    except ValueError:
        return None
    if isinstance(payload, list):
        payload = {"data": payload}
    if not isinstance(payload, dict) or "data" not in payload:
        return None
    return SlayerResponse.model_validate({"data": payload["data"], "warnings": payload.get("warnings", [])})


def run_session(block: SlayerBlock, base_db: Path, workdir: Path) -> SessionResult:
    """Run the steps through the MCP server on a private copy of the database and a fresh store."""
    workdir.mkdir()
    db_path = workdir / "probe.duckdb"
    shutil.copy(base_db, db_path)
    storage = YAMLStorage(base_dir=str(workdir / "store"))
    run_sync(fill_store(storage=storage, db_path=db_path, setup=block.setup))
    server = create_mcp_server(storage)
    text = ""
    try:
        for step in block.steps:
            if step.sql is not None:
                run_sql(db_path=db_path, sql=step.sql)
                continue
            args = json.loads(json.dumps(step.args).replace("{db_path}", str(db_path)))
            blocks, _ = run_sync(server.call_tool(step.tool, args))
            text = blocks[0].text
    except Exception as e:  # noqa: BLE001 - a probe's error is data
        return SessionResult(err=e)
    finally:
        run_sync(server._slayer_engine.aclose())
    return SessionResult(text=text, resp=as_response(text) if block.steps[-1].tool == "query" else None)


def evaluate_session(p: Probe, res: SessionResult, truth: Dict) -> Outcome:
    assert p.slayer is not None
    ex = p.slayer.expect if isinstance(p.slayer.expect, SlayerExpect) else SlayerExpect()
    if res.err is None and ex.error:
        return Outcome(status=FAIL, detail=f"expected error {ex.error!r}, got: {res.text[:120]!r}")
    if res.err is None and ex.contains:
        missing = [s for s in ex.contains if s not in res.text]
        if missing:
            return Outcome(status=FAIL, detail=f"output lacks {missing}")
        return Outcome(status=PASS, detail=f"output has {ex.contains}")
    if res.err is None and res.resp is None:
        return Outcome(status=FAIL, detail=f"no rows to compare: {res.text[:120]!r}")
    return evaluate(p, res.resp, res.err, truth)


def first_line(e: Exception) -> str:
    return f"{type(e).__name__}: {str(e).strip().splitlines()[0][:160]}"


def _matches(expected: str, err: Exception) -> bool:
    return expected == type(err).__name__ or expected in str(err)


def _error_outcome(ex: SlayerExpect, resp: Optional[SlayerResponse], err: Optional[Exception]) -> Optional[Outcome]:
    if ex.error:
        if err is None:
            assert resp is not None
            return Outcome(status=FAIL, detail=f"expected error {ex.error!r}, got {resp.row_count} rows")
        if _matches(ex.error, err):
            return Outcome(status=PASS, detail=f"errors as expected: {first_line(err)}")
        return Outcome(status=FAIL, detail=f"wrong error: {first_line(err)}")
    if err is None:
        return None
    if ex.known_bug and ex.buggy_error:
        if _matches(ex.buggy_error, err):
            return Outcome(status=KNOWN, detail=f"{ex.known_bug} ({first_line(err)})")
        return Outcome(status=FAIL, detail=f"neither truth nor the known buggy error: {first_line(err)}")
    return Outcome(status=FAIL, detail=f"unexpected error: {first_line(err)}")


def _known_bug_outcome(ex: SlayerExpect, got: List[List[Any]], ok: bool, why: str, tol: float) -> Outcome:
    if ok:
        return Outcome(status=FIXED, detail=f"now matches truth ({why}); update the expectation")
    # An error signature needs the error; a result can't reproduce it.
    sig = not ex.buggy_error and all(has_row(got, b, tol) for b in ex.buggy_rows)
    if ex.buggy_row_count is not None:
        sig = sig and len(got) == ex.buggy_row_count
    if sig:
        return Outcome(status=KNOWN, detail=f"{ex.known_bug} ({why})")
    return Outcome(status=FAIL, detail=f"neither truth nor the known buggy value: {why}")


def _truth_outcome(
    p: Probe, ex: SlayerExpect, got: List[List[Any]], ok: bool, why: str, kinds: List[str], truth: Dict
) -> Outcome:
    if not ok:
        return Outcome(status=FAIL, detail=f"{why}; warnings={kinds}")
    n_keys = len(p.slayer.keys if p.slayer and p.slayer.keys is not None else p.compare.keys)
    if p.contrast_sql and rows_match(got, [list(r) for r in truth[(p.id, "contrast")]], p.compare, n_keys)[0]:
        return Outcome(status=FAIL, detail="result also equals contrast_sql; the probe shows nothing")
    if ex.warning and ex.warning not in kinds:
        return Outcome(status=FAIL, detail=f"{why}, but warning {ex.warning!r} missing (got {kinds})")
    suffix = f" + warning {ex.warning}" if ex.warning else ""
    return Outcome(status=PASS, detail=f"{why}{suffix}")


def evaluate(p: Probe, resp: Optional[SlayerResponse], err: Optional[Exception], truth: Dict) -> Outcome:
    block = p.slayer
    assert block is not None
    ex = block.expect if isinstance(block.expect, SlayerExpect) else SlayerExpect()
    failed = _error_outcome(ex, resp, err)
    if failed:
        return failed

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

    if ex.known_bug:
        return _known_bug_outcome(ex, got, ok, why, p.compare.tolerance)
    return _truth_outcome(p, ex, got, ok, why, kinds, truth)


def print_summary(results: List[Tuple[Probe, Outcome]]) -> None:
    rows = sorted({p.row for p, _ in results}, key=lambda r: (r[0], int(re.sub(r"\D", "", r) or 0)))
    print("\nSummary (SLayer)")
    print(f"{'row':<6}" + "".join(f"{s:>11}" for s in STATUSES))
    for r in rows:
        counts = [sum(1 for p, o in results if p.row == r and o.status == s) for s in STATUSES]
        print(f"{r:<6}" + "".join(f"{c:>11}" for c in counts))
    totals = [sum(1 for _, o in results if o.status == s) for s in STATUSES]
    print(f"{'total':<6}" + "".join(f"{c:>11}" for c in totals) + f"   ({len(results)} probes)")


def print_verbose(p: Probe, resp: Optional[SlayerResponse], err: Optional[Exception], truth: Dict) -> None:
    if resp is not None:
        print(f"    sql: {resp.sql}\n    columns: {resp.columns}\n    data: {resp.data}")
        print(f"    warnings: {[w.kind for w in resp.warnings]}")
    if err is not None:
        print(f"    error: {err}")
    if (p.id, "truth") in truth:
        print(f"    truth: {truth[(p.id, 'truth')]}")


def run_probe(
    p: Probe, engine: SlayerQueryEngine, by_policy: Dict[str, SlayerQueryEngine], truth: Dict, verbose: bool,
    tmp: Path,
) -> Outcome:
    assert p.slayer is not None
    text = ""
    if p.slayer.steps:
        res = run_session(block=p.slayer, base_db=tmp / "probe.duckdb", workdir=tmp / p.id)
        resp, err, text = res.resp, res.err, res.text
        outcome = evaluate_session(p=p, res=res, truth=truth)
    else:
        policy = p.slayer.options.get("policy")
        resp, err = run_query(by_policy[policy] if policy else engine, p.slayer)
        outcome = evaluate(p, resp, err, truth)
    print(f"{outcome.status:<9} {p.id:<22} {p.row:<4} {p.title[:60]:<60}  {outcome.detail}")
    if verbose:
        if text and resp is None:
            print(f"    output: {text}")
        print_verbose(p, resp, err, truth)
    return outcome


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--row", help="only probes of this matrix row, e.g. Q4")
    ap.add_argument("--id", help="only probes whose id contains this substring")
    ap.add_argument("--verbose", action="store_true", help="show SQL, truth and returned values")
    args = ap.parse_args()
    warnings.simplefilter("ignore")  # responses carry their own typed warnings
    # Search runs on its offline channels only: no embedding provider, so results don't depend on one.
    os.environ[SLAYER_EMBEDDING_MODEL_ENV] = "openai/text-embedding-3-small"
    os.environ.pop("OPENAI_API_KEY", None)

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
        results = [(p, run_probe(p, engine, by_policy, truth, args.verbose, Path(tmp))) for p in probes]
    print_summary(results)
    return 1 if any(o.status == FAIL for _, o in results) else 0


if __name__ == "__main__":
    sys.exit(main())
