"""Cube Core runner for the semantic-layer probe suite: start Cube, run every probe in ../probes.yaml, check it."""

import argparse
import datetime as dt
import decimal
import json
import math
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, LiteralString, Literal, Optional, Tuple, cast

import duckdb
import psycopg
import yaml
from pydantic import BaseModel, ConfigDict, Field

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SERVER = HERE / "node_modules" / "@cubejs-backend" / "server" / "bin" / "server"
PLANNERS = ("tesseract", "legacy")
PASS, KNOWN, FIXED, FAIL = "PASS", "KNOWN-BUG", "FIXED", "FAIL"
STATUSES = (PASS, KNOWN, FIXED, FAIL)
SQL_USER = SQL_PASSWORD = "cube"
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


class CubeExpect(BaseModel):
    model_config = ConfigDict(extra="forbid")
    error: Optional[str] = None
    known_bug: Optional[str] = None
    buggy_rows: List[List[Any]] = Field(default_factory=list)
    buggy_row_count: Optional[int] = None
    buggy_error: Optional[str] = None
    sql_lacks: Optional[str] = None


Expect = Literal["match"] | CubeExpect


class CubeBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")
    rest: Optional[Dict[str, Any]] = None
    sql: Optional[str] = None
    planners: List[Literal["tesseract", "legacy"]] = Field(default_factory=lambda: list(PLANNERS))
    model_declared: bool = False
    schema_files: List[str] = Field(default_factory=list, alias="schema")
    keys: Optional[List[str]] = None
    values: Optional[List[str]] = None
    expect: Expect = "match"
    planner_expect: Dict[Literal["tesseract", "legacy"], Expect] = Field(default_factory=dict)
    note: Optional[str] = None


class Probe(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: str
    row: str
    title: str
    truth_sql: Optional[str] = None
    contrast_sql: Optional[str] = None
    compare: Compare = Field(default_factory=Compare)
    cube: Optional[CubeBlock] = None


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
    """All truth/contrast rows up front: Cube's DuckDB driver needs the file to itself."""
    con = duckdb.connect(str(db_path), read_only=True)
    out: Dict[Tuple[str, str], List[tuple]] = {}
    for p in probes:
        for kind in ("truth", "contrast"):
            sql = getattr(p, f"{kind}_sql")
            if sql:
                out[(p.id, kind)] = con.execute(sql).fetchall()
    con.close()
    return out


def canon(v: Any) -> Any:
    """Normalise a cell so Cube (REST strings, SQL API values) and DuckDB spellings of one value compare equal."""
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


def first_line(s: str) -> str:
    return " ".join(s.split())[:200]


class CubeServer:
    """One Cube Core process (plus the Cube Store it spawns in dev mode) over one assembled schema."""

    def __init__(self, workdir: Path, db_path: Path, planner: str, schema_files: List[str], port: int, sql_port: int):
        self.workdir, self.port, self.sql_port = workdir, port, sql_port
        schema = workdir / "model"
        shutil.rmtree(schema, ignore_errors=True)
        schema.mkdir(parents=True)
        for f in sorted((HERE / "model").glob("*.yml")):
            shutil.copy(f, schema / f.name)
        for rel in schema_files:
            shutil.copy(HERE / rel, schema / Path(rel).name)
        # FileRepository joins CUBEJS_SCHEMA_PATH onto the cwd, so it must be relative.
        self.env = {
            **os.environ,
            "CUBEJS_DB_TYPE": "duckdb",
            "CUBEJS_DB_DUCKDB_DATABASE_PATH": str(db_path),
            "CUBEJS_DEV_MODE": "true",
            "CUBEJS_SCHEMA_PATH": "model",
            "CUBEJS_TESSERACT_SQL_PLANNER": "true" if planner == "tesseract" else "false",
            "CUBEJS_PG_SQL_PORT": str(sql_port),
            "CUBEJS_SQL_USER": SQL_USER,
            "CUBEJS_SQL_PASSWORD": SQL_PASSWORD,
            "CUBEJS_TELEMETRY": "false",
            "PORT": str(port),
        }
        self.proc: Optional[subprocess.Popen] = None
        self.log = workdir / f"cube-{planner}.log"

    def __enter__(self) -> "CubeServer":
        for p in (self.port, self.sql_port):
            wait_port(p, free=True)
        with self.log.open("w") as log:
            self.proc = subprocess.Popen(
                ["node", str(SERVER)], cwd=self.workdir, env=self.env, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        try:
            self._wait_ready()
        except BaseException:
            self.__exit__()  # __exit__ does not run when __enter__ raises
            raise
        return self

    def _wait_ready(self) -> None:
        assert self.proc is not None
        deadline = time.time() + 90
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise SystemExit(f"Cube exited early:\n{self.log.read_text()[-2000:]}")
            try:
                with urllib.request.urlopen(f"http://localhost:{self.port}/readyz", timeout=2) as r:
                    if r.status == 200:
                        break
            except OSError:
                pass
            time.sleep(0.5)
        else:
            raise SystemExit(f"Cube not ready after 90s:\n{self.log.read_text()[-2000:]}")
        wait_port(self.sql_port, free=False)

    def __exit__(self, *exc: Any) -> None:
        if self.proc is None or self.proc.poll() is not None:
            return
        os.killpg(self.proc.pid, signal.SIGTERM)
        try:
            self.proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            os.killpg(self.proc.pid, signal.SIGKILL)
            self.proc.wait()
        try:  # Cube Store runs in the same process group; make sure it is gone too.
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def _get(self, path: str, query: Dict[str, Any], extra: str = "") -> Any:
        url = f"http://localhost:{self.port}/cubejs-api/v1/{path}?{extra}query=" + urllib.parse.quote(json.dumps(query))
        deadline = time.time() + 60
        while True:
            try:
                with urllib.request.urlopen(url, timeout=60) as r:
                    body = json.loads(r.read())
            except urllib.error.HTTPError as e:
                try:
                    body = json.loads(e.read())
                except json.JSONDecodeError:
                    return {"error": f"HTTP {e.code} {e.reason}"}
            except (OSError, json.JSONDecodeError) as e:
                return {"error": f"{type(e).__name__}: {e}"}
            if not isinstance(body, dict) or body.get("error") != "Continue wait" or time.time() > deadline:
                return body
            time.sleep(0.2)

    def rest(self, query: Dict[str, Any]) -> Result:
        multi = any("compareDateRange" in td for td in query.get("timeDimensions", []))
        body = self._get("load", query, "queryType=multi&" if multi else "")
        sql_body = self._get("sql", query)
        sql_body = sql_body[0] if isinstance(sql_body, list) else sql_body  # compareDateRange: one per range
        sql = (sql_body.get("sql") or {}).get("sql", [None])[0] if "error" not in sql_body else None
        if "error" in body:
            return Result(error=str(body["error"]), sql=sql)
        data = [r for res in body["results"] for r in res["data"]] if multi else body["data"]
        columns = list(data[0]) if data else annotated_columns(body["results"][0] if multi else body)
        return Result(columns=columns, rows=[[r.get(c) for c in columns] for r in data], sql=sql)

    def sql(self, query: str) -> Result:
        dsn = f"host=localhost port={self.sql_port} user={SQL_USER} password={SQL_PASSWORD} dbname=cube"
        try:
            with psycopg.connect(dsn, autocommit=True) as con:
                cur = con.execute(cast(LiteralString, query))
                columns = [d.name for d in cur.description or []]
                return Result(columns=columns, rows=[list(r) for r in cur.fetchall()], sql=query)
        except psycopg.Error as e:
            return Result(error=f"{type(e).__name__}: {e}", sql=query)


def annotated_columns(body: Dict[str, Any]) -> List[str]:
    """Result column names from a /load response's annotation (what an empty result still reports)."""
    ann = body.get("annotation") or {}
    return [name for kind in ("dimensions", "timeDimensions", "measures") for name in ann.get(kind, {})]


def wait_port(port: int, free: bool, timeout: float = 30) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            in_use = s.connect_ex(("localhost", port)) == 0
        if in_use != free:
            return
        time.sleep(0.3)
    raise SystemExit(f"port {port} still {'busy' if free else 'closed'} after {timeout}s")


def _error_outcome(ex: CubeExpect, res: Result) -> Optional[Outcome]:
    err = res.error or ""
    if ex.error:
        if not res.error:
            return Outcome(status=FAIL, detail=f"expected error {ex.error!r}, got {len(res.rows)} rows")
        if ex.error not in err:
            return Outcome(status=FAIL, detail=f"wrong error: {first_line(err)}")
        return Outcome(status=PASS, detail=f"errors as expected: {first_line(err)}")
    if ex.known_bug and ex.buggy_error:
        if ex.buggy_error in err:
            return Outcome(status=KNOWN, detail=f"{ex.known_bug} ({first_line(err)})")
        if res.error:
            return Outcome(status=FAIL, detail=f"different error: {first_line(err)}")
        return Outcome(status=FIXED, detail=f"no longer fails with {ex.buggy_error!r}; update the expectation")
    if res.error:
        return Outcome(status=FAIL, detail=f"unexpected error: {first_line(err)}")
    return None


def _known_bug_outcome(ex: CubeExpect, got: List[List[Any]], ok: bool, why: str, lacks: bool, tol: float) -> Outcome:
    if ok and not lacks:
        return Outcome(status=FIXED, detail=f"now matches truth ({why}); update the expectation")
    sig = all(has_row(got, b, tol) for b in ex.buggy_rows)
    if ex.buggy_row_count is not None:
        sig = sig and len(got) == ex.buggy_row_count
    if ex.sql_lacks is not None:
        sig = sig and lacks
        why = f"{why}; SQL has no {ex.sql_lacks}"
    if sig:
        return Outcome(status=KNOWN, detail=f"{ex.known_bug} ({why})")
    return Outcome(status=FAIL, detail=f"neither truth nor the known buggy value: {why}")


def evaluate(p: Probe, expect: Expect, res: Result, truth: Dict) -> Outcome:
    block = p.cube
    assert block is not None
    ex = expect if isinstance(expect, CubeExpect) else CubeExpect()
    failed = _error_outcome(ex, res)
    if failed:
        return failed

    keys = block.keys if block.keys is not None else p.compare.keys
    values = block.values if block.values is not None else p.compare.values
    try:
        cols = [resolve(c, res.columns) for c in keys + values]
    except KeyError as e:
        return Outcome(status=FAIL, detail=str(e))
    if p.compare.columns_exact and res.columns != cols:
        return Outcome(status=FAIL, detail=f"columns {res.columns} != {cols}")
    got = [[r[res.columns.index(c)] for c in cols] for r in res.rows]
    exp_rows = [list(r) for r in truth.get((p.id, "truth"), [])]
    ok, why = rows_match(got, exp_rows, p.compare, len(keys)) if p.truth_sql else (True, "no truth_sql")
    lacks = ex.sql_lacks is not None and res.sql is not None and ex.sql_lacks not in res.sql

    if ex.known_bug:
        return _known_bug_outcome(ex, got, ok, why, lacks, p.compare.tolerance)
    if not ok:
        return Outcome(status=FAIL, detail=why)
    if ex.sql_lacks is not None and not lacks:
        return Outcome(status=FAIL, detail=f"{why}, but the SQL contains {ex.sql_lacks!r}")
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


def print_verbose(p: Probe, res: Result, truth: Dict) -> None:
    print(f"    sql: {res.sql}\n    columns: {res.columns}\n    rows: {res.rows}")
    if res.error:
        print(f"    error: {res.error[:2000]}")
    if (p.id, "truth") in truth:
        print(f"    truth: {truth[(p.id, 'truth')]}")


def run_planner(
    planner: str, probes: List[Probe], truth: Dict, tmp: Path, db_path: Path, args: argparse.Namespace
) -> List[Tuple[Probe, Outcome]]:
    """Run one planner's probes, one Cube process per distinct schema (a compile error poisons a whole schema)."""
    groups: Dict[Tuple[str, ...], List[Probe]] = {}
    for p in probes:
        assert p.cube is not None
        if planner in p.cube.planners:
            groups.setdefault(tuple(p.cube.schema_files), []).append(p)
    results: Dict[str, Outcome] = {}
    for schema_files, group in groups.items():
        print(f"--- schema: model/{''.join(' + ' + f for f in schema_files)}")
        with CubeServer(tmp, db_path, planner, list(schema_files), args.port, args.sql_port) as server:
            for p in group:
                results[p.id] = run_probe(server, p, planner, truth, args.verbose)
    return [(p, results[p.id]) for p in probes if p.id in results]


def run_probe(server: CubeServer, p: Probe, planner: str, truth: Dict, verbose: bool) -> Outcome:
    block = p.cube
    assert block is not None
    res = server.rest(block.rest) if block.rest is not None else server.sql(block.sql or "")
    outcome = evaluate(p, block.planner_expect.get(planner, block.expect), res, truth)
    print(f"{outcome.status:<9} {p.id:<24} {p.row:<4} {p.title[:60]:<60}  {outcome.detail}")
    if verbose:
        print_verbose(p, res, truth)
    return outcome


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--row", help="only probes of this matrix row, e.g. Q4")
    ap.add_argument("--id", help="only probes whose id contains this substring")
    ap.add_argument("--planner", choices=[*PLANNERS, "both"], default="both", help="SQL planner (default: both)")
    ap.add_argument("--port", type=int, default=4000, help="REST API port")
    ap.add_argument("--sql-port", type=int, default=15432, help="SQL API (Postgres wire) port")
    ap.add_argument("--verbose", action="store_true", help="show SQL, truth and returned values")
    args = ap.parse_args()
    if not SERVER.exists():
        raise SystemExit(f"{SERVER} missing; run `npm ci` in {HERE}")

    doc = yaml.safe_load((ROOT / "probes.yaml").read_text())
    probes = [Probe.model_validate(p) for p in doc["probes"]]
    probes = [p for p in probes if p.cube and (not args.row or p.row == args.row) and (not args.id or args.id in p.id)]
    declared = {p.id for p in probes if p.cube and p.cube.model_declared}
    planners = PLANNERS if args.planner == "both" else (args.planner,)
    failed = False
    with tempfile.TemporaryDirectory(prefix="cube-probes-") as tmp_name:
        tmp = Path(tmp_name)
        db_path = tmp / "probe.duckdb"
        seed(db_path)
        truth = compute_truth(db_path, probes)
        for planner in planners:
            print(f"=== Cube Core 1.7.46, {planner} planner ===")
            results = run_planner(planner, probes, truth, tmp, db_path, args)
            query_only = [(p, o) for p, o in results if p.id not in declared]
            print_summary(f"Summary (Cube, {planner}, query-only)", query_only)
            print_summary(f"Summary (Cube, {planner}, model-declared)", [(p, o) for p, o in results if p.id in declared])
            failed = failed or any(o.status == FAIL for _, o in results)
            print()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
