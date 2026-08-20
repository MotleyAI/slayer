"""Engine A/B audit orchestrator: branch engine vs pinned PyPI release.

Usage (from the repo root, inside the poetry venv):
    poetry run python tests/perf/compare/compare.py                # full audit
    poetry run python tests/perf/compare/compare.py --skip-timing  # correctness only
    poetry run python tests/perf/compare/compare.py --backends sqlite --entries id1,id2

Seeds identical DBs once per (backend, scale), runs runner.py under the
baseline venv (pip-installed motley-slayer==PIN) and under this venv (branch),
classifies per-entry differences with pandas-oracle arbitration, times both
sides in ABBA order, and writes out/report.md + JSON artifacts.
"""

import argparse
import datetime as dt
import json
import platform
import subprocess
import sys
from pathlib import Path

_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_DIR))

import corpus  # noqa: E402
import oracle  # noqa: E402
from classify import (  # noqa: E402
    Verdict, canonical_rows, cells_equal, classify_entry, flag_perf, is_problem,
    pool_abba, warning_drift,
)
from audit_params import (  # noqa: E402
    ADVERSARIAL_DDL, BACKENDS, CORRECTNESS_SCALE, DATA_END_DATE, DATA_START_DATE,
    EXTRA_SCALES, INDEXES, PERF_FLOOR, PERF_RATIO, PYPI_PIN, REPEATS, SCALES,
    SEED, SUBSET_SCALE,
)

ALL_SCALES = {**SCALES, **EXTRA_SCALES}

REPO_ROOT = _DIR.parents[2]
RUNNER = _DIR / "runner.py"
SUBPROCESS_TIMEOUT = 3600


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default=str(_DIR / "out"))
    parser.add_argument("--backends", default=",".join(BACKENDS))
    parser.add_argument("--scales", default=",".join(SCALES))
    parser.add_argument("--pypi-pin", default=PYPI_PIN)
    parser.add_argument("--repeats", type=int, default=REPEATS)
    parser.add_argument("--entries", default=None, help="comma-separated entry id filter")
    parser.add_argument("--skip-timing", action="store_true")
    parser.add_argument("--skip-correctness", action="store_true",
                        help="reuse out/correctness.json from a previous run")
    parser.add_argument("--skip-subset", action="store_true",
                        help="skip the 100k subset timing pass")
    parser.add_argument("--skip-adversarial", action="store_true")
    parser.add_argument("--reseed", action="store_true", help="recreate DB files")
    parser.add_argument("--retime", action="store_true",
                        help="re-run timing invocations whose JSON already exists")
    parser.add_argument("--db-url", default=None,
                        help="external DB escape hatch (URL must contain 'bench'; "
                             "tables are DROPPED and reseeded)")
    parser.add_argument("--db-type", default=None, help="slayer type for --db-url")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Baseline venv
# ---------------------------------------------------------------------------

def ensure_baseline_venv(out_dir: Path, pin: str) -> Path:
    venv_dir = out_dir / "pypi-venv"
    python = venv_dir / "bin" / "python"
    if python.exists():
        probe = subprocess.run(
            [str(python), "-c", "import slayer; print(slayer.__version__)"],
            capture_output=True, text=True, timeout=120,
        )
        if probe.returncode == 0 and probe.stdout.strip() == pin:
            return python
    print(f"[setup] creating baseline venv (motley-slayer=={pin}) ...")
    # virtualenv, not stdlib venv: it bundles pip and needs no ensurepip
    create = subprocess.run(
        [sys.executable, "-m", "virtualenv", "--clear", str(venv_dir)],
        capture_output=True, text=True, timeout=300,
    )
    if create.returncode != 0:
        raise SystemExit(f"virtualenv creation failed:\n{create.stderr[-3000:]}")
    install = subprocess.run(
        [str(python), "-m", "pip", "install", "--quiet", f"motley-slayer=={pin}"],
        capture_output=True, text=True, timeout=900,
    )
    if install.returncode != 0:
        raise SystemExit(f"baseline pip install failed:\n{install.stderr[-3000:]}")
    return python


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def _db_url(backend: str, path: Path) -> str:
    return f"{backend}:///{path}"


def _apply_indexes(engine) -> None:
    import sqlalchemy as sa

    with engine.connect() as conn:
        for idx_sql in INDEXES:
            try:
                conn.execute(sa.text(idx_sql))
            except Exception as exc:
                print(f"[seed] index skipped ({engine.dialect.name}): {exc}")
        conn.commit()


def seed_generated(backend: str, path: Path, order_count: int, seed_mod) -> None:
    import sqlalchemy as sa

    dataset = seed_mod.generate_dataset(
        order_count=order_count, start_date=DATA_START_DATE,
        end_date=DATA_END_DATE, seed=SEED,
    )
    engine = sa.create_engine(_db_url(backend, path))
    seed_mod.seed_database(engine=engine, dataset=dataset)
    _apply_indexes(engine)
    engine.dispose()


def seed_adversarial(backend: str, path: Path) -> None:
    import sqlalchemy as sa

    engine = sa.create_engine(_db_url(backend, path))
    with engine.connect() as conn:
        for stmt in ADVERSARIAL_DDL.strip().split(";"):
            if stmt.strip():
                conn.execute(sa.text(stmt))
        for table, rows in corpus.ADVERSARIAL_TABLES.items():
            cols = list(rows[0])
            insert = sa.text(
                f"INSERT INTO {table} ({', '.join(cols)}) "
                f"VALUES ({', '.join(':' + c for c in cols)})"
            )
            conn.execute(insert, rows)
        conn.commit()
    engine.dispose()


# ---------------------------------------------------------------------------
# Runner invocation
# ---------------------------------------------------------------------------

def run_runner(python: Path, side: str, *, db_type: str, db: str, out: Path,
               phase: str, repeats: int, entries: str | None,
               pin: str | None) -> None:
    cmd = [str(python), str(RUNNER), "--side", side, "--db-type", db_type,
           "--db", db, "--out", str(out), "--phase", phase,
           "--repeats", str(repeats)]
    if side == "pypi":
        cmd += ["--expect-version", pin or PYPI_PIN]
    else:
        cmd += ["--expect-root", str(REPO_ROOT)]
    if entries:
        cmd += ["--entries", entries]
    proc = subprocess.run(cmd, capture_output=True, text=True,
                          timeout=SUBPROCESS_TIMEOUT, cwd=str(REPO_ROOT))
    if proc.returncode != 0:
        raise SystemExit(
            f"runner failed ({side}, {phase}, {db_type}):\n"
            f"stdout: {proc.stdout[-2000:]}\nstderr: {proc.stderr[-4000:]}"
        )


def _load(path: Path) -> dict:
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Oracle arbitration
# ---------------------------------------------------------------------------

def rows_vs_oracle(result: dict, expected_rows: list) -> str | None:
    """'match' / 'mismatch' / 'shape' vs the oracle, or None if inapplicable."""
    if result["status"] != "ok":
        return None
    rows = canonical_rows(result["rows"], ordered=False)
    expected = canonical_rows(expected_rows, ordered=False)
    if rows and expected and len(rows[0]) != len(expected[0]):
        return "shape"
    if len(rows) != len(expected):
        return "mismatch"
    for row_a, row_b in zip(rows, expected):
        for cell_a, cell_b in zip(row_a, row_b):
            if not cells_equal(cell_a, cell_b)[0]:
                return "mismatch"
    return "match"


# ---------------------------------------------------------------------------
# Audit passes
# ---------------------------------------------------------------------------

def _entries_arg(args) -> str | None:
    return args.entries


def _selected_entries(args) -> list[dict]:
    if not args.entries:
        return corpus.ENTRIES
    wanted = set(args.entries.split(","))
    return [e for e in corpus.ENTRIES if e["id"] in wanted]


def correctness_pass(args, out_dir, pypi_python, backend, dataset_label, db_path,
                     frames) -> list[dict]:
    """Run both sides + classify. Returns one record per entry."""
    results = {}
    for side, python in (("pypi", pypi_python), ("branch", Path(sys.executable))):
        out = out_dir / f"results-{side}-{backend}-{dataset_label}.json"
        run_runner(python, side, db_type=backend, db=str(db_path), out=out,
                   phase="correctness", repeats=args.repeats,
                   entries=_entries_arg(args), pin=args.pypi_pin)
        results[side] = _load(out)["results"]

    selected_ids = {e["id"] for e in _selected_entries(args)}
    for side, side_results in results.items():
        missing = selected_ids - set(side_results)
        if missing:
            raise SystemExit(
                f"correctness coverage hole: {side}/{backend}/{dataset_label} "
                f"produced no record for {sorted(missing)}"
            )

    records = []
    for entry in _selected_entries(args):
        pypi_result = results["pypi"][entry["id"]]
        branch_result = results["branch"][entry["id"]]
        verdict = classify_entry(entry, pypi_result, branch_result)
        record = {
            "entry": entry["id"], "family": entry["family"],
            "backend": backend, "dataset": dataset_label,
            "verdict": verdict.model_dump(),
            "warning_drift": warning_drift(pypi_result.get("warnings", []),
                                           branch_result.get("warnings", [])),
            "sql_differs": (pypi_result.get("sql") or "") != (branch_result.get("sql") or ""),
        }
        spec = entry.get("oracle")
        if spec is not None and frames is not None:
            try:
                expected_rows = oracle.expected(spec, frames)
                record["oracle_pypi"] = rows_vs_oracle(pypi_result, expected_rows)
                record["oracle_branch"] = rows_vs_oracle(branch_result, expected_rows)
            except Exception as exc:
                record["oracle_error"] = f"{type(exc).__name__}: {exc}"
        records.append(record)
    return records


def timing_pass(args, out_dir, pypi_python, backend, scale_label, db_path,
                entry_filter: str | None) -> dict:
    """ABBA order: pypi, branch, branch, pypi. Returns pooled per-side timings."""
    order = [("pypi", pypi_python, 1), ("branch", Path(sys.executable), 1),
             ("branch", Path(sys.executable), 2), ("pypi", pypi_python, 2)]
    runs: dict[str, list[dict]] = {"pypi": [], "branch": []}
    for side, python, run_no in order:
        out = out_dir / f"timings-{side}{run_no}-{backend}-{scale_label}.json"
        if args.retime or not out.exists():
            run_runner(python, side, db_type=backend, db=str(db_path), out=out,
                       phase="timing", repeats=args.repeats,
                       entries=entry_filter, pin=args.pypi_pin)
        payload = _load(out)
        meta = payload.get("meta", {})
        stale = (meta.get("repeats") != args.repeats
                 or meta.get("entries") != entry_filter
                 or (side == "pypi" and meta.get("slayer_version") != args.pypi_pin))
        if stale:
            raise SystemExit(
                f"stale timing artifact {out.name} (meta {meta.get('repeats')=} "
                f"{meta.get('entries')=} {meta.get('slayer_version')=}); "
                f"re-run with --retime"
            )
        runs[side].append(payload["timings"])
    return {side: pool_abba(runs[side][0], runs[side][1]) for side in runs}


def perf_flags(pooled: dict, backend: str, scale_label: str, repeats: int) -> list[dict]:
    flags = []
    shared = set(pooled["pypi"]) & set(pooled["branch"])
    one_sided = set(pooled["pypi"]) ^ set(pooled["branch"])
    for entry_id in sorted(one_sided):
        flags.append({"entry": entry_id, "backend": backend, "scale": scale_label,
                      "metric": "n/a", "flagged": False,
                      "error": "timed on only one side"})
    for entry_id in sorted(shared):
        pypi_metrics, branch_metrics = pooled["pypi"][entry_id], pooled["branch"][entry_id]
        if "error" in pypi_metrics or "error" in branch_metrics:
            flags.append({"entry": entry_id, "backend": backend, "scale": scale_label,
                          "metric": "n/a", "flagged": False,
                          "error": pypi_metrics.get("error") or branch_metrics.get("error")})
            continue
        for metric in ("exec", "gen"):
            try:
                flag = flag_perf(entry_id, metric,
                                 pypi_times=pypi_metrics[metric],
                                 branch_times=branch_metrics[metric],
                                 ratio_threshold=PERF_RATIO, floor_seconds=PERF_FLOOR,
                                 expected_samples=2 * repeats)
            except ValueError as exc:
                flags.append({"entry": entry_id, "backend": backend,
                              "scale": scale_label, "metric": metric,
                              "flagged": False, "error": str(exc)})
                continue
            flags.append({"entry": entry_id, "backend": backend, "scale": scale_label,
                          **flag.model_dump()})
    return flags


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

INTERESTING = [
    "BOTH_ERROR_UNEXPECTED", "VALUE_MISMATCH", "SHAPE_MISMATCH", "ORDER_MISMATCH",
    "BRANCH_ONLY_ERROR", "PYPI_ONLY_ERROR", "NAME_DRIFT", "BOTH_ERROR", "MATCH",
]


def write_report(out_dir: Path, manifest: dict, correctness: list[dict],
                 flags: list[dict], filename: str = "report.md") -> str:
    lines = ["# Engine A/B audit: branch vs PyPI " + manifest["pypi_pin"], ""]
    lines.append(f"Generated {manifest['timestamp']} on {manifest['platform']}; "
                 f"branch commit {manifest['head'][:10]}"
                 + (" (dirty)" if manifest["dirty"] else "") + ".")
    lines.append("")

    lines.append("## Correctness summary")
    lines.append("")
    lines.append("| backend | dataset | " + " | ".join(INTERESTING) + " |")
    lines.append("|---" * (len(INTERESTING) + 2) + "|")
    combos = sorted({(r["backend"], r["dataset"]) for r in correctness})
    for backend, dataset in combos:
        subset = [r for r in correctness if r["backend"] == backend and r["dataset"] == dataset]
        counts = {status: 0 for status in INTERESTING}
        for r in subset:
            counts[r["verdict"]["status"]] = counts.get(r["verdict"]["status"], 0) + 1
        lines.append(f"| {backend} | {dataset} | "
                     + " | ".join(str(counts.get(s, 0)) for s in INTERESTING) + " |")
    lines.append("")

    problems = [r for r in correctness if is_problem(Verdict(**r["verdict"]))]
    oracle_disagreements = [
        r for r in correctness
        if r.get("oracle_pypi") == "match" and r.get("oracle_branch") == "mismatch"
        or r.get("oracle_pypi") == "mismatch" and r.get("oracle_branch") == "match"
    ]

    lines.append("## Non-matching entries")
    lines.append("")
    if not problems:
        lines.append("None.")
    for r in sorted(problems, key=lambda r: (r["verdict"]["status"], r["entry"])):
        verdict = r["verdict"]
        oracle_note = ""
        if "oracle_pypi" in r or "oracle_branch" in r:
            oracle_note = f" | oracle: pypi={r.get('oracle_pypi')} branch={r.get('oracle_branch')}"
        match_note = (f" | expect_error_match FAILED on: {verdict['error_match_failed']}"
                      if verdict.get("error_match_failed") else "")
        lines.append(f"- **{verdict['status']}** `{r['entry']}` ({r['backend']}/{r['dataset']})"
                     f"{oracle_note}{match_note}")
        if verdict["detail"]:
            lines.append(f"  - {verdict['detail'][:400]}")
    lines.append("")

    oracle_errors = [r for r in correctness if r.get("oracle_error")]
    lines.append("## Oracle failures (entries left un-arbitrated)")
    lines.append("")
    if not oracle_errors:
        lines.append("None.")
    for r in oracle_errors:
        lines.append(f"- `{r['entry']}` ({r['backend']}/{r['dataset']}): {r['oracle_error']}")
    lines.append("")

    lines.append("## Oracle disagreements (one side right, one side wrong)")
    lines.append("")
    if not oracle_disagreements:
        lines.append("None.")
    for r in oracle_disagreements:
        wrong = "branch" if r.get("oracle_branch") == "mismatch" else "pypi"
        lines.append(f"- `{r['entry']}` ({r['backend']}/{r['dataset']}): **{wrong} side is wrong**"
                     f" (engine-vs-engine verdict: {r['verdict']['status']})")
    lines.append("")

    both_wrong = [r for r in correctness
                  if r.get("oracle_pypi") not in (None, "match")
                  and r.get("oracle_branch") not in (None, "match")]
    lines.append("## Oracle mismatches on BOTH sides (oracle spec or both engines suspect)")
    lines.append("")
    if not both_wrong:
        lines.append("None.")
    for r in both_wrong:
        lines.append(f"- `{r['entry']}` ({r['backend']}/{r['dataset']}): "
                     f"pypi={r.get('oracle_pypi')} branch={r.get('oracle_branch')}"
                     f" (engine-vs-engine: {r['verdict']['status']})")
    lines.append("")

    warn_drifts = [r for r in correctness if r.get("warning_drift")]
    lines.append("## Warning drift (informational)")
    lines.append("")
    if not warn_drifts:
        lines.append("None.")
    for r in warn_drifts:
        lines.append(f"- `{r['entry']}` ({r['backend']}/{r['dataset']}): {r['warning_drift']}")
    lines.append("")

    lines.append("## Performance flags")
    lines.append("")
    flagged = [f for f in flags if f.get("flagged")]
    if not flagged:
        lines.append("No entries exceeded the regression thresholds "
                     f"(> {PERF_RATIO}x AND > {int(PERF_FLOOR * 1000)}ms, pooled ABBA medians).")
    else:
        lines.append("| backend | scale | entry | metric | pypi median | branch median | ratio |")
        lines.append("|---|---|---|---|---|---|---|")
        for f in sorted(flagged, key=lambda f: -f["ratio"]):
            lines.append(f"| {f['backend']} | {f['scale']} | {f['entry']} | {f['metric']} "
                         f"| {f['pypi_median'] * 1000:.1f}ms | {f['branch_median'] * 1000:.1f}ms "
                         f"| {f['ratio']:.2f}x |")
    lines.append("")

    report = "\n".join(lines)
    (out_dir / filename).write_text(report)
    return report


def write_timings_csv(out_dir: Path, flags: list[dict]) -> None:
    rows = ["backend,scale,entry,metric,pypi_median_s,branch_median_s,ratio,delta_s,flagged"]
    for f in flags:
        if f.get("metric") == "n/a":
            continue
        rows.append(f"{f['backend']},{f['scale']},{f['entry']},{f['metric']},"
                    f"{f['pypi_median']:.6f},{f['branch_median']:.6f},"
                    f"{f['ratio']:.4f},{f['delta']:.6f},{f['flagged']}")
    (out_dir / "timings.csv").write_text("\n".join(rows) + "\n")


def build_manifest(args) -> dict:
    def _git(*cmd):
        return subprocess.run(["git", *cmd], capture_output=True, text=True,
                              cwd=str(REPO_ROOT)).stdout.strip()

    import importlib.metadata as md

    versions = {}
    for pkg in ("motley-slayer", "duckdb", "pandas", "sqlalchemy", "pydantic"):
        try:
            versions[pkg] = md.version(pkg)
        except md.PackageNotFoundError:
            versions[pkg] = "absent"
    return {
        "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
        "head": _git("rev-parse", "HEAD"),
        "dirty": bool(_git("status", "--porcelain")),
        "pypi_pin": args.pypi_pin,
        "branch_versions": versions,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "params": {"scales": args.scales, "backends": args.backends,
                   "repeats": args.repeats, "perf_ratio": PERF_RATIO,
                   "perf_floor_s": PERF_FLOOR, "seed": SEED,
                   "data_range": [DATA_START_DATE, DATA_END_DATE]},
        "run_order": "correctness: pypi,branch; timing: ABBA (pypi,branch,branch,pypi)",
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _seed_external(args, seed_mod, count: int) -> None:
    """Drop + reseed the external DB at ``count`` orders; COPY on postgres."""
    import sqlalchemy as sa

    dataset = seed_mod.generate_dataset(
        order_count=count, start_date=DATA_START_DATE,
        end_date=DATA_END_DATE, seed=SEED,
    )
    print(f"[seed] external {args.db_type} ({count} orders, clean=True) ...")
    engine = sa.create_engine(args.db_url)
    if args.db_type in ("postgres", "postgresql"):
        _seed_postgres_copy(args.db_url, seed_mod, dataset)
    else:
        seed_mod.seed_database(engine=engine, dataset=dataset, clean=True)
    _apply_indexes(engine)
    with engine.connect() as conn:
        seeded = conn.execute(sa.text("SELECT COUNT(*) FROM orders")).scalar()
    engine.dispose()
    if seeded != count:
        raise SystemExit(
            f"external seed check failed: orders has {seeded} rows, expected {count}")


def _seed_postgres_copy(url: str, seed_mod, dataset) -> None:
    """COPY-based bulk load via psycopg2 directly — executemany is far too
    slow at 1M+ rows, and mixing raw-driver COPY with a SQLAlchemy-managed
    transaction loses the data (the driver-level txn rolls back on close)."""
    import io

    import psycopg2

    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cursor:
            for block in (seed_mod._DROP_TABLES_SQL, seed_mod._CREATE_TABLES_SQL):
                for stmt in block.strip().split(";"):
                    if stmt.strip():
                        cursor.execute(stmt)

            def _copy(table: str, columns: list[str], rows) -> None:
                buffer = io.StringIO()
                for row in rows:
                    buffer.write("\t".join(
                        r"\N" if value is None else str(value) for value in row) + "\n")
                buffer.seek(0)
                cursor.copy_expert(
                    f"COPY {table} ({', '.join(columns)}) FROM STDIN", buffer)

            _copy("regions", ["id", "name"],
                  ((r.id, r.name) for r in dataset.regions))
            _copy("shops", ["id", "name", "region_id", "avg_cost", "avg_frequency", "size"],
                  ((s.id, s.name, s.region_id, s.avg_cost, s.avg_frequency, s.size)
                   for s in dataset.shops))
            _copy("customers", ["id", "name", "segment", "primary_shop_id"],
                  ((c.id, c.name, c.segment, c.shop_ids[0]) for c in dataset.customers))
            _copy("orders", ["id", "customer_id", "shop_id", "category", "cost",
                             "created_at", "completed_at", "cancelled_at"],
                  ((o.id, o.customer_id, o.shop_id, o.category, o.cost,
                    o.created_at, o.completed_at, o.cancelled_at)
                   for o in dataset.orders))
        conn.commit()
    finally:
        conn.close()


def _run_external_db(args, out_dir: Path, pypi_python: Path, seed_mod) -> None:
    """--db-url escape hatch: correctness + timing against one external DB.

    Destructively reseeds (drop + recreate) per pass: once at the correctness
    scale, then once per requested --scales entry for ABBA timing. The
    adversarial dataset stays local-file-only.
    """
    correctness: list[dict] = []
    if not args.skip_correctness:
        count = SCALES[CORRECTNESS_SCALE]
        _seed_external(args, seed_mod, count)
        dataset = seed_mod.generate_dataset(
            order_count=count, start_date=DATA_START_DATE,
            end_date=DATA_END_DATE, seed=SEED,
        )
        frames = oracle.frames_from_dataset(dataset)
        correctness = correctness_pass(
            args, out_dir, pypi_python, args.db_type, f"gen-{CORRECTNESS_SCALE}",
            args.db_url, frames)
        (out_dir / "correctness-external.json").write_text(
            json.dumps(correctness, indent=2))

    flags: list[dict] = []
    if not args.skip_timing:
        scales = {name: ALL_SCALES[name] for name in args.scales.split(",") if name}
        for scale_name, count in scales.items():
            _seed_external(args, seed_mod, count)
            print(f"[timing] external {args.db_type} {scale_name} (ABBA x{args.repeats}) ...")
            pooled = timing_pass(args, out_dir, pypi_python, args.db_type,
                                 scale_name, args.db_url, _entries_arg(args))
            flags += perf_flags(pooled, args.db_type, scale_name, repeats=args.repeats)
        (out_dir / "perf-flags-external.json").write_text(json.dumps(flags, indent=2))

    manifest = build_manifest(args)
    (out_dir / "run-manifest-external.json").write_text(json.dumps(manifest, indent=2))
    print(write_report(out_dir, manifest, correctness, flags,
                       filename="report-external.md"))


def main() -> None:
    args = _parse_args()
    if args.db_url:
        from urllib.parse import urlsplit

        if not args.db_type:
            raise SystemExit("--db-url requires --db-type")
        if "bench" not in urlsplit(args.db_url).path.lower():
            raise SystemExit(
                "--db-url database name must contain 'bench' (destructive reseed safety)")

    out_dir = Path(args.out_dir)
    (out_dir / "db").mkdir(parents=True, exist_ok=True)
    pypi_python = ensure_baseline_venv(out_dir, args.pypi_pin)
    seed_mod = oracle.load_seed_module()
    if args.db_url:
        _run_external_db(args, out_dir, pypi_python, seed_mod)
        return
    backends = args.backends.split(",")
    scales = {name: ALL_SCALES[name] for name in args.scales.split(",") if name}

    manifest = build_manifest(args)
    (out_dir / "run-manifest.json").write_text(json.dumps(manifest, indent=2))

    ext = {"sqlite": "db", "duckdb": "duckdb"}
    db_paths: dict[tuple[str, str], Path] = {}
    all_scales = dict(scales)
    if not args.skip_timing and not args.skip_subset:
        all_scales[SUBSET_SCALE[0]] = SUBSET_SCALE[1]
    for backend in backends:
        for scale_name, count in all_scales.items():
            path = out_dir / "db" / f"gen-{backend}-{scale_name}.{ext[backend]}"
            db_paths[(backend, scale_name)] = path
            if args.reseed and path.exists():
                path.unlink()
            if not path.exists():
                print(f"[seed] {backend} {scale_name} ({count} orders) ...")
                seed_generated(backend, path, count, seed_mod)
        adv_path = out_dir / "db" / f"adv-{backend}.{ext[backend]}"
        db_paths[(backend, "adv")] = adv_path
        if args.reseed and adv_path.exists():
            adv_path.unlink()
        if not adv_path.exists():
            print(f"[seed] {backend} adversarial ...")
            seed_adversarial(backend, adv_path)

    gen_dataset = seed_mod.generate_dataset(
        order_count=scales.get(CORRECTNESS_SCALE, SCALES[CORRECTNESS_SCALE]),
        start_date=DATA_START_DATE, end_date=DATA_END_DATE, seed=SEED,
    )
    gen_frames = oracle.frames_from_dataset(gen_dataset)
    adv_frames = oracle.frames_from_tables(corpus.ADVERSARIAL_TABLES)

    if args.skip_correctness:
        correctness = json.loads((out_dir / "correctness.json").read_text())
    else:
        correctness = []
        for backend in backends:
            print(f"[correctness] {backend} generated@{CORRECTNESS_SCALE} ...")
            correctness += correctness_pass(
                args, out_dir, pypi_python, backend, f"gen-{CORRECTNESS_SCALE}",
                db_paths[(backend, CORRECTNESS_SCALE)], gen_frames)
            if not args.skip_adversarial:
                print(f"[correctness] {backend} adversarial ...")
                correctness += correctness_pass(
                    args, out_dir, pypi_python, backend, "adv",
                    db_paths[(backend, "adv")], adv_frames)
        (out_dir / "correctness.json").write_text(json.dumps(correctness, indent=2))

    flags: list[dict] = []
    if not args.skip_timing:
        subset_ids = ",".join(e["id"] for e in corpus.ENTRIES if e.get("subset_100k"))
        timing_jobs = [(backend, name, _entries_arg(args)) for backend in backends
                       for name in scales]
        if not args.skip_subset:
            timing_jobs += [(backend, SUBSET_SCALE[0], subset_ids) for backend in backends]
        for backend, scale_name, entry_filter in timing_jobs:
            print(f"[timing] {backend} {scale_name} (ABBA x{args.repeats}) ...")
            pooled = timing_pass(args, out_dir, pypi_python, backend, scale_name,
                                 db_paths[(backend, scale_name)], entry_filter)
            flags += perf_flags(pooled, backend, scale_name, repeats=args.repeats)
        write_timings_csv(out_dir, flags)
    (out_dir / "perf-flags.json").write_text(json.dumps(flags, indent=2))

    report = write_report(out_dir, manifest, correctness, flags)
    print("\n" + "=" * 70)
    print(report)
    print(f"\nArtifacts in {out_dir}")


if __name__ == "__main__":
    main()
