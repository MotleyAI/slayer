"""Version-agnostic corpus executor for the engine A/B audit.

Invoked by compare.py as a subprocess under EITHER interpreter:
the pinned-PyPI baseline venv or the branch poetry venv. Uses only APIs
present in both engine versions (execute with dict/list queries, dry_run,
variables) plus the slayer-free sibling modules corpus.py / classify.py.
"""

import argparse
import asyncio
import json
import sys
import time
import traceback
from pathlib import Path

_DIR = Path(__file__).parent
sys.path.insert(0, str(_DIR))

import corpus  # noqa: E402
from classify import encode_cell  # noqa: E402

MAX_ERROR_LEN = 2000


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--side", required=True, choices=["pypi", "branch"])
    parser.add_argument("--expect-version", default=None,
                        help="require slayer.__version__ to equal this (pypi side)")
    parser.add_argument("--expect-root", default=None,
                        help="require slayer.__file__ under this dir")
    parser.add_argument("--db-type", required=True)
    parser.add_argument("--db", required=True, help="database path or URL")
    parser.add_argument("--out", required=True)
    parser.add_argument("--phase", default="correctness", choices=["correctness", "timing"])
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--entries", default=None,
                        help="comma-separated entry ids to run (default: all)")
    return parser.parse_args()


def _guard(args) -> str:
    import slayer

    version = getattr(slayer, "__version__", "unknown")
    location = str(Path(slayer.__file__).resolve())
    if args.expect_version and version != args.expect_version:
        raise SystemExit(f"guard: slayer {version} != expected {args.expect_version} at {location}")
    if args.expect_root:
        root = str(Path(args.expect_root).resolve())
        if not location.startswith(root + "/"):
            raise SystemExit(f"guard: slayer at {location}, expected under {root}")
    return version


def _select_entries(args) -> list[dict]:
    if not args.entries:
        return corpus.ENTRIES
    wanted = set(args.entries.split(","))
    unknown = wanted - {e["id"] for e in corpus.ENTRIES}
    if unknown:
        raise SystemExit(f"unknown entry ids: {sorted(unknown)}")
    return [e for e in corpus.ENTRIES if e["id"] in wanted]


def _serialize_response(resp) -> dict:
    columns = list(resp.columns)
    rows = []
    for row in resp.data:
        missing = [col for col in columns if col not in row]
        if missing:
            # never fabricate nulls for a declared column: that would let a
            # result-key mapping defect masquerade as a MATCH
            return {"status": "error", "error_type": "ResultShapeError",
                    "error_msg": f"declared columns absent from a data row: {missing[:5]}",
                    "columns": columns, "rows": [], "warnings": []}
        rows.append([encode_cell(row[col]) for col in columns])
    warnings = []
    for warning in getattr(resp, "warnings", []) or []:
        try:
            warnings.append(warning.model_dump(mode="json"))
        except Exception:
            warnings.append({"kind": str(warning)})
    return {"status": "ok", "error_type": None, "error_msg": None,
            "columns": columns, "rows": rows, "warnings": warnings}


def _serialize_error(exc: BaseException) -> dict:
    return {"status": "error", "error_type": type(exc).__name__,
            "error_msg": str(exc)[:MAX_ERROR_LEN],
            "columns": [], "rows": [], "warnings": []}


async def _run_correctness(engine, entries: list[dict]) -> dict:
    results = {}
    for entry in entries:
        record: dict = {"sql": None}
        try:
            dry = await engine.execute(query=entry["query"],
                                       variables=entry.get("variables"), dry_run=True)
            record["sql"] = dry.sql
        except Exception:
            record["sql_error"] = traceback.format_exc(limit=2)[:MAX_ERROR_LEN]
        try:
            resp = await engine.execute(query=entry["query"],
                                        variables=entry.get("variables"))
            record.update(_serialize_response(resp))
        except Exception as exc:
            record.update(_serialize_error(exc))
        results[entry["id"]] = record
    return results


async def _run_timing(engine, entries: list[dict], repeats: int) -> dict:
    timings = {}
    for entry in entries:
        if entry.get("expect_error"):
            continue
        exec_times: list[float] = []
        gen_times: list[float] = []
        try:
            await engine.execute(query=entry["query"], variables=entry.get("variables"))
            for _ in range(repeats):
                start = time.perf_counter()
                await engine.execute(query=entry["query"], variables=entry.get("variables"))
                exec_times.append(time.perf_counter() - start)
            for _ in range(repeats):
                start = time.perf_counter()
                await engine.execute(query=entry["query"],
                                     variables=entry.get("variables"), dry_run=True)
                gen_times.append(time.perf_counter() - start)
        except Exception as exc:
            timings[entry["id"]] = {"error": f"{type(exc).__name__}: {str(exc)[:200]}"}
            continue
        timings[entry["id"]] = {"exec": exec_times, "gen": gen_times}
    return timings


async def _main_async(args, version: str) -> dict:
    import tempfile

    from slayer.core.models import DatasourceConfig, SlayerModel
    from slayer.engine.query_engine import SlayerQueryEngine
    from slayer.storage.yaml_storage import YAMLStorage

    tmpdir = tempfile.mkdtemp(prefix=f"slayer-audit-{args.side}-")
    storage = YAMLStorage(base_dir=tmpdir)
    ds_kwargs = ({"connection_string": args.db} if "://" in args.db
                 else {"database": args.db})
    await storage.save_datasource(DatasourceConfig(
        name=corpus.DS, type=args.db_type, **ds_kwargs,
    ))
    for model_dict in corpus.MODELS:
        await storage.save_model(SlayerModel(**model_dict))

    engine = SlayerQueryEngine(storage=storage)
    await engine.execute(query={"source_model": "orders", "measures": ["*:count"]})

    entries = _select_entries(args)
    payload: dict = {
        "meta": {"side": args.side, "slayer_version": version,
                 "db_type": args.db_type, "db": args.db,
                 "python": sys.version.split()[0], "phase": args.phase,
                 "repeats": args.repeats, "entries": args.entries},
    }
    if args.phase == "correctness":
        payload["results"] = await _run_correctness(engine, entries)
    else:
        payload["timings"] = await _run_timing(engine, entries, args.repeats)

    aclose = getattr(engine, "aclose", None)
    if aclose is not None:
        await aclose()
    return payload


def main() -> None:
    args = _parse_args()
    version = _guard(args)
    payload = asyncio.run(_main_async(args, version))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, allow_nan=False))
    print(f"[runner:{args.side}] {args.phase} done -> {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
