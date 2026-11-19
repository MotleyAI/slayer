"""DEV-1836 semantic-equivalence driver.

Seeds RANDOM data (respecting PK/FK/arity so join fan-out is exercised) into
each cross-model golden suite's fixture schema, executes EVERY case query
through the engine against SQLite, and dumps {module::case: {status, rows}} to
JSON. Run in the pre-change worktree AND the current worktree, then diff:

  identical rows        -> PRESERVED (safe to re-bless)
  both raise            -> PRESERVED (guard preserved)
  old raise, new ok     -> NEWLY-WORKS (guard lifted; sanity-check)
  old ok, new raise     -> REGRESSION
  both ok, rows differ  -> VALUE CHANGE (approve or bug)

Executing the SQLite rendering proves the PLAN logic; per-dialect goldens are
syntactic re-renders of the same plan.
"""
from __future__ import annotations

import importlib
import json
import os
import random
from slayer.storage.sqlite_conn import transaction
import sys
import tempfile
import traceback

# argv: <out_json> <db_dir> [code_root]. A code_root (an old worktree) is
# prepended to sys.path so ``slayer`` / ``tests`` resolve to THAT tree. Guarded
# to direct script invocation so importing this module never mutates sys.path.
if sys.argv and sys.argv[0].endswith("_dev1836_equiv_driver.py") and len(sys.argv) > 3 and sys.argv[3]:
    sys.path.insert(0, sys.argv[3])

from slayer.async_utils import run_sync  # ALLOW(import-not-top): must follow the sys.path guard (old-worktree resolution)
from slayer.core.enums import DataType  # ALLOW(import-not-top): must follow the sys.path guard (old-worktree resolution)
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset  # ALLOW(import-not-top): must follow the sys.path guard (old-worktree resolution)
from slayer.engine.query_engine import SlayerQueryEngine  # ALLOW(import-not-top): must follow the sys.path guard (old-worktree resolution)
from slayer.storage.yaml_storage import YAMLStorage  # ALLOW(import-not-top): must follow the sys.path guard (old-worktree resolution)

# module -> (fixtures_module, models_callable_name) ; None name => build inline
MODULES = {
    "tests.test_dev1739_golden_sql": ("tests._dev1739_fixtures", "dev1739_models"),
    "tests.test_dev1747_golden_sql": ("tests._dev1747_fixtures", "dev1747_models"),
    "tests.test_dev1748_golden_sql": ("tests._dev1748_fixtures", "dev1748_models"),
    "tests.test_dev1750_golden_sql": ("tests._dev1750_fixtures", "dev1750_models"),
    "tests.test_dev1824_golden_sql": ("tests._dev1824_fixtures", "dev1824_models"),
    "tests.test_dev1745_golden_sql": (None, None),  # local _orders/_customers/_regions
}

_SQLITE_TYPE = {
    DataType.INTEGER: "INTEGER", DataType.FLOAT: "REAL", DataType.BOOLEAN: "INTEGER",
    DataType.STRING: "TEXT", DataType.DATE: "TEXT", DataType.DATETIME: "TEXT",
    DataType.OPAQUE: "TEXT",
}


def _physical_columns(dataset: SlayerDataset) -> list[DatasetField]:
    """Only bare/identity columns are real table columns; a ``sql`` expression
    (derived/filtered) or a filter-bearing column is computed by the query."""
    out: list[DatasetField] = []
    for c in dataset.fields:
        sql = getattr(c, "sql", None)
        if getattr(c, "filter", None) is not None:
            # a filtered column's PHYSICAL backing (its sql) is a separate col;
            # only include if sql is a bare identifier equal-ish and not already present
            continue
        if sql is None or sql.strip() == c.name:
            out.append(c)
    return out


def _rand_value(dtype, rng: random.Random):
    if dtype == DataType.INTEGER:
        return rng.randint(0, 50)
    if dtype in (DataType.FLOAT,):
        return round(rng.uniform(0, 100), 2)
    if dtype == DataType.BOOLEAN:
        return rng.choice([0, 1])
    if dtype in (DataType.DATE, DataType.DATETIME):
        m = rng.randint(1, 3)
        d = rng.randint(1, 28)
        return f"2024-{m:02d}-{d:02d}" + ("" if dtype == DataType.DATE else " 00:00:00")
    return rng.choice(["a", "b", "c", "web", "app", "ok", "hold", None])


def _topo(datasets: list[SlayerDataset]) -> list[SlayerDataset]:
    by = {m.name: m for m in datasets}
    seen: set[str] = set()
    out: list[SlayerDataset] = []

    def visit(m: SlayerDataset) -> None:
        if m.name in seen:
            return
        seen.add(m.name)
        for j in m.relationships:
            if j.to in by:
                visit(by[j.to])
        out.append(m)

    for m in datasets:
        visit(m)
    return out


def seed_sqlite(datasets: list[SlayerDataset], db_path: str, *, seed: int = 20240501, n_rows: int = 9) -> None:
    rng = random.Random(seed)
    with transaction(db_path) as con:
        pk_values: dict[str, dict[str, list]] = {}
        for m in _topo(datasets):
            if not m.sql_table:
                continue
            cols = _physical_columns(m)
            if not cols:
                continue
            coldefs = ", ".join(f'"{c.name}" {_SQLITE_TYPE.get(c.datatype, "TEXT")}' for c in cols)
            con.execute(f'CREATE TABLE "{m.sql_table}" ({coldefs})')
            colnames = {c.name for c in cols}
            # FK columns: source-side join cols sample from the target's target-col values.
            fk: dict[str, list] = {}
            for j in m.relationships:
                for src_col, tgt_col in j.column_pairs:
                    vals = pk_values.get(j.to, {}).get(tgt_col)
                    if vals and src_col in colnames:
                        fk[src_col] = vals
            rows = []
            for i in range(n_rows):
                row = {}
                for c in cols:
                    if c.name in fk:
                        # repetition -> fan-out; plus an occasional orphan NULL.
                        row[c.name] = rng.choice(fk[c.name] + fk[c.name] + [None])
                    elif c.primary_key or c.unique:
                        row[c.name] = (i + 1) if c.datatype == DataType.INTEGER else f"{c.name[:3]}{i + 1}"
                    else:
                        row[c.name] = _rand_value(c.datatype, rng)
                rows.append(row)
            placeholders = ", ".join("?" for _ in cols)
            con.executemany(
                f'INSERT INTO "{m.sql_table}" VALUES ({placeholders})',
                [[r[c.name] for c in cols] for r in rows],
            )
            pk_values[m.name] = {c.name: [r[c.name] for r in rows] for c in cols}


def _canon_rows(rows):
    out = []
    for r in rows:
        item = {}
        for k, v in r.items():
            if isinstance(v, float):
                v = round(v, 6)
            item[k] = v
        out.append(tuple(sorted(item.items(), key=lambda kv: kv[0])))
    return sorted([str(x) for x in out])


def _build_engine(datasets, db_path, store_dir):
    storage = YAMLStorage(base_dir=store_dir)
    run_sync(storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=db_path)))
    # some fixtures use data_source "test"; force it consistently
    for m in datasets:
        m = m.model_copy(update={"data_source": "test"})
        run_sync(storage.save_dataset(m, _validate=False))
    return SlayerQueryEngine(storage=storage)


def _get_datasets(fixtures_mod, datasets_fn):
    if fixtures_mod is None:
        gm = importlib.import_module("tests.test_dev1745_golden_sql")
        return [gm._orders(), gm._customers(), gm._regions()]
    fm = importlib.import_module(fixtures_mod)
    return list(getattr(fm, datasets_fn)())


def run_module(module_name, fixtures_mod, datasets_fn, tmp):
    mod = importlib.import_module(module_name)
    cases = mod._cases()
    models = _get_datasets(fixtures_mod, datasets_fn)
    db_path = os.path.join(tmp, module_name.split(".")[-1] + ".sqlite")
    if not os.path.exists(db_path):
        seed_sqlite(models, db_path)
    store_dir = tempfile.mkdtemp(dir=tmp)
    engine = _build_engine(models, db_path, store_dir)
    results = {}
    for case_id, query in cases.items():
        key = f"{module_name.split('.')[-1]}::{case_id}"
        try:
            resp = run_sync(engine.execute(query))
            results[key] = {"status": "ok", "rows": _canon_rows(resp.data),
                            "n": len(resp.data)}
        except Exception as exc:  # noqa: BLE001
            results[key] = {"status": "raised", "error": type(exc).__name__}
    return results


def main():
    out_path = sys.argv[1]
    db_dir = sys.argv[2]  # shared seeded-DB dir (identical old/new)
    os.makedirs(db_dir, exist_ok=True)
    all_results = {}
    for module_name, (fixtures_mod, models_fn) in MODULES.items():
        try:
            all_results.update(run_module(module_name, fixtures_mod, models_fn, db_dir))
        except Exception as exc:  # noqa: BLE001
            all_results[f"{module_name}::__MODULE_ERROR__"] = {
                "status": "module_error", "error": repr(exc),
                "tb": traceback.format_exc()[-800:],
            }
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2, sort_keys=True)
    print(f"wrote {out_path}: {len(all_results)} cases")


if __name__ == "__main__":
    main()
