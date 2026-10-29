"""Raw stores whose datasource defines a custom granularity, as the last release (v1.4.1) wrote them.

Physical table (SQLite)::

    events (id, created_at, status, amount)
"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from typing import Any

import yaml

from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._stored_upgrade_fixtures import run

DS = "gds"
OTHER_DS = "other"
BACKENDS = ["yaml", "sqlite"]
#: The versions v1.4.1 wrote.
STORED_VERSIONS = {"SlayerModel": 15, "SlayerQuery": 6, "DatasourceConfig": 2, "Memory": 4}

# (id, created_at, status, amount)
EVENT_ROWS = [
    (1, "2025-01-20 00:00:00", "ok", 1.0),
    (2, "2025-02-10 00:00:00", "new", 2.0),
    (3, "2025-02-20 00:00:00", "ok", 4.0),
    (4, "2025-03-01 00:00:00", "new", 8.0),
]
#: ``sum(amount)`` per mid-month bucket (``granularity_definition``).
EXPECTED_BUCKETS = {"2025-01-15": 3.0, "2025-02-15": 12.0}
SUM = {"formula": "sum(amount)", "name": "s"}


def renamed(name: str) -> str:
    """The name a colliding ``name`` loads as when ``<name>_granularity`` is free."""
    return f"{name}_granularity"


def live_db(base: str) -> str:
    """A seeded ``events`` database inside ``base``."""
    os.makedirs(base, exist_ok=True)
    db_path = os.path.join(base, "live.db")
    seed_events(db_path)
    return db_path


def seed_events(db_path: str) -> None:
    with transaction(db_path) as cur:
        cur.execute("CREATE TABLE events (id INTEGER, created_at TIMESTAMP, status TEXT, amount DOUBLE)")
        cur.executemany("INSERT INTO events VALUES (?,?,?,?)", EVENT_ROWS)


def granularity_definition(name: str) -> dict:
    """Months starting on the 15th."""
    return {"name": name, "base": "month", "origin": "2000-01-15"}


def datasource_doc(*, db_path: str, granularities: Sequence[str] = (), name: str = DS) -> dict:
    doc: dict[str, Any] = {"version": STORED_VERSIONS["DatasourceConfig"], "name": name, "type": "sqlite",
                           "database": db_path}
    if granularities:
        doc["granularities"] = [granularity_definition(g) for g in granularities]
    return doc


def events_doc(*, name: str = "events", data_source: str = DS, column_granularity: str | None = None,
               measures: Sequence[dict] = ()) -> dict:
    created_at: dict[str, Any] = {"name": "created_at", "type": "TIMESTAMP"}
    if column_granularity is not None:
        created_at["granularity"] = column_granularity
    doc: dict[str, Any] = {
        "version": STORED_VERSIONS["SlayerModel"], "name": name, "sql_table": "events", "data_source": data_source,
        "columns": [{"name": "id", "type": "INT", "primary_key": True}, created_at,
                    {"name": "status", "type": "TEXT"}, {"name": "amount", "type": "DOUBLE"}],
    }
    if measures:
        doc["measures"] = list(measures)
    return doc


def stage_doc(*, source_model: Any = "events", **fields: Any) -> dict:
    return {"version": STORED_VERSIONS["SlayerQuery"], "source_model": source_model, **fields}


def bucket_stage(granularity: str) -> dict:
    """``sum(amount)`` by ``created_at`` bucketed by ``granularity``."""
    return stage_doc(time_dimensions=[{"dimension": "created_at", "granularity": granularity}], measures=[SUM])


def query_backed_doc(*, name: str, stage: dict, data_source: str = DS) -> dict:
    return {"version": STORED_VERSIONS["SlayerModel"], "name": name, "data_source": data_source,
            "source_queries": [stage]}


def memory_doc(*, memory_id: str, query: dict) -> dict:
    return {"version": STORED_VERSIONS["Memory"], "id": memory_id, "learning": f"learning {memory_id}",
            "entities": [], "query": query}


async def write_raw_store(
    *, backend: str, base: str, datasources: Sequence[dict], models: Sequence[dict] = (),
    memories: Sequence[dict] = (),
) -> StorageBackend:
    """A YAML or SQLite store whose documents are written verbatim (never validated)."""
    os.makedirs(base, exist_ok=True)
    if backend == "yaml":
        storage: StorageBackend = YAMLStorage(base_dir=base)
        for doc in datasources:
            _write_text(path=os.path.join(base, "datasources", f"{doc['name']}.yaml"), text=yaml.safe_dump(doc))
        for doc in models:
            _write_text(path=os.path.join(base, "models", doc["data_source"], f"{doc['name']}.yaml"),
                        text=yaml.safe_dump(doc, sort_keys=False))
        for mem in memories:
            front = {k: v for k, v in mem.items() if k not in ("id", "learning")}
            _write_text(path=os.path.join(base, "memories", f"{mem['id']}.md"),
                        text=f"---\n{yaml.safe_dump(front)}---\n{mem['learning']}")
        return storage
    db_path = os.path.join(base, "store.db")
    storage = SQLiteStorage(db_path=db_path)
    with transaction(db_path) as conn:
        for doc in datasources:
            conn.execute("INSERT INTO datasources (name, data) VALUES (?, ?)", (doc["name"], json.dumps(doc)))
        for doc in models:
            conn.execute("INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)",
                         (doc["data_source"], doc["name"], json.dumps(doc)))
        for mem in memories:
            conn.execute("INSERT INTO memories (id, data) VALUES (?, ?)", (mem["id"], json.dumps(mem)))
    return storage


def reopen(*, backend: str, base: str) -> StorageBackend:
    """A fresh backend over an existing store (a process restart)."""
    if backend == "yaml":
        return YAMLStorage(base_dir=base)
    return SQLiteStorage(db_path=os.path.join(base, "store.db"))


def raw_documents(*, backend: str, base: str) -> dict[str, str]:
    """Every persisted datasource, model and memory exactly as stored, keyed ``<kind>/<name>``."""
    out: dict[str, str] = {}
    if backend == "yaml":
        for kind, ext in (("datasources", ".yaml"), ("models", ".yaml"), ("memories", ".md")):
            root = os.path.join(base, kind)
            for dirpath, _, files in os.walk(root):
                for f in files:
                    if f.endswith(ext):
                        rel = os.path.relpath(os.path.join(dirpath, f), root)
                        with open(os.path.join(dirpath, f), encoding="utf-8") as fh:  # NOSONAR(S7493) — test read
                            out[f"{kind}/{rel[: -len(ext)]}"] = fh.read()
        return out
    with transaction(os.path.join(base, "store.db")) as conn:
        for name, data in conn.execute("SELECT name, data FROM datasources"):
            out[f"datasources/{name}"] = data
        for ds, name, data in conn.execute("SELECT data_source, name, data FROM models"):
            out[f"models/{ds}/{name}"] = data
        for mem_id, data in conn.execute("SELECT id, data FROM memories"):
            out[f"memories/{mem_id}"] = data
    return out


async def granularity_store(*, backend: str, base: str, name: str) -> StorageBackend:
    """A v1.4.1 store: datasource ``DS`` defining ``name`` (``granularity_definition``), the ``events`` model,
    and the query-backed model ``bucketed`` whose stage buckets ``created_at`` by ``name``."""
    db_path = live_db(base)
    return await write_raw_store(
        backend=backend, base=base,
        datasources=[datasource_doc(db_path=db_path, granularities=[name])],
        models=[events_doc(), query_backed_doc(name="bucketed", stage=bucket_stage(name))],
    )


async def bucket_totals(storage: StorageBackend, *, model: str = "bucketed") -> dict[str, float]:
    """``sum(s)`` per ``created_at`` bucket of the query-backed ``model`` (compare with ``EXPECTED_BUCKETS``)."""
    resp = await run(storage, {"source_model": model, "dimensions": ["created_at"], "measures": ["sum(s)"]})
    return {str(r[f"{model}.created_at"])[:10]: float(r[f"{model}.s_sum"]) for r in resp.data}


def _write_text(*, path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:  # NOSONAR(S7493) — test seed
        f.write(text)
