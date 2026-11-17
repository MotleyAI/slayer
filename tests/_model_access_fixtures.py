"""A tagged store for model-access tests: ``pub`` (untagged), ``hr`` ({hr}), ``fin`` ({fin}) and their dependents.

Physical tables (one SQLite or DuckDB file)::

    pub_items  (id, label)
    hr_staff   (id, dept, salary, hired_at)
    fin_ledger (id, staff_id, amount)
    pay_runs   (id, staff_id, gross)
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
import yaml
from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import SlayerQuery
from slayer.embeddings import client as embedding_client
from slayer.embeddings.models import Embedding, EntityKind
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.database import engine_factory
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

DS = "acme"
#: (storage backend, database dialect) pairs; together they cover both of each.
ACCESS_PARAMS = [
    pytest.param(("yaml", "sqlite"), id="yaml-sqlite"),
    pytest.param(("sqlite", "duckdb"), id="sqlitestore-duckdb"),
]

#: Models a ``{fin}`` caller must not see.
HIDDEN_FROM_FIN = frozenset({
    "hr", "hr_summary", "hr_rollup", "fin_hop", "fin_ordered",
    "pay", "pay_col", "pay_colfilter", "pay_filter", "pay_measure", "pay_agg",
})
#: Models a ``{fin}`` caller sees.
VISIBLE_TO_FIN = frozenset({"pub", "fin", "fin_report", "pub_link", "dangling"})
UNTAGGED_PUBLIC = frozenset({"pub", "pub_link", "dangling"})
ALL_LOADABLE = HIDDEN_FROM_FIN | VISIBLE_TO_FIN
UNLOADABLE = "broken"
#: Name of ``pub_link``'s edge to ``hr``.
PRUNED_EDGE = "staffer"

HR_ONLY_MEMORY = "hr-only"
MIXED_MEMORY = "mixed"
QUERY_HOP_MEMORY = "qhop"
MEMORY_REF_MEMORY = "memref"
DATASOURCE_MEMORY = "dsnote"
HELP_MEMORY = "help.access"
FIN_MEMORY = "finnote"
GHOST_MEMORY = "ghostnote"
GHOST_HR_MEMORY = "ghostmix"
HR_LEARNING = "Payroll grades are confidential ZEBRA"

#: Any spelling naming a model or memory hidden from ``{fin}``, or an ``hr`` column (``hr.dept``, ``hr__dept``).
HIDDEN_TOKEN = re.compile(
    r"(?<!\w)(?:hr(?:_\w+)?|qhop|"
    + "|".join(re.escape(n) for n in sorted(HIDDEN_FROM_FIN - {"hr"}, key=len, reverse=True))
    + r")(?!\w)"
)

STAFF = [(1, "eng", 100.0, "2024-01-05"), (2, "ops", 80.0, "2024-02-10"), (3, "eng", 120.0, "2024-03-15")]
LEDGER = [(1, 1, 10.0), (2, 2, 20.0), (3, 3, 30.0)]
PAY_RUNS = [(1, 1, 5.0), (2, 3, 7.0)]
ITEMS = [(1, "a"), (2, "b")]
LEDGER_TOTAL = 60.0


def _seed(db_path: str, *, dialect: str) -> None:
    ddl = (
        "CREATE TABLE pub_items (id INTEGER, label TEXT)",
        "CREATE TABLE hr_staff (id INTEGER, dept TEXT, salary DOUBLE, hired_at DATE)",
        "CREATE TABLE fin_ledger (id INTEGER, staff_id INTEGER, amount DOUBLE)",
        "CREATE TABLE pay_runs (id INTEGER, staff_id INTEGER, gross DOUBLE)",
    )
    rows = (
        ("INSERT INTO pub_items VALUES (?,?)", ITEMS),
        ("INSERT INTO hr_staff VALUES (?,?,?,?)", STAFF),
        ("INSERT INTO fin_ledger VALUES (?,?,?)", LEDGER),
        ("INSERT INTO pay_runs VALUES (?,?,?)", PAY_RUNS),
    )
    if dialect == "duckdb":
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
        try:
            for stmt in ddl:
                con.execute(stmt)
            for stmt, data in rows:
                con.executemany(stmt, data)
        finally:
            con.close()
        return
    with transaction(db_path) as cur:
        for stmt in ddl:
            cur.execute(stmt)
        for stmt, data in rows:
            cur.executemany(stmt, data)


def _staff_join() -> DatasetRelationship:
    return DatasetRelationship(to="hr", join_pairs=[["staff_id", "id"]], cardinality=RelationshipCardinality.MANY_TO_ONE)


def _pay(name: str, **extra) -> SlayerDataset:
    columns = [
        DatasetField(name="id", datatype=DataType.INT, primary_key=True),
        DatasetField(name="staff_id", datatype=DataType.INT),
        DatasetField(name="gross", datatype=DataType.DOUBLE),
        *extra.pop("columns", []),
    ]
    return SlayerDataset(name=name, sql_table="pay_runs", data_source=DS, fields=columns, relationships=[_staff_join()], **extra)


def table_models() -> list[SlayerDataset]:
    """The table-backed models (saved unvalidated, in this order)."""
    hr_dept = DatasetField(name="dept_label", datatype=DataType.TEXT, sql="hr.dept")
    masked = DatasetField(name="gross_eng", datatype=DataType.DOUBLE, sql="gross", filter="hr.dept = 'eng'")
    hr_total = DatasetMetric(name="hr_salary_total", expression="sum(hr.salary)")
    return [
        SlayerDataset(
            name="pub", sql_table="pub_items", data_source=DS, description="public catalogue items",
            fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True), DatasetField(name="label", datatype=DataType.TEXT)],
        ),
        SlayerDataset(
            name="hr", sql_table="hr_staff", data_source=DS, access_tags=["hr"], description="payroll staff roster",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="dept", datatype=DataType.TEXT),
                DatasetField(name="salary", datatype=DataType.DOUBLE),
                DatasetField(name="hired_at", datatype=DataType.DATE),
            ],
        ),
        SlayerDataset(
            name="fin", sql_table="fin_ledger", data_source=DS, access_tags=["fin"], description="finance ledger entries",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="staff_id", datatype=DataType.INT),
                DatasetField(name="amount", datatype=DataType.DOUBLE),
            ],
            relationships=[_staff_join()],
        ),
        SlayerDataset(
            name="pub_link", sql_table="pub_items", data_source=DS,
            fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True), DatasetField(name="label", datatype=DataType.TEXT)],
            relationships=[DatasetRelationship(
                to="hr", name=PRUNED_EDGE, join_pairs=[["id", "id"]], cardinality=RelationshipCardinality.ONE_TO_ONE,
            )],
        ),
        SlayerDataset(
            name="dangling", sql_table="pub_items", data_source=DS,
            fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True)],
            relationships=[DatasetRelationship(to="ghost", join_pairs=[["id", "id"]])],
        ),
        _pay("pay", columns=[hr_dept], measures=[hr_total], filters=["hr.dept IS NOT NULL"]),
        _pay("pay_col", columns=[hr_dept]),
        _pay("pay_colfilter", columns=[masked]),
        _pay("pay_filter", filters=["hr.dept = 'eng'"]),
        _pay("pay_measure", measures=[hr_total]),
        _pay("pay_agg", aggregations=[Aggregation(
            name="weighted", expression="SUM({value} * {weight})", params=[AggregationParam(name="weight", sql="hr.salary")],
        )]),
    ]


def query_backed_models() -> list[SlayerDataset]:
    """The query-backed models (saved through the engine, in dependency order)."""
    def qb(name: str, *stages: dict) -> SlayerDataset:
        return SlayerDataset(name=name, source_queries=[SlayerQuery.model_validate(s) for s in stages])

    return [
        qb("hr_summary", {"source_model": "hr", "dimensions": ["dept"], "measures": [{"formula": "sum(salary)", "name": "total"}]}),
        qb("hr_rollup", {"source_model": "hr_summary", "measures": [{"formula": "sum(total)", "name": "grand"}]}),
        qb("fin_hop", {"source_model": "fin", "dimensions": ["hr.dept"], "measures": [{"formula": "sum(amount)", "name": "amt"}]}),
        qb("fin_ordered", {
            "source_model": "fin", "dimensions": ["id"], "measures": [{"formula": "sum(amount)", "name": "amt"}],
            "order": [{"column": "hr.salary", "direction": "desc"}],
        }),
        qb("fin_report", {"source_model": "fin", "measures": [{"formula": "sum(amount)", "name": "amt"}]}),
    ]


MIXED_QUERY = {"source_model": "fin", "dimensions": ["hr.dept"], "measures": [{"formula": "sum(amount)"}]}
QHOP_QUERY = {"source_model": "pay", "dimensions": ["hr.dept"], "measures": [{"formula": "sum(gross)"}]}


async def _save_memories(storage: StorageBackend) -> None:
    await storage.save_memory(id=HR_ONLY_MEMORY, learning=HR_LEARNING, entities=[f"{DS}.hr", f"{DS}.hr.dept"])
    await storage.save_memory(
        id=MIXED_MEMORY, learning="Ledger by department", entities=[f"{DS}.hr", f"{DS}.fin"],
        query=SlayerQuery.model_validate(MIXED_QUERY),
    )
    await storage.save_memory(id=QUERY_HOP_MEMORY, learning="Pay by department", entities=[],
                              query=SlayerQuery.model_validate(QHOP_QUERY))
    await storage.save_memory(id=MEMORY_REF_MEMORY, learning="See the other note", entities=[f"memory:{HR_ONLY_MEMORY}"])
    await storage.save_memory(id=DATASOURCE_MEMORY, learning="Datasource note", entities=[DS])
    await storage.save_memory(id=HELP_MEMORY, learning="General help", entities=[])
    await storage.save_memory(id=FIN_MEMORY, learning="Ledger note", entities=[f"{DS}.fin"])
    await storage.save_memory(id=GHOST_MEMORY, learning="Retired note", entities=[f"{DS}.ghost"])
    await storage.save_memory(id=GHOST_HR_MEMORY, learning="Half retired note", entities=[f"{DS}.ghost", f"{DS}.hr"])


#: canonical id -> entity kind, one embedding row each.
EMBEDDED: dict[str, EntityKind] = {
    DS: "datasource",
    f"{DS}.pub": "dataset", f"{DS}.pub.label": "field",
    f"{DS}.hr": "dataset", f"{DS}.hr.dept": "field", f"{DS}.hr.salary": "field",
    f"{DS}.hr_summary": "dataset", f"{DS}.pay": "dataset", f"{DS}.pay.hr_salary_total": "metric",
    f"{DS}.fin": "dataset", f"{DS}.fin.amount": "field",
    f"memory:{HR_ONLY_MEMORY}": "memory", f"memory:{MIXED_MEMORY}": "memory",
    f"memory:{QUERY_HOP_MEMORY}": "memory", f"memory:{FIN_MEMORY}": "memory",
}
HIDDEN_EMBEDDINGS_FOR_FIN = frozenset({
    f"{DS}.hr", f"{DS}.hr.dept", f"{DS}.hr.salary", f"{DS}.hr_summary", f"{DS}.pay",
    f"{DS}.pay.hr_salary_total", f"memory:{HR_ONLY_MEMORY}", f"memory:{QUERY_HOP_MEMORY}",
})


async def _save_embeddings(storage: StorageBackend) -> None:
    name = embedding_client.current_model()
    await storage.save_embeddings([
        Embedding(canonical_id=cid, embedding_model_name=name, entity_kind=kind, content_hash=cid, embedding=[1.0, 0.0])
        for cid, kind in EMBEDDED.items()
    ])


async def _write_unloadable(storage: StorageBackend, *, backend: str, base: str) -> None:
    """An untagged stored model whose document fails validation (no source)."""
    doc = {"version": 14, "name": UNLOADABLE, "data_source": DS, "columns": [{"name": "id"}]}
    if backend == "yaml":
        path = os.path.join(base, "models", DS, f"{UNLOADABLE}.yaml")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:  # NOSONAR(S7493) — test seed
            f.write(yaml.safe_dump(doc))
        return
    assert isinstance(storage, SQLiteStorage)
    with transaction(storage.db_path) as conn:
        conn.execute("INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)", (DS, UNLOADABLE, json.dumps(doc)))


class AccessStore(BaseModel):
    """The full (unwrapped) store and where it lives."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    storage: StorageBackend
    backend: str
    dialect: str
    base: str
    db_path: str


def make_storage(*, backend: str, base: str) -> StorageBackend:
    if backend == "yaml":
        return YAMLStorage(base_dir=os.path.join(base, "store"))
    return SQLiteStorage(db_path=os.path.join(base, "store.db"))


@asynccontextmanager
async def access_store(*, backend: str, dialect: str, unloadable: bool = True) -> AsyncGenerator[AccessStore]:
    """The seeded full store; disposes its datasource engine on exit."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "acme.duckdb" if dialect == "duckdb" else "acme.db")
        _seed(db_path, dialect=dialect)
        storage = make_storage(backend=backend, base=tmp)
        ds = DatasourceConfig(name=DS, type=dialect, database=db_path)
        await storage.save_datasource(ds)
        for model in table_models():
            await storage.save_dataset(model, _validate=False)
        engine = SlayerQueryEngine(storage=storage)
        try:
            for model in query_backed_models():
                await engine.save_dataset(model)
            await _save_memories(storage)
            await _save_embeddings(storage)
            if unloadable:
                await _write_unloadable(storage, backend=backend, base=os.path.join(tmp, "store"))
            yield AccessStore(storage=storage, backend=backend, dialect=dialect, base=tmp, db_path=db_path)
        finally:
            engine.close()
            engine_factory.invalidate_engine(ds)


@asynccontextmanager
async def reference_store(*, backend: str, dialect: str) -> AsyncGenerator[AccessStore]:
    """What a ``{fin}`` caller sees, built for real: hidden models deleted and the joins into them dropped."""
    async with access_store(backend=backend, dialect=dialect, unloadable=False) as s:
        for name in sorted(HIDDEN_FROM_FIN):
            await s.storage.delete_dataset(name, data_source=DS)
        for name in ("fin", "pub_link"):
            model = await must_get(s.storage, name)
            await s.storage.save_dataset(model.model_copy(update={"relationships": []}), _validate=False)
        yield s


async def must_get(storage: StorageBackend, name: str, data_source: str = DS) -> SlayerDataset:
    model = await storage.get_dataset(name, data_source=data_source)
    assert model is not None, f"{data_source}.{name} is absent"
    return model


async def mcp_text(storage: StorageBackend, tool: str, **arguments) -> str:
    """Call one MCP ``tool`` on a fresh server over ``storage``; the text reply."""
    server = create_mcp_server(storage=storage, _seed_help=False)
    try:
        blocks, _ = await server.call_tool(name=tool, arguments=arguments)
    finally:
        await getattr(server, "_slayer_engine").aclose()
    return getattr(next(iter(blocks)), "text")


async def outcome(awaitable) -> tuple[str, str]:
    """``("ok", repr(result))``, or the raised error's type name and text."""
    try:
        return "ok", repr(await awaitable)
    except Exception as exc:  # noqa: BLE001 — the outcome is the assertion subject
        return type(exc).__name__, str(exc)


def model_ids(identities) -> set[str]:
    """``{name}`` of the ``DS`` identities in ``identities``."""
    return {name for ds, name in identities if ds == DS}
