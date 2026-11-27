"""Writes the format-freeze corpus: a YAML store (``yaml/``) and a SQLite store (``store.db``) in the stored format.

Run once when the format is declared, from the repository root: ``poetry run python tests/fixtures/format_freeze/build.py``.
The committed output is what later versions must load unchanged; never regenerate it to make a test pass.
"""

from __future__ import annotations

import asyncio
import shutil
import sys
from pathlib import Path

import yaml

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.osi.importer import import_osi
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

HERE = Path(__file__).parent
DS = "shop"


def _f(name: str, sql: str | None = None, *, datatype: str = "String", **extra: object) -> dict[str, object]:
    return {"name": name, "expression": {"sql": sql or name}, "datatype": datatype, **extra}


SALES = {
    "version": "0.3.0.dev0", "name": "sales", "ai_context": "Sales model.",
    "nonportable_extensions": [{"vendor_name": "DBT", "data": {"project": "jaffle"}}],
    "datasets": [
        {"name": "orders", "source": "main.orders", "primary_key": ["order_id", "line"],
         "unique_keys": [["order_id", "line"], ["code", "region"]], "description": "Order lines.",
         "ai_context": {"instructions": "One row per order line.", "synonyms": ["sales orders"]},
         "nonportable_extensions": [{"vendor_name": "SNOWFLAKE", "data": {"cluster_by": ["order_id"]}}],
         "fields": [
             _f("order_id", label="Order", ai_context="The order id.",
                nonportable_extensions=[{"vendor_name": "SNOWFLAKE", "data": {"masking_policy": "pii"}}]),
             _f("line", datatype="Integer"), _f("code"), _f("region", ai_context={"synonyms": ["area"]}),
             _f("period", datatype="Integer", dimension={"is_time": True}),
             _f("ordered_on", datatype="Date", dimension={"is_time": False}),
             _f("shipped_on", datatype="Date"),
             {"name": "amount", "datatype": "Decimal", "expression": {"dialects": [
                 {"dialect": "ANSI_SQL", "expression": "amount"},
                 {"dialect": "BIGQUERY", "expression": "SAFE_CAST(amount AS NUMERIC)"}]}},
             _f("at_time", datatype="Time"), _f("at_tz", datatype="DateTimeTz"),
             {"name": "fx", "datatype": "Float", "expression": {"dialects": [
                 {"dialect": "SNOWFLAKE", "expression": "TRY_TO_DOUBLE(fx)"}]}},
         ],
         "metrics": [{"name": "total", "expression": {"sql": "SUM(amount)"}, "ai_context": "Gross revenue.",
                      "nonportable_extensions": [{"vendor_name": "DBT", "data": {"meta": {"owner": "finance"}}}]}]},
        {"name": "customers", "source": "SELECT code, segment FROM customers", "primary_key": ["code"],
         "fields": [_f("code"), _f("segment")]},
    ],
    "relationships": [{"name": "orders_to_customers", "from": "orders", "to": "customers", "from_columns": ["code"],
                       "to_columns": ["code"], "ai_context": "Each line's buyer.",
                       "nonportable_extensions": [{"vendor_name": "DBT", "data": {"tests": ["relationships"]}}]}],
    "metrics": [{"name": "revenue_per_customer",
                 "expression": {"sql": "SUM(orders.amount) / COUNT(DISTINCT customers.code)"},
                 "ai_context": {"synonyms": ["ARPU"]}}],
}


def _datasource_model_datasets() -> list[SlayerDataset]:
    """Datasets of the datasource model: SLayer-only content in SLAYER entries, and names that are not file names."""
    tagged = SlayerDataset(
        name="tickets", data_source=DS, sql_table="tickets", filters=["status <> 'test'"],
        fields=[DatasetField(name="ticket_id", datatype=DataType.STRING, primary_key=True),
                DatasetField(name="status", datatype=DataType.STRING),
                DatasetField(name="labels", datatype=DataType.ARRAY, element_type=DataType.STRING)],
        metrics=[DatasetMetric(name="ticket_count", expression="COUNT(*)", label="Tickets")])
    backed = SlayerDataset(name="open_tickets", data_source=DS,
                           source_queries=[SlayerQuery.model_validate(
                               {"population": "tickets", "dimensions": ["tickets.status"]})],
                           fields=[DatasetField(name="status", datatype=DataType.STRING)])
    oddly = SlayerDataset(name="tickets/2024 Q1", data_source=DS, sql_table="Tickets2024",
                          fields=[DatasetField(name="ticket_id", datatype=DataType.STRING)])
    returns = SlayerDataset(name="returns", data_source=DS, sql_table="returns",
                            fields=[DatasetField(name="return_id", datatype=DataType.STRING, primary_key=True)])
    return [tagged, backed, oddly, returns]


async def build(storage: StorageBackend, *, scratch: Path) -> None:
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database="corpus.duckdb"))
    source = scratch / "sales.yaml"
    source.write_text(yaml.safe_dump(SALES, sort_keys=False), encoding="utf-8")
    result = await import_osi(storage=storage, path=source, data_source=DS)
    assert result.refused == [], result.refused
    for dataset in _datasource_model_datasets():
        await storage.save_dataset(dataset, _validate=False)
    await storage.create_semantic_model(data_source=DS, name="Support / EMEA", datasets=["returns"])


def main() -> None:
    target = HERE / "yaml"
    database = HERE / "store.db"
    shutil.rmtree(target, ignore_errors=True)
    database.unlink(missing_ok=True)
    scratch = HERE / ".scratch"
    scratch.mkdir(exist_ok=True)
    try:
        asyncio.run(build(YAMLStorage(base_dir=str(target)), scratch=scratch))
        asyncio.run(build(SQLiteStorage(db_path=str(database)), scratch=scratch))
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    for transient in (".locks", ".staging", "embeddings.db", "models", "memories"):
        path = target / transient
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)
    print(f"wrote {target} and {database}", file=sys.stderr)


if __name__ == "__main__":
    main()
