"""Compact builders for the fields, edges and stock datasets tests construct."""

from __future__ import annotations

from typing import Any

from slayer.core.enums import DataType
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset

_LOG_DIALECTS = ("BIGQUERY", "CLICKHOUSE", "DATABRICKS", "DUCKDB", "MYSQL", "ORACLE", "POSTGRES", "PRESTO",
                 "REDSHIFT", "SNOWFLAKE", "SPARK", "SQLITE", "TRINO", "TSQL")
_LOG10_DIALECTS = tuple(d for d in _LOG_DIALECTS if d not in ("ORACLE", "SNOWFLAKE"))


def native(text: str, dialects: tuple[str, ...]) -> ExpressionObject:
    """``text`` as the entry of each of ``dialects``."""
    return ExpressionObject(dialects=[DialectEntry(dialect=d, expression=text) for d in dialects])


def col(name: str, datatype: DataType = DataType.FLOAT, *, expression: str | None = None, **extra: Any) -> DatasetField:
    """A field whose expression defaults to its own name."""
    return DatasetField(name=name, expression=name if expression is None else expression, datatype=datatype, **extra)


def pk(name: str = "id", datatype: DataType = DataType.FLOAT) -> DatasetField:
    return col(name, datatype, primary_key=True)


def to_customers() -> DatasetRelationship:
    """``customer_id`` → ``customers.id``."""
    return DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])


def public_orders() -> SlayerDataset:
    """``public.orders`` with a status, a monthly time axis and two numeric fields."""
    return SlayerDataset(
        name="orders", sql_table="public.orders", data_source="test", default_time_dimension="created_at",
        fields=[pk(), col("status", DataType.STRING), col("created_at", DataType.DATETIME),
                col("revenue", expression="amount"), col("quantity")],
    )


def literal_time_orders() -> SlayerDataset:
    """``public.orders`` whose ``ts`` time axis is a bare literal, beside a plain ``created_at``."""
    return SlayerDataset(
        name="orders", sql_table="public.orders", data_source="test",
        fields=[pk(datatype=DataType.INTEGER), col("ts", DataType.DATETIME, expression="'2025-12-01'"),
                col("created_at", DataType.DATETIME)],
    )


def log_orders() -> SlayerDataset:
    """``public.orders`` with native ``log10``/``log2`` fields, a ``log(3, x)`` control and an ``ln``."""
    return SlayerDataset(
        name="orders", sql_table="public.orders", data_source="test",
        fields=[
            pk(), col("status", DataType.STRING), col("amount"),
            DatasetField(name="log_amount", expression=native("log10(amount)", _LOG_DIALECTS), datatype=DataType.FLOAT),
            DatasetField(name="log2_amount", expression=native("log2(amount)", _LOG_DIALECTS), datatype=DataType.FLOAT),
            DatasetField(name="log3_amount", expression=native("log(3, amount)", _LOG_DIALECTS), datatype=DataType.FLOAT),
            DatasetField(name="ln_amount", expression=native("ln(amount)", _LOG_DIALECTS), datatype=DataType.FLOAT),
        ],
    )


def log_completed_orders(*, filter: str) -> SlayerDataset:
    """``public.orders`` with a native ``log10`` field masked by ``filter``."""
    return SlayerDataset(
        name="orders", sql_table="public.orders", data_source="test",
        fields=[
            pk(), col("status", DataType.STRING), col("amount"),
            DatasetField(name="log_completed_amount", expression=native("log10(amount)", _LOG10_DIALECTS),
                         filter=filter, datatype=DataType.FLOAT),
        ],
    )
