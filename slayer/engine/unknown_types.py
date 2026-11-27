"""Unknown field types resolved from the live database before a query binds; never persisted."""

from __future__ import annotations

import logging
from collections.abc import Callable

from sqlglot import exp

from slayer.core.enums import DataType, finer_datatype
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.database.client import SqlClient, render_type_probe
from slayer.database.sqlite_introspect import ingested_sqlite_type
from slayer.sql.column_sources import ColumnSource, column_sources
from slayer.sql.dialects import SqlDialect, dialect_for_ds_type
from slayer.sql.dialects.base import ascii_lower
from slayer.sql.dialects.reflection import ReflectedColumn, TableLocation
from slayer.sql.render.parse import parse_table
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)

#: ``(storage fingerprint, datasource, dataset)``.
_Key = tuple[str, str, str]


def _probeable(field: DatasetField) -> bool:
    return field.datatype is None and field.definition_known and field.reads_own_source


class UnknownTypes:
    """One engine's resolved unknown types, cached per storage fingerprint."""

    def __init__(self) -> None:
        self._cache: dict[_Key, dict[str, DatasetField]] = {}

    async def resolve(
        self, dataset: SlayerDataset, *, storage: StorageBackend, client_for: Callable[[DatasourceConfig], SqlClient],
    ) -> SlayerDataset:
        """``dataset`` with each untyped field over its own source typed by its live type, where the probe finds one."""
        untyped = [f for f in dataset.fields if _probeable(f)]
        if not untyped or dataset.source_queries or not (dataset.sql_table or dataset.sql):
            return dataset
        fingerprint = await storage.graph_fingerprint()
        key = (fingerprint or "", dataset.data_source, dataset.name)
        typed = self._cache.get(key) if fingerprint is not None else None
        if typed is None:
            typed = await _probe(dataset, untyped, storage=storage, client_for=client_for)
            if fingerprint is not None:
                self._cache[key] = typed
        if not typed:
            return dataset
        return dataset.model_copy(update={"fields": [typed.get(f.name, f) for f in dataset.fields]})


async def _probe(
    dataset: SlayerDataset, fields: list[DatasetField], *, storage: StorageBackend,
    client_for: Callable[[DatasourceConfig], SqlClient],
) -> dict[str, DatasetField]:
    """Each probed field typed by its expression's result type over ``dataset``'s source: a zero-row query where the
    cursor types results, else a census over sampled rows; on SQLite a plain column read as ingestion types it."""
    datasource = await storage.get_datasource(dataset.data_source)
    if datasource is None:
        return {}
    dialect = dialect_for_ds_type(datasource.type)
    name = dialect.sqlglot_name
    if dataset.sql_table:
        source: exp.Expr = parse_table(name=dataset.sql_table, dialect=dialect)
    else:
        source = exp.maybe_parse(dataset.sql or "", dialect=name).subquery("_src")
    select = exp.select(*(
        exp.alias_(exp.maybe_parse(f.definition_sql or f.name, dialect=name), f.name, quoted=True) for f in fields
    )).from_(source)
    if dialect.result_census_rows is None:
        select = select.where(exp.EQ(this=exp.Literal.number(1), expression=exp.Literal.number(0)))
    client = client_for(datasource)
    try:
        found = await client.get_result_column_types(render_type_probe(select, dialect=dialect))
    except Exception as exc:  # noqa: BLE001 — an unprobeable field stays unknown; the query refuses it where it binds
        logger.info("Type probe of %s.%s failed: %s", dataset.data_source, dataset.name, exc)
        return {}
    try:
        ingested = await _ingested_types(select, client=client, dialect=dialect)
    except Exception as exc:  # noqa: BLE001 — the refinement is optional; the probed result types stand
        logger.info("Ingestion typing of %s.%s failed: %s", dataset.data_source, dataset.name, exc)
        ingested = {}
    columns = {column.casefold(): result for column, result in found.items()}
    out: dict[str, DatasetField] = {}
    for f in fields:
        column = ingested.get(f.name.casefold())
        if column is not None:
            out[f.name] = f.model_copy(update={"datatype": column[0], "element_type": column[1].element_type,
                                               "db_type": column[1].db_type or f.db_type})
            continue
        result = columns.get(f.name.casefold())
        if result is not None:
            out[f.name] = f.model_copy(update={"datatype": finer_datatype(result.type, result.db_type),
                                               "element_type": result.element_type,
                                               "db_type": result.db_type or f.db_type})
    return out


async def _ingested_types(
    select: exp.Select, *, client: SqlClient, dialect: SqlDialect,
) -> dict[str, tuple[DataType, ReflectedColumn]]:
    """Per folded result name, the type ingestion gives the one table column it reads unchanged (integer-affinity
    dialects only); a result with several candidate owners keeps its probed type."""
    if not dialect.probes_integer_affinity:
        return {}
    tables: dict[tuple[str, str | None], list[ReflectedColumn]] = {}
    owners: dict[str, list[tuple[ColumnSource, ReflectedColumn]]] = {}
    for read in column_sources(select):
        if read.result is None or read.column is None:
            continue
        where = (read.table, read.schema_name)
        if where not in tables:
            tables[where] = await client.reflect_columns(TableLocation(name=read.table, schema_name=read.schema_name))
        reflected = _reflected(tables[where], column=read.column)
        if reflected is not None:
            owners.setdefault(read.result.casefold(), []).append((read, reflected))
    out: dict[str, tuple[DataType, ReflectedColumn]] = {}
    for result, candidates in owners.items():
        if len(candidates) == 1:
            read, reflected = candidates[0]
            datatype = await ingested_sqlite_type(client=client, table=read.table, name=reflected.name,
                                                  declared=reflected.narrowed_type, schema=read.schema_name)
            out[result] = (datatype, reflected)
    return out


def _reflected(columns: list[ReflectedColumn], *, column: str) -> ReflectedColumn | None:
    """The reflected column ``column`` names: its exact spelling, else the unique one equal up to ASCII case (SQLite's
    identifier comparison)."""
    exact = [c for c in columns if c.name == column]
    hits = exact or [c for c in columns if ascii_lower(c.name) == ascii_lower(column)]
    return hits[0] if len(hits) == 1 else None
