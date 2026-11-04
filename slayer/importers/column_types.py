"""Imported column types: the database's first, then the source's declared type, then the aggregation's."""

from __future__ import annotations

import sqlalchemy as sa
from sqlglot import exp
from sqlglot.expressions.core import Expression
from sqlglot.errors import SqlglotError

from slayer.core.enums import DataType
from slayer.engine.ingestion import introspect_table_to_model
from slayer.engine.schema_scope import table_schema_ref
from slayer.sql.client import CATEGORY_DATA_TYPES, probe_columns_sync, render_type_probe
from slayer.sql.dialects import SQLGLOT_NAMES, SqlDialect, dialect_for_ds_type, dialect_for_sa_backend, get_dialect
from slayer.sql.dialects.base import slayer_type_of
from slayer.sql.render.parse import parse_expression, parse_query, parse_table

Typed = tuple[DataType, DataType | None]

_NUMERIC_AGGS = frozenset({"sum", "avg", "average", "median", "percentile", "stddev", "variance"})
_COUNT_AGGS = frozenset({"count", "count_distinct", "count_distinct_approx"})


def declared_type(text: str, *, dialect: str | None) -> Typed | None:
    """A declared SQL type in ``dialect`` as SLayer's; ``None`` when unrecognised."""
    try:
        found = slayer_type_of(exp.DataType.build(text, dialect=dialect))
    except (SqlglotError, ValueError):
        return None
    return None if found[0] is DataType.UNKNOWN else found


def implied_type(agg: str) -> Typed | None:
    """The type an aggregation implies of its input: numeric for ``sum``-like, ``TEXT`` for a count."""
    if agg == "sum_boolean":
        return DataType.INT, None
    if agg in _NUMERIC_AGGS:
        return DataType.DOUBLE, None
    return (DataType.TEXT, None) if agg in _COUNT_AGGS else None


def refused_over_list(agg: str) -> bool:
    """Whether SLayer refuses ``agg`` over a list (only ``count`` counts a list's rows)."""
    return agg != "count"


_FAMILIES = ({DataType.INT, DataType.DOUBLE}, {DataType.DATE, DataType.TIMESTAMP})


def contradicts(declared: Typed, live: Typed) -> bool:
    """Whether a live type contradicts a declared one (``INT`` vs ``DOUBLE``, ``DATE`` vs ``TIMESTAMP`` agree)."""
    if declared[0] is DataType.ARRAY or live[0] is DataType.ARRAY:
        return declared != live and not (declared[0] is live[0] and DataType.UNKNOWN in (declared[1], live[1]))
    return declared[0] is not live[0] and not any({declared[0], live[0]} <= family for family in _FAMILIES)


def offline_note(reason: str) -> str:
    return f"Column types were not checked against the database ({reason}); re-import against a reachable " \
           f"datasource to type them from the live schema."


def untyped_message(measure: str, *, agg: str, column: str) -> str:
    return (f"Measure {measure!r} was dropped: {agg} needs the type of {column!r}, which no datasource or "
            f"declaration gives; re-import against a reachable datasource or declare the type.")


def list_refusal(measure: str, *, agg: str, column: str) -> str:
    return f"Measure {measure!r} was dropped: {agg} over the list (ARRAY) column {column!r} is not supported."


def contradiction_note(column: str, *, declared: Typed, live: Typed) -> str:
    return (f"Column {column!r} declares {_shown(declared)} but the live column is {_shown(live)}; using "
            f"{_shown(live)}.")


def _shown(typed: Typed) -> str:
    return f"{typed[0].value}<{typed[1].value}>" if typed[1] is not None else typed[0].value


class LiveSource:
    """A source's live column types, and a probe for an expression's type over it."""

    def __init__(self, *, engine: sa.Engine, data_source: str, db_type: str | None, dialect: str | None,
                 sql_table: str | None = None, sql: str | None = None) -> None:
        self.engine = engine
        self.data_source = data_source
        self.db_type = db_type or engine.dialect.name
        self.dialect = dialect
        self._target = dialect_for_ds_type(db_type) if db_type else dialect_for_sa_backend(engine.dialect.name)
        self._source: SqlDialect = get_dialect(dialect) if dialect in SQLGLOT_NAMES else self._target
        if sql_table:
            table = parse_table(name=sql_table, dialect=self._source)
            self._relation: Expression = table
            self.columns: dict[str, Typed] = self._reflect(table)
        else:
            query = parse_query(sql=sql or "", target_dialect=self._target, parse_dialect=self._source)
            self._relation = query.subquery("_slayer_src")
            self.columns = self._described()

    def _reflect(self, table: exp.Table) -> dict[str, Typed]:
        ref = table_schema_ref(catalog=table.catalog, schema=table.db, sa_engine=self.engine)
        model = introspect_table_to_model(sa_engine=self.engine, inspector=sa.inspect(self.engine),
                                          table_name=table.name, ref=ref, data_source=self.data_source)
        if not model.columns:
            raise ValueError(f"table {table.sql(dialect=self._source.sqlglot_name)} was not found")
        return {c.name: (c.type, c.element_type) for c in model.columns}

    def _probed(self, query: exp.Select) -> dict[str, Typed]:
        """Each column of ``query``: the driver's type name where it gives one, else its type category."""
        out: dict[str, Typed] = {}
        for col, found in probe_columns_sync(render_type_probe(query, dialect=self._target), engine=self.engine,
                                             db_type=self.db_type, datasource_name=self.data_source).items():
            named = declared_type(found.type_name, dialect=self.dialect) if found.type_name else None
            out[col] = named or (CATEGORY_DATA_TYPES[found.category],
                                 DataType.UNKNOWN if found.category == "list" else None)
        return out

    def _described(self) -> dict[str, Typed]:
        return self._probed(exp.select("*").from_(self._relation.copy()))

    def column(self, physical: str) -> Typed | None:
        """The live type of the physical column ``physical``."""
        if physical in self.columns:
            return self.columns[physical]
        return next((t for n, t in self.columns.items() if n.lower() == physical.lower()), None)

    def probe(self, expr: str) -> Typed | None:
        """``expr``'s type from a zero-row probe over the source; a list's elements are ``UNKNOWN``."""
        try:
            value = parse_expression(sql=expr, target_dialect=self._target, parse_dialect=self._source)
            types = self._probed(exp.select(exp.paren(value).as_("_slayer_probe")).from_(self._relation.copy()))
        except Exception:  # noqa: BLE001 — a probe is best-effort
            return None
        typed = next(iter(types.values()), None)
        return (DataType.ARRAY, DataType.UNKNOWN) if typed is not None and typed[0] is DataType.ARRAY else typed


class LiveDatasource:
    """An importer's datasource: each source's live schema, the connection tried at most once."""

    def __init__(self, *, engine: sa.Engine | None, offline_reason: str | None, data_source: str,
                 db_type: str | None, dialect: str | None) -> None:
        self.engine = engine
        self.data_source = data_source
        self.db_type = db_type
        self.dialect = dialect
        self._offline = None if engine is not None else offline_reason or "no datasource connection was given"
        self._tried = False

    def _reachable(self, engine: sa.Engine) -> bool:
        if not self._tried:
            self._tried = True
            try:
                engine.connect().close()
            except Exception as exc:  # noqa: BLE001 — an unreachable datasource still imports
                self._offline = f"datasource {self.data_source!r} could not be reached: {type(exc).__name__}"
        return self._offline is None

    def source(self, *, sql_table: str | None = None, sql: str | None = None) -> LiveSource | str:
        """The source's live schema, or why types are not checked against the database."""
        engine = self.engine
        if engine is None or not self._reachable(engine):
            return self._offline or "no datasource connection was given"
        try:
            return LiveSource(engine=engine, data_source=self.data_source, db_type=self.db_type, dialect=self.dialect,
                              sql_table=sql_table, sql=sql)
        except Exception as exc:  # noqa: BLE001 — an unreadable source still imports
            return f"the source could not be read from datasource {self.data_source!r}: {type(exc).__name__}"
