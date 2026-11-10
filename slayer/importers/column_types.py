"""Imported column types: the database's first, then the source's declared type, then the aggregation's."""

from __future__ import annotations

from collections.abc import Callable

from sqlglot import exp
from sqlglot.expressions.core import Expression
from sqlglot.errors import SqlglotError

from slayer.core.enums import DataType
from slayer.live_schema.ingestion import introspect_table_columns
from slayer.live_schema.schema_scope import live_name, table_live_parts, table_schema_ref
from slayer.importers.physical import PhysicalIndex, case_insensitive, live_exact_name
from slayer.database.client import ResultColumnType, SqlClient, render_type_probe
from slayer.sql.dialects import SQLGLOT_NAMES, SqlDialect, dialect_for_ds_type, get_dialect
from slayer.sql.dialects.base import slayer_type_of
from slayer.sql.naming import denoted_name
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


#: Conversion passes ``LiveDatasource.collect`` runs to discover the sources and probes conversion asks for.
MAX_COLLECT_PASSES = 8

_PENDING = "the source is being read"

SourceKey = tuple[str | None, str | None]


def _output_labels(query: exp.Query) -> list[exp.Identifier]:
    """The named output labels of a query's select list (``*`` and unnamed expressions are skipped)."""
    labels = []
    for select in query.selects:
        found = select.args.get("alias") if isinstance(select, exp.Alias) else select.this
        if isinstance(select, (exp.Alias, exp.Column)) and isinstance(found, exp.Identifier):
            labels.append(found)
    return labels


def _typed(found: ResultColumnType) -> Typed:
    if found.type is DataType.ARRAY:
        return DataType.ARRAY, found.element_type or DataType.UNKNOWN
    return found.type, None


class LiveSource:
    """A source's collected live column types, and its expressions' collected probe types; never reads the database."""

    def __init__(self, *, owner: LiveDatasource, key: SourceKey, relation: Expression,
                 found: list[tuple[str, ResultColumnType]], target: SqlDialect, source: SqlDialect,
                 labels: list[exp.Identifier] | None = None) -> None:
        self._owner = owner
        self.key = key
        self.relation = relation
        self.target = target
        self.source = source
        # Live column names in SQLAlchemy's spelling, its quote flag kept.
        self.names: list[str] = [name for name, _ in found]
        self.result_types: dict[str, ResultColumnType] = dict(found)
        self.columns: dict[str, Typed] = {name: _typed(t) for name, t in found}
        self._index = PhysicalIndex([], dialect=source.sqlglot_name, live=self.names)
        self._by_exact = {live_exact_name(name, dialect=source): _typed(t) for name, t in found}
        keys = [self._label_key(label) for label in labels or []]
        self._duplicated = frozenset(k for k in keys if keys.count(k) > 1)

    def _label_key(self, ident: exp.Identifier) -> str:
        name = denoted_name(ident, dialect=self.source)
        return name.casefold() if case_insensitive(self.source.sqlglot_name) else name

    def duplicated(self, ident: exp.Identifier) -> bool:
        """Whether ``ident`` names an output label a query source holds more than once."""
        return self._label_key(ident) in self._duplicated

    def column(self, physical: exp.Identifier | str) -> Typed | None:
        """The live type of the one physical column ``physical`` names; ``None`` for none or several."""
        found = self._index.live_names(exp.to_identifier(physical) if isinstance(physical, str) else physical)
        return self._by_exact[found[0]] if len(found) == 1 else None

    def probed(self, expr: str) -> ResultColumnType | None:
        """``expr``'s collected probe type over the source; ``None`` when not (yet) collected or unprobeable."""
        return self._owner.probed(self, expr)

    def probe(self, expr: str) -> Typed | None:
        """``expr``'s type from a zero-row probe over the source; a list's elements are ``UNKNOWN``."""
        found = self.probed(expr)
        if found is None:
            return None
        return (DataType.ARRAY, DataType.UNKNOWN) if found.type is DataType.ARRAY else (found.type, None)


class LiveDatasource:
    """An importer's datasource: each source's live schema and probes, read by ``collect`` before a pure convert."""

    def __init__(self, *, offline_reason: str | None, data_source: str, db_type: str | None,
                 dialect: str | None) -> None:
        self.data_source = data_source
        self.db_type = db_type
        self.dialect = dialect
        self._offline: str | None = offline_reason or "no datasource connection was given"
        self._sources: dict[SourceKey, LiveSource | str] = {}
        self._probes: dict[tuple[SourceKey, str], ResultColumnType | None] = {}
        self._wanted_sources: set[SourceKey] = set()
        self._wanted_probes: set[tuple[SourceKey, str]] = set()
        self._discovering = False

    def source(self, *, sql_table: str | None = None, sql: str | None = None) -> LiveSource | str:
        """The source's live schema, or why types are not checked against the database."""
        key: SourceKey = (sql_table, None) if sql_table else (None, sql)
        found = self._sources.get(key)
        if found is not None:
            return found
        if self._discovering:
            self._wanted_sources.add(key)
            return _PENDING
        return self._offline or f"the source was not read from datasource {self.data_source!r}"

    def probed(self, source: LiveSource, expr: str) -> ResultColumnType | None:
        """``expr``'s collected probe type over ``source`` (asked for while discovering)."""
        key = (source.key, expr)
        if key not in self._probes and self._discovering:
            self._wanted_probes.add(key)
        return self._probes.get(key)

    async def collect(self, *, client: SqlClient, convert: Callable[[], object]) -> None:
        """Read through ``client`` each source and probe ``convert`` asks for, re-running it until it asks for
        nothing new; an unreachable datasource leaves every source offline."""
        try:
            await client.ping()
        except Exception as exc:  # noqa: BLE001 — an unreachable datasource still imports
            self._offline = f"datasource {self.data_source!r} could not be reached: {type(exc).__name__}"
            return
        self._offline = None
        self._discovering = True
        try:
            for _ in range(MAX_COLLECT_PASSES):
                self._wanted_sources.clear()
                self._wanted_probes.clear()
                convert()
                sources = self._wanted_sources - self._sources.keys()
                probes = self._wanted_probes - self._probes.keys()
                if not sources and not probes:
                    break
                for key in sorted(sources, key=str):
                    self._sources[key] = await self._read(client=client, key=key)
                for probe in sorted(probes, key=str):
                    self._probes[probe] = await self._probe(client=client, key=probe)
        finally:
            self._discovering = False

    def _dialects(self, client: SqlClient) -> tuple[SqlDialect, SqlDialect]:
        target = dialect_for_ds_type(self.db_type or client.datasource.type)
        return target, get_dialect(self.dialect) if self.dialect in SQLGLOT_NAMES else target

    async def _read(self, *, client: SqlClient, key: SourceKey) -> LiveSource | str:
        sql_table, sql = key
        target, source = self._dialects(client)
        labels: list[exp.Identifier] = []
        try:
            if sql_table:
                table = parse_table(name=sql_table, dialect=source)
                *qualifiers, name = table_live_parts(table)
                ref = await table_schema_ref(catalog=qualifiers[-2] if len(qualifiers) > 1 else None,
                                             schema=qualifiers[-1] if qualifiers else None, client=client)
                columns = await introspect_table_columns(client=client, table_name=name, ref=ref)
                if not columns:
                    raise ValueError(f"table {table.sql(dialect=source.sqlglot_name)} was not found")
                relation: Expression = table
                found = [(live_name(c.name, quoted=c.quoted),
                          ResultColumnType(type=c.type, element_type=c.element_type, db_type=c.db_type))
                         for c in columns]
            else:
                query = parse_query(sql=sql or "", target_dialect=target, parse_dialect=source)
                relation = query.subquery("_slayer_src")
                labels = _output_labels(query)
                # The driver's result names are in SQLAlchemy's spelling already.
                found = list((await client.get_result_column_types(
                    render_type_probe(exp.select("*").from_(relation.copy()), dialect=target))).items())
        except Exception as exc:  # noqa: BLE001 — an unreadable source still imports
            return f"the source could not be read from datasource {self.data_source!r}: {type(exc).__name__}"
        return LiveSource(owner=self, key=key, relation=relation, found=found, target=target, source=source,
                          labels=labels)

    async def _probe(self, *, client: SqlClient, key: tuple[SourceKey, str]) -> ResultColumnType | None:
        live = self._sources.get(key[0])
        if not isinstance(live, LiveSource):
            return None
        try:
            value = parse_expression(sql=key[1], target_dialect=live.target, parse_dialect=live.source)
            query = exp.select(exp.paren(value).as_("_slayer_probe")).from_(live.relation.copy())
            types = await client.get_result_column_types(render_type_probe(query, dialect=live.target))
        except Exception:  # noqa: BLE001 — a probe is best-effort
            return None
        return next(iter(types.values()), None)
