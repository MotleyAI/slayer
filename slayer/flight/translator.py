"""SQL → SlayerQuery translator (DEV-1390 §6).

Shared pipeline for every SQL string entering the Flight SQL facade,
whether through ``CommandStatementQuery`` or the prepared-statement
triplet. Returns a tagged-union ``TranslatorResult`` whose subclass
tells the handler which kind of response to send; raises
``TranslationError`` on user-visible failures (parse error, unknown
table, ``SELECT *``, DML/DDL, etc.).

The pipeline (see §6 of DEV-1390):

1. Parse with sqlglot.
2. Classify AST root → reject DML/DDL, no-op SET/SHOW/BEGIN/COMMIT,
   continue on SELECT.
3. Probe-query whitelist → canned table.
4. INFORMATION_SCHEMA dispatch → canned table.
5. ``SELECT *`` rejection.
6. SLayer-table translation → ``SlayerQuery`` + column-name mapping.

The translator never touches the engine or storage — it produces a
``SlayerQuery`` description and lets the handler decide when to call
``engine.execute()`` (the LIMIT-0 schema vs full-execute distinction
lives in §6.4 Path A / Path B, not here).
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Sequence, Tuple

import pyarrow as pa
import sqlglot
import sqlglot.errors
import sqlglot.expressions as exp
from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.query import (
    ColumnRef,
    OrderItem,
    SlayerQuery,
    TimeDimension,
)
from slayer.flight.catalog import (
    CATALOG_NAME,
    FlightCatalog,
    FlightDimension,
    FlightMetric,
    FlightTable,
)
from slayer.flight.info_schema import match_info_schema
from slayer.flight.probe_queries import match_probe

logger = logging.getLogger(__name__)


# --- result types (tagged union via subclassing) -----------------------------


class TranslatorResult(BaseModel):
    """Base for every translator outcome. Handlers ``isinstance``-dispatch."""

    model_config = ConfigDict(arbitrary_types_allowed=True)


class ProbeResult(TranslatorResult):
    """One of the four whitelisted connection probes matched."""

    table: pa.Table


class InfoSchemaResult(TranslatorResult):
    """``SELECT ... FROM INFORMATION_SCHEMA.<TABLE>`` matched."""

    table: pa.Table


class NoOpResult(TranslatorResult):
    """``BEGIN`` / ``COMMIT`` / ``ROLLBACK`` / ``SET`` / ``SHOW`` — empty success."""

    pass


class QueryResult(TranslatorResult):
    """Translated SlayerQuery for engine execution.

    ``column_name_mapping`` is ordered to match the user's projection
    list; each tuple is ``(engine_alias, bi_tool_projected_name)``.
    Server uses this to rewrite the SLayer response's column keys
    (``orders.revenue_sum``) back into the BI-tool's flat names
    (``revenue_sum``) before emitting Arrow.

    ``projection_types`` is the catalog-declared ``DataType`` for each
    projected item, in the same order. ``None`` entries fall back to
    ``utf8`` at Arrow-schema build time (custom aggs, measures with
    unknown declared type, …). Used in Phase 1 as the wire schema
    source; a future Phase-2 task will derive types from the actual
    LIMIT-0 query execution instead.
    """

    query: SlayerQuery
    column_name_mapping: List[Tuple[str, str]]
    flight_table: FlightTable
    schema_name: str
    projection_types: "List[Optional['DataType']]"


# --- error types -------------------------------------------------------------


class TranslationError(Exception):
    """User-visible translation failure; carries a Flight gRPC status code."""

    def __init__(self, message: str, *, status: str = "INVALID_ARGUMENT") -> None:
        super().__init__(message)
        self.status = status


READ_ONLY_MESSAGE = "SLayer Flight SQL endpoint is read-only"
SELECT_STAR_MESSAGE = (
    "SELECT * not supported; project specific metric or dimension names. "
    "Use 'SELECT * FROM INFORMATION_SCHEMA.METRICS WHERE table_name=...' "
    "to discover available names."
)


# --- AST helpers -------------------------------------------------------------


_TIME_GRAIN_NAMES: Dict[str, TimeGranularity] = {
    "year": TimeGranularity.YEAR,
    "quarter": TimeGranularity.QUARTER,
    "month": TimeGranularity.MONTH,
    "week": TimeGranularity.WEEK,
    "day": TimeGranularity.DAY,
    "hour": TimeGranularity.HOUR,
    "minute": TimeGranularity.MINUTE,
    "second": TimeGranularity.SECOND,
}

# sqlglot represents the unwrapped one-arg time functions as dedicated nodes
# (exp.Month, exp.Year, …). date_trunc is exp.DateTrunc with a literal unit.
_TIME_GRAIN_CLASSES: Dict[type, TimeGranularity] = {
    exp.Year: TimeGranularity.YEAR,
    exp.Quarter: TimeGranularity.QUARTER,
    exp.Month: TimeGranularity.MONTH,
    exp.Week: TimeGranularity.WEEK,
    exp.Day: TimeGranularity.DAY,
    # Hour/Minute/Second don't all have dedicated AST classes; we also accept
    # them via exp.Anonymous below.
}


def _column_to_dotted(col: exp.Column) -> str:
    """Reconstruct the dotted reference from a sqlglot ``Column``.

    ``customers.regions.name`` (3-part) → ``"customers.regions.name"``
    ``customers.row_count`` (2-part)    → ``"customers.row_count"``
    ``revenue_sum``         (bare)      → ``"revenue_sum"``
    """
    parts: List[str] = []
    for key in ("catalog", "db", "table"):
        node = col.args.get(key)
        if node is None:
            continue
        parts.append(str(node.this) if hasattr(node, "this") else str(node))
    leaf = col.this
    parts.append(str(leaf.this) if hasattr(leaf, "this") else str(leaf))
    return ".".join(parts)


def _detect_time_grain_date_trunc(
    node: exp.DateTrunc,
) -> Optional[Tuple[TimeGranularity, exp.Column]]:
    unit = node.args.get("unit")
    col = node.this
    if unit is None or not isinstance(col, exp.Column):
        return None
    unit_str = (
        str(unit.this) if isinstance(unit, exp.Literal) else str(unit)
    ).lower()
    grain = _TIME_GRAIN_NAMES.get(unit_str)
    if grain is None:
        return None
    return grain, col


def _detect_time_grain_single_arg(
    node: exp.Expression,
) -> Optional[Tuple[TimeGranularity, exp.Column]]:
    """Dedicated AST classes like ``exp.Month`` / ``exp.Year``."""
    for cls, grain in _TIME_GRAIN_CLASSES.items():
        if isinstance(node, cls):
            target = node.this
            if isinstance(target, exp.Column):
                return grain, target
            return None
    return None


def _detect_time_grain_anonymous(
    node: exp.Anonymous,
) -> Optional[Tuple[TimeGranularity, exp.Column]]:
    """``hour(col)`` / ``minute(col)`` / ``second(col)`` come through here."""
    grain = _TIME_GRAIN_NAMES.get(str(node.this).lower())
    if grain is None:
        return None
    args = node.args.get("expressions") or []
    if len(args) == 1 and isinstance(args[0], exp.Column):
        return grain, args[0]
    return None


def _detect_time_grain(node: exp.Expression) -> Optional[Tuple[TimeGranularity, exp.Column]]:
    """If ``node`` is ``<grain>(<column>)`` or ``date_trunc('<grain>', <column>)``,
    return ``(granularity, column)``. Otherwise ``None``.
    """
    if isinstance(node, exp.DateTrunc):
        match = _detect_time_grain_date_trunc(node)
        if match is not None:
            return match
    single = _detect_time_grain_single_arg(node)
    if single is not None:
        return single
    if isinstance(node, exp.Anonymous):
        return _detect_time_grain_anonymous(node)
    return None


def _alias_for_time_grain(grain: TimeGranularity, col: exp.Column) -> str:
    """The flat projection name we expose for ``month(ordered_at)`` etc.

    Format: ``"<grain>(<column-ref>)"`` lowercased so it round-trips
    cleanly through GROUP BY / ORDER BY equality checks.
    """
    return f"{grain.value}({_column_to_dotted(col)})"


# --- table resolution --------------------------------------------------------


def _flatten_catalog(catalog: FlightCatalog) -> Dict[str, List[Tuple[str, FlightTable]]]:
    """Build a (model_name → [(schema, table), …]) index for bare-name lookup."""
    by_name: Dict[str, List[Tuple[str, FlightTable]]] = {}
    for sch in catalog.schemas:
        for tbl in sch.tables:
            by_name.setdefault(tbl.name, []).append((sch.name, tbl))
    return by_name


def _unwrap_identifier(node: Optional[exp.Expression]) -> Optional[str]:
    """Pull the string value out of a sqlglot identifier-ish node."""
    if node is None:
        return None
    return str(node.this) if hasattr(node, "this") else str(node)


def _resolve_qualified_table(
    *, schema_str: str, table_name: str, catalog: FlightCatalog,
) -> Tuple[str, FlightTable]:
    for sch in catalog.schemas:
        if sch.name != schema_str:
            continue
        for tbl in sch.tables:
            if tbl.name == table_name:
                return sch.name, tbl
        raise TranslationError(
            f"Unknown table {table_name!r} in schema {schema_str!r}"
        )
    raise TranslationError(f"Unknown schema: {schema_str!r}")


def _resolve_bare_table(
    *, table_name: str, catalog: FlightCatalog,
) -> Tuple[str, FlightTable]:
    matches = _flatten_catalog(catalog).get(table_name, [])
    if not matches:
        raise TranslationError(f"Unknown table: {table_name!r}")
    if len(matches) > 1:
        candidates = ", ".join(f"{s}.{t.name}" for s, t in matches)
        raise TranslationError(
            f"Ambiguous table name {table_name!r}; qualify with one of: "
            f"{candidates}"
        )
    return matches[0]


def _resolve_table(
    from_clause: exp.From, catalog: FlightCatalog,
) -> Tuple[str, FlightTable]:
    """Resolve a SELECT's FROM into ``(schema_name, FlightTable)``.

    Handles the three qualification forms (§6.1):

    * ``<catalog>.<schema>.<table>`` — must match ``slayer.<ds>.<model>``.
    * ``<schema>.<table>`` — direct schema lookup.
    * ``<table>`` — searches every schema; unique match → use, multiple →
      error naming the candidates, zero → "Unknown table".
    """
    inner = from_clause.this
    if not isinstance(inner, exp.Table):
        raise TranslationError(
            f"FROM clause must reference a table, got "
            f"{type(inner).__name__}"
        )
    table_name = _unwrap_identifier(inner.this)
    if not table_name:
        raise TranslationError("FROM clause is missing a table name")
    schema_str = _unwrap_identifier(inner.args.get("db"))
    catalog_str = _unwrap_identifier(inner.args.get("catalog"))

    if catalog_str is not None and catalog_str != CATALOG_NAME:
        raise TranslationError(
            f"Unknown catalog: {catalog_str!r} (only {CATALOG_NAME!r} is exposed)"
        )

    if schema_str is not None:
        return _resolve_qualified_table(
            schema_str=schema_str, table_name=table_name, catalog=catalog,
        )
    return _resolve_bare_table(table_name=table_name, catalog=catalog)


# --- projection translation --------------------------------------------------


class _ProjectionItem(BaseModel):
    """One resolved projection entry."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    projected_name: str  # what the BI tool sees (alias or natural name)
    metric: Optional[FlightMetric] = None
    dimension: Optional[FlightDimension] = None
    time_grain: Optional[TimeGranularity] = None
    time_grain_underlying: Optional[FlightDimension] = None


def _resolve_time_grain_projection(
    *,
    grain: TimeGranularity,
    col: exp.Column,
    alias_name: Optional[str],
    table: FlightTable,
    dims_by_name: Dict[str, FlightDimension],
) -> _ProjectionItem:
    dotted = _column_to_dotted(col)
    dim = dims_by_name.get(dotted)
    if dim is None:
        raise TranslationError(
            f"Unknown dimension {dotted!r} inside time-grain "
            f"{grain.value}() on table {table.name!r}"
        )
    if not dim.is_time:
        raise TranslationError(
            f"Dimension {dotted!r} is not a time column; cannot wrap "
            f"in {grain.value}()"
        )
    return _ProjectionItem(
        projected_name=alias_name or _alias_for_time_grain(grain, col),
        dimension=dim,
        time_grain=grain,
        time_grain_underlying=dim,
    )


def _resolve_column_projection(
    *,
    body: exp.Column,
    alias_name: Optional[str],
    table: FlightTable,
    metrics_by_name: Dict[str, FlightMetric],
    dims_by_name: Dict[str, FlightDimension],
) -> _ProjectionItem:
    dotted = _column_to_dotted(body)
    if dotted in metrics_by_name:
        return _ProjectionItem(
            projected_name=alias_name or dotted,
            metric=metrics_by_name[dotted],
        )
    if dotted in dims_by_name:
        return _ProjectionItem(
            projected_name=alias_name or dotted,
            dimension=dims_by_name[dotted],
        )
    raise TranslationError(
        f"Unknown projection item {dotted!r} on table {table.name!r}"
    )


def _resolve_projection(
    expressions: Sequence[exp.Expression], table: FlightTable,
) -> List[_ProjectionItem]:
    """Walk the projection list, classifying each item against the table."""
    metrics_by_name = {m.name: m for m in table.metrics}
    dims_by_name = {d.name: d for d in table.dimensions}

    out: List[_ProjectionItem] = []
    for expr in expressions:
        if isinstance(expr, exp.Star):
            raise TranslationError(SELECT_STAR_MESSAGE)

        alias_name: Optional[str] = None
        body: exp.Expression = expr
        if isinstance(expr, exp.Alias):
            alias_name = str(expr.alias)
            body = expr.this

        grain_match = _detect_time_grain(body)
        if grain_match is not None:
            grain, col = grain_match
            out.append(_resolve_time_grain_projection(
                grain=grain, col=col, alias_name=alias_name,
                table=table, dims_by_name=dims_by_name,
            ))
            continue

        if isinstance(body, exp.Column):
            out.append(_resolve_column_projection(
                body=body, alias_name=alias_name, table=table,
                metrics_by_name=metrics_by_name, dims_by_name=dims_by_name,
            ))
            continue

        raise TranslationError(
            f"Unsupported projection expression: {body.sql()!r}"
        )
    return out


# --- WHERE translation -------------------------------------------------------


def _split_and_chain(node: exp.Expression) -> List[exp.Expression]:
    """Flatten a top-level AND chain into its conjuncts."""
    out: List[exp.Expression] = []
    stack = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, exp.And):
            stack.append(cur.expression)
            stack.append(cur.this)
        else:
            out.append(cur)
    return out


def _lift_time_between(
    conj: exp.Between, time_dim_names: set[str],
) -> Optional[Tuple[str, Optional[str], Optional[str]]]:
    col = conj.this
    if not isinstance(col, exp.Column):
        return None
    dotted = _column_to_dotted(col)
    if dotted not in time_dim_names:
        return None
    lo = _literal_str(conj.args.get("low"))
    hi = _literal_str(conj.args.get("high"))
    if lo and hi:
        return dotted, lo, hi
    return None


def _lift_time_comparator(
    conj: exp.Expression, time_dim_names: set[str],
) -> Optional[Tuple[str, Optional[str], Optional[str]]]:
    col = conj.this
    if not isinstance(col, exp.Column):
        return None
    dotted = _column_to_dotted(col)
    if dotted not in time_dim_names:
        return None
    val = _literal_str(conj.expression)
    if val is None:
        return None
    if isinstance(conj, (exp.GTE, exp.GT)):
        return dotted, val, None
    return dotted, None, val


def _classify_where_conjunct(
    conj: exp.Expression, time_dim_names: set[str],
) -> Tuple[Optional[Tuple[str, Optional[str], Optional[str]]], Optional[str]]:
    """Classify a single conjunct.

    Returns ``((time_dim, date_range_lo, date_range_hi), None)`` if this is
    a time-dim filter that should lift to ``time_dimensions[*].date_range``.
    Returns ``(None, verbatim_sql)`` for the everything-else case.
    """
    if isinstance(conj, exp.Between):
        lifted = _lift_time_between(conj, time_dim_names)
        if lifted is not None:
            return lifted, None
    if isinstance(conj, (exp.GTE, exp.GT, exp.LTE, exp.LT)):
        lifted = _lift_time_comparator(conj, time_dim_names)
        if lifted is not None:
            return lifted, None
    return None, _rewrite_neq(conj.sql())


def _literal_str(node: Optional[exp.Expression]) -> Optional[str]:
    if node is None:
        return None
    if isinstance(node, exp.Literal):
        return str(node.this)
    return None


def _rewrite_neq(sql: str) -> str:
    """SQL ``!=`` → SLayer DSL ``<>`` (DSL preference per §6.2)."""
    return sql.replace("!=", "<>")


def _apply_where(
    where: Optional[exp.Where],
    time_dims_built: Dict[str, TimeDimension],
    filters_out: List[str],
) -> None:
    """Walk the WHERE chain; lift time-dim filters, append verbatim rest."""
    if where is None:
        return
    time_dim_names = set(time_dims_built.keys())
    for conj in _split_and_chain(where.this):
        lifted, verbatim = _classify_where_conjunct(conj, time_dim_names)
        if lifted is not None:
            name, lo, hi = lifted
            td = time_dims_built[name]
            existing = list(td.date_range or [None, None])
            if lo is not None:
                existing[0] = lo
            if hi is not None:
                existing[1] = hi
            td.date_range = existing  # type: ignore[assignment]
        elif verbatim is not None:
            filters_out.append(verbatim)


# --- ORDER BY / GROUP BY -----------------------------------------------------


def _translate_order_by(
    order: Optional[exp.Order],
    item_by_projected_name: Dict[str, _ProjectionItem],
) -> List[OrderItem]:
    if order is None:
        return []
    out: List[OrderItem] = []
    for ord_expr in order.args.get("expressions") or []:
        if not isinstance(ord_expr, exp.Ordered):
            continue
        body = ord_expr.this
        direction = "desc" if ord_expr.args.get("desc") else "asc"
        if isinstance(body, exp.Column):
            name = _column_to_dotted(body)
        else:
            name = body.sql()
        if name not in item_by_projected_name:
            raise TranslationError(
                f"ORDER BY column {name!r} is not in the projection list"
            )
        item = item_by_projected_name[name]
        if item.metric is not None:
            ref = ColumnRef(name=item.metric.name)
        else:
            assert item.dimension is not None
            ref = ColumnRef.from_string(item.dimension.dimension_ref)
        out.append(OrderItem(column=ref, direction=direction))
    return out


def _validate_group_by(
    group: Optional[exp.Group],
    derived: List[str],
) -> None:
    """Apply the strict-on-extras / lenient-on-omissions policy (§6.1)."""
    if group is None:
        return
    derived_set = set(derived)
    user_items: List[str] = []
    for g in group.args.get("expressions") or []:
        if isinstance(g, exp.Column):
            user_items.append(_column_to_dotted(g))
        else:
            grain_match = _detect_time_grain(g)
            if grain_match is not None:
                grain, col = grain_match
                user_items.append(_alias_for_time_grain(grain, col))
            else:
                user_items.append(g.sql())
    for u in user_items:
        if u not in derived_set:
            raise TranslationError(
                f"GROUP BY item {u!r} is not in the projection's derived "
                f"dimension set ({sorted(derived_set)})"
            )


# --- main entry point --------------------------------------------------------


def _is_start_transaction(node: exp.Expression) -> bool:
    """`START TRANSACTION` parses oddly: sqlglot sees `START` as a column and
    `TRANSACTION` as an alias. Match that pattern explicitly."""
    if not isinstance(node, exp.Alias):
        return False
    body = node.this
    if not isinstance(body, exp.Column):
        return False
    body_name = (
        str(body.this.this) if hasattr(body.this, "this") else str(body.this)
    ).upper()
    alias_name = str(node.alias).upper()
    return body_name == "START" and alias_name == "TRANSACTION"


def translate(sql: str, catalog: FlightCatalog) -> TranslatorResult:
    """Translate a SQL string into a TranslatorResult.

    Raises ``TranslationError`` on user-visible failures.
    """
    try:
        parsed = sqlglot.parse_one(sql)
    except sqlglot.errors.ParseError as exc:
        raise TranslationError(f"SQL parse error: {exc}") from exc

    # Step 2 — AST root classification.
    if isinstance(parsed, (exp.Insert, exp.Update, exp.Delete, exp.Merge,
                            exp.TruncateTable)):
        raise TranslationError(READ_ONLY_MESSAGE)
    if isinstance(parsed, (exp.Create, exp.Drop, exp.Alter)):
        raise TranslationError(READ_ONLY_MESSAGE)
    if isinstance(parsed, (exp.Transaction, exp.Commit, exp.Rollback,
                            exp.Set)):
        return NoOpResult()
    # sqlglot quirks: `START TRANSACTION` parses as an Alias (column "START"
    # aliased as "TRANSACTION"), and `SHOW <anything>` falls through to the
    # generic exp.Command with `this="SHOW"`. Catch both.
    if _is_start_transaction(parsed):
        return NoOpResult()
    if isinstance(parsed, exp.Command):
        verb = str(parsed.this).upper() if parsed.this else ""
        if verb in {"SHOW", "USE", "RESET"}:
            return NoOpResult()
    if not isinstance(parsed, exp.Select):
        raise TranslationError(
            f"Unsupported statement: {type(parsed).__name__}"
        )

    # Step 3 — probe-query whitelist.
    probe = match_probe(parsed)
    if probe is not None:
        return ProbeResult(table=probe)

    # Step 4 — INFORMATION_SCHEMA dispatch.
    info = match_info_schema(parsed=parsed, catalog=catalog)
    if info is not None:
        return InfoSchemaResult(table=info)

    # Step 5 / 6 — SLayer-table translation.
    return _translate_slayer_select(parsed, catalog)


class _ProjectionPlan(BaseModel):
    """Pieces of a SlayerQuery derived from the SELECT projection."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    measures: List[dict]
    dimension_refs: List[ColumnRef]
    time_dims: List[TimeDimension]
    time_dim_by_name: Dict[str, TimeDimension]
    derived_dims: List[str]
    column_name_mapping: List[Tuple[str, str]]
    projection_types: List[Optional[DataType]]


def _record_metric(
    *, plan: _ProjectionPlan, item: _ProjectionItem, table: FlightTable,
) -> None:
    assert item.metric is not None
    plan.measures.append({
        "formula": item.metric.measure_formula,
        "name": item.projected_name,
    })
    engine_alias = f"{table.name}.{item.projected_name}"
    plan.column_name_mapping.append((engine_alias, item.projected_name))
    plan.projection_types.append(item.metric.data_type)


def _record_time_grain(
    *, plan: _ProjectionPlan, item: _ProjectionItem, table: FlightTable,
) -> None:
    assert item.time_grain is not None and item.time_grain_underlying is not None
    dotted = item.time_grain_underlying.dimension_ref
    td = TimeDimension(
        dimension={"name": dotted},
        granularity=item.time_grain,
    )
    plan.time_dims.append(td)
    plan.time_dim_by_name[dotted] = td
    plan.derived_dims.append(item.projected_name)
    engine_alias = f"{table.name}.{dotted}"
    plan.column_name_mapping.append((engine_alias, item.projected_name))
    plan.projection_types.append(item.time_grain_underlying.data_type)


def _record_dimension(
    *, plan: _ProjectionPlan, item: _ProjectionItem, table: FlightTable,
) -> None:
    assert item.dimension is not None
    plan.dimension_refs.append(ColumnRef.from_string(item.dimension.dimension_ref))
    plan.derived_dims.append(item.projected_name)
    engine_alias = f"{table.name}.{item.dimension.dimension_ref}"
    plan.column_name_mapping.append((engine_alias, item.projected_name))
    plan.projection_types.append(item.dimension.data_type)


def _build_projection_plan(
    items: Sequence[_ProjectionItem], table: FlightTable,
) -> _ProjectionPlan:
    plan = _ProjectionPlan(
        measures=[], dimension_refs=[], time_dims=[], time_dim_by_name={},
        derived_dims=[], column_name_mapping=[], projection_types=[],
    )
    for item in items:
        if item.metric is not None:
            _record_metric(plan=plan, item=item, table=table)
        elif item.time_grain is not None:
            _record_time_grain(plan=plan, item=item, table=table)
        else:
            _record_dimension(plan=plan, item=item, table=table)
    return plan


def _parse_int_literal(node: Optional[exp.Expression]) -> Optional[int]:
    """Pull an int out of ``LIMIT N`` / ``OFFSET N`` style nodes."""
    if node is None or not isinstance(node.expression, exp.Literal):
        return None
    try:
        return int(str(node.expression.this))
    except ValueError:
        return None


def _translate_slayer_select(
    parsed: exp.Select, catalog: FlightCatalog,
) -> QueryResult:
    from_clause = parsed.args.get("from_")
    if from_clause is None:
        raise TranslationError(
            "No FROM clause; expected one of the registered Flight tables "
            "or INFORMATION_SCHEMA.*"
        )
    schema_name, table = _resolve_table(from_clause, catalog)

    proj_exprs = parsed.args.get("expressions") or []
    # Reject SELECT * before catalog lookup so we get the named error
    # instead of "Unknown projection item '*'".
    if any(isinstance(e, exp.Star) for e in proj_exprs):
        raise TranslationError(SELECT_STAR_MESSAGE)

    items = _resolve_projection(proj_exprs, table)
    plan = _build_projection_plan(items, table)

    _validate_group_by(parsed.args.get("group"), plan.derived_dims)

    filters: List[str] = []
    _apply_where(parsed.args.get("where"), plan.time_dim_by_name, filters)

    item_by_projected_name = {item.projected_name: item for item in items}
    order_items = _translate_order_by(parsed.args.get("order"), item_by_projected_name)

    query = SlayerQuery(
        source_model=table.name,
        measures=plan.measures or None,
        dimensions=plan.dimension_refs or None,
        time_dimensions=plan.time_dims or None,
        filters=filters or None,
        order=order_items or None,
        limit=_parse_int_literal(parsed.args.get("limit")),
        offset=_parse_int_literal(parsed.args.get("offset")),
    )

    return QueryResult(
        query=query,
        column_name_mapping=plan.column_name_mapping,
        flight_table=table,
        schema_name=schema_name,
        projection_types=plan.projection_types,
    )
