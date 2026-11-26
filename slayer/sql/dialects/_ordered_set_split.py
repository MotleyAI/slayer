"""``PERCENTILE_CONT`` / ``PERCENTILE_DISC … WITHIN GROUP`` on a database without it as a GROUP BY aggregate.

Each SELECT reading one is split in two: a row-level SELECT over the same FROM / WHERE computing, per row, the window
values the database has, partitioned by the GROUP BY keys; and a regrouping SELECT over it in which each percentile
becomes an aggregate of those values and every other column reads its row-level copy.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from sqlglot import exp
from sqlglot.expressions.core import Expr, Expression

from slayer.core.enums import DataType, TypeKind, kind_of

_ROWS = "__slayer_os"
_COLUMN = "__slayer_o"

#: ``(p, value, partition)`` → ``(row-level window columns, pick over their outer reads)``.
PercentileColumns = Callable[
    [Expr, Expr, Sequence[Expr]],
    Tuple[Sequence[Expr], Callable[[List[exp.Column]], Expr]],
]
#: The columns builder splitting an ordered-set node, ``None`` to leave it in place.
ColumnsFor = Callable[[exp.WithinGroup], Optional[PercentileColumns]]
#: Marks a ``PERCENTILE_DISC`` node the split ranks even where the database has a native one.
RANKED = "__slayer_ranked_percentile"


def _ranked(value: Expr, partition: Sequence[Expr]) -> List[Expr]:
    """Per row: its rank among the group's non-NULL values, the group's non-NULL count, and the value."""
    is_null = exp.Case(ifs=[exp.If(this=exp.Is(this=value.copy(), expression=exp.Null()),
                                   true=exp.Literal.number(1))], default=exp.Literal.number(0))
    rank = exp.Window(this=exp.RowNumber(), partition_by=[*(k.copy() for k in partition), is_null],
                      order=exp.Order(expressions=[exp.Ordered(this=value.copy(), nulls_first=True)]))
    count = exp.Window(this=exp.Count(this=value.copy()), partition_by=[k.copy() for k in partition])
    return [rank, count, value.copy()]


def _value_at(rn: Expr, v: Expr, rank: Expr) -> Expression:
    """The non-NULL value ranked ``rank`` in its group."""
    hit = exp.and_(exp.EQ(this=rn.copy(), expression=rank), exp.Not(this=exp.Is(this=v.copy(), expression=exp.Null())))
    return exp.Max(this=exp.Case(ifs=[exp.If(this=hit, true=v.copy())]))


def discrete_percentile(p: Expr, value: Expr, partition: Sequence[Expr]
                        ) -> Tuple[Sequence[Expr], Callable[[List[exp.Column]], Expr]]:
    """Exact ``PERCENTILE_DISC`` from ``ROW_NUMBER`` / ``COUNT`` windows: the non-NULL value ranked ``⌈p·n⌉`` per
    group, rank 1 at ``p = 0``."""

    def pick(reads: List[exp.Column]) -> Expr:
        rn, n, v = reads
        zero = isinstance(p, exp.Literal) and p.is_number and Decimal(p.this) == 0
        rank = exp.Literal.number(1) if zero else exp.Ceil(this=exp.Mul(this=p.copy(), expression=n.copy()))
        return _value_at(rn, v, rank)

    return _ranked(value, partition), pick


def interpolated_percentile(p: Expr, value: Expr, partition: Sequence[Expr]
                            ) -> Tuple[Sequence[Expr], Callable[[List[exp.Column]], Expr]]:
    """Exact ``PERCENTILE_CONT`` from ``ROW_NUMBER`` / ``COUNT`` windows: the non-NULL values ranked per group,
    interpolated between ranks ``⌊h⌋`` and ``⌈h⌉`` at ``h = 1 + p·(n − 1)``."""

    def pick(reads: List[exp.Column]) -> Expr:
        rn, n, v = reads

        def position(size: Expression) -> Expression:
            return exp.Add(this=exp.Literal.number(1), expression=exp.Mul(
                this=p.copy(), expression=exp.Paren(this=exp.Sub(this=size, expression=exp.Literal.number(1)))))

        def at(rank_of: type[exp.Func]) -> Expression:
            return _value_at(rn, v, rank_of(this=position(n.copy())))

        h = position(exp.Max(this=n.copy()))
        fraction = exp.Paren(this=exp.Sub(this=h, expression=exp.Floor(this=h.copy())))
        low = at(exp.Floor)
        return exp.Add(this=low, expression=exp.Mul(
            this=fraction, expression=exp.Paren(this=exp.Sub(this=at(exp.Ceil), expression=low.copy()))))

    return _ranked(value, partition), pick


def ranked_unless_numeric(node: exp.WithinGroup, *, input_type: Optional[DataType]) -> exp.WithinGroup:
    """``node`` marked for ranking unless it orders numbers (a native ``PERCENTILE_DISC`` that orders numbers only)."""
    if kind_of(input_type) not in (TypeKind.INT, TypeKind.DOUBLE):
        node.meta[RANKED] = True
    return node


def marked_ranked(node: exp.WithinGroup) -> Optional[PercentileColumns]:
    """Ranks only the nodes marked for it."""
    return discrete_percentile if node.meta.get(RANKED) else None


def ranked_percentiles(node: exp.WithinGroup) -> Optional[PercentileColumns]:
    """Both percentiles from ranks: for a database with neither as an aggregate."""
    if isinstance(node.this, exp.PercentileCont):
        return interpolated_percentile
    return discrete_percentile if isinstance(node.this, exp.PercentileDisc) else None


def split_percentile_selects(statement: Expression, *, columns: ColumnsFor) -> Expression:
    """``statement`` with every SELECT reading an ordered-set node ``columns`` splits, split as above."""
    for select in list(statement.find_all(exp.Select)):  # NOSONAR(S7504) — each split edits the tree being walked
        if any(_splits(n, columns) for n in _own_nodes(select)):
            _split(select, columns=columns)
    return statement


def _splits(node: Expr, columns: ColumnsFor) -> bool:
    return isinstance(node, exp.WithinGroup) and columns(node) is not None


def _outer_parts(select: exp.Select) -> List[Expr]:
    return [v for key, value in select.args.items() if key not in ("from_", "joins", "where", "with_")
            for v in (value if isinstance(value, list) else [value]) if isinstance(v, Expr)]


def _own_nodes(select: exp.Select):
    """The nodes of ``select``'s own scope outside FROM / JOIN / WHERE; nested SELECTs are their own."""
    stack = _outer_parts(select)
    while stack:
        node = stack.pop()
        yield node
        stack.extend(c for c in node.iter_expressions() if not isinstance(c, (exp.Select, exp.Subquery)))


def _grouping_term(key: Expr, select: exp.Select) -> Expr:
    """A GROUP BY ordinal or output alias as the projection it names (a window partition has neither)."""
    if isinstance(key, exp.Literal) and key.is_int and 1 <= int(key.this) <= len(select.expressions):
        key = select.expressions[int(key.this) - 1]
    elif isinstance(key, exp.Column) and not key.table:
        key = next((e for e in select.expressions if isinstance(e, exp.Alias) and e.alias == key.name), key)
    return (key.this if isinstance(key, exp.Alias) else key).copy()


def _complement(p: Expr) -> Expr:
    """``1 − p``; a literal for a literal ``p`` (SQL Server and BigQuery take only a literal fraction)."""
    if isinstance(p, exp.Literal) and p.is_number:
        return exp.Literal.number(str(Decimal(1) - Decimal(p.this)))
    return exp.Paren(this=exp.Sub(this=exp.Literal.number(1), expression=p.copy()))


def _named(projection: Expr) -> Expr:
    """A bare column projection under its output name: it will read a row-level copy named otherwise."""
    if isinstance(projection, exp.Column) and isinstance(projection.this, exp.Identifier):
        return exp.Alias(this=projection, alias=projection.this.copy())
    return projection


def _row_reads(select: exp.Select, nodes: List[Expr], percentiles: List[Expr]) -> List[exp.Column]:
    """The columns outside the percentiles that read a row: an unqualified name reads a source column inside the
    projections, and may name an output outside them."""
    aliases = {e.alias for e in select.expressions if isinstance(e, exp.Alias)}
    in_projections = {id(d) for e in select.expressions for d in e.walk()}
    inside = {id(d) for p in percentiles for d in p.walk()}
    return [c for c in nodes if isinstance(c, exp.Column) and id(c) not in inside
            and (c.table or c.name not in aliases or id(c) in in_projections)]


def _split(select: exp.Select, *, columns: ColumnsFor) -> None:
    select.set("expressions", [_named(p) for p in select.expressions])
    group = select.args.get("group")
    partition = [_grouping_term(k, select) for k in group.expressions] if group is not None else []
    nodes = list(_own_nodes(select))
    if any(isinstance(n, exp.Star) and not isinstance(n.parent, exp.AggFunc) for n in nodes):
        raise ValueError("A percentile beside a * projection cannot be split into a window subquery")
    builders = [(n, b) for n in nodes if isinstance(n, exp.WithinGroup) and (b := columns(n)) is not None]
    reads = _row_reads(select, nodes, [n for n, _ in builders])
    names: Dict[Tuple[str, str], str] = {}
    projected: List[Expr] = []

    def read(name: str) -> exp.Column:
        return exp.Column(this=exp.to_identifier(name), table=exp.to_identifier(_ROWS))

    for column in reads:
        ident = (column.table, column.name)
        if ident not in names:
            names[ident] = f"{_COLUMN}{len(projected)}"
            projected.append(column.copy().as_(names[ident]))
    for node, builder in builders:
        p, ordered = node.this.this, node.expression.expressions[0]
        if ordered.args.get("desc"):  # the p-th value descending is the (1 − p)-th ascending
            p = _complement(p)
        windowed, pick = builder(p, ordered.this, partition)
        outer: List[exp.Column] = []
        for window in windowed:
            name = f"{_COLUMN}{len(projected)}"
            projected.append(window.as_(name))
            outer.append(read(name))
        node.replace(pick(outer))
    for column in reads:
        column.replace(read(names[(column.table, column.name)]))
    inner = exp.Select(expressions=projected, **{k: select.args[k] for k in ("from_", "joins", "where")
                                                 if select.args.get(k)})
    select.set("joins", None)
    select.set("where", None)
    select.set("from_", exp.From(this=exp.Subquery(this=inner, alias=exp.TableAlias(this=exp.to_identifier(_ROWS)))))
