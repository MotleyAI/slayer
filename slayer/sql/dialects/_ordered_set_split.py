"""``PERCENTILE_CONT … WITHIN GROUP`` on a database without it as a GROUP BY aggregate.

Each SELECT reading one is split in two: a row-level SELECT over the same FROM / WHERE computing, per row, the window
values the database has, partitioned by the GROUP BY keys; and a regrouping SELECT over it in which each percentile
becomes an aggregate of those values and every other column reads its row-level copy.
"""

from __future__ import annotations

from typing import Callable, Dict, List, Sequence, Tuple

from sqlglot import exp
from sqlglot.expressions.core import Expr, Expression

_ROWS = "__slayer_os"
_COLUMN = "__slayer_o"

#: ``(p, value, partition)`` → ``(row-level window columns, pick over their outer reads)``.
PercentileColumns = Callable[
    [Expr, Expr, Sequence[Expr]],
    Tuple[Sequence[Expr], Callable[[List[exp.Column]], Expr]],
]


def interpolated_percentile(p: Expr, value: Expr, partition: Sequence[Expr]
                            ) -> Tuple[Sequence[Expr], Callable[[List[exp.Column]], Expr]]:
    """Exact ``PERCENTILE_CONT`` from ``ROW_NUMBER`` / ``COUNT`` windows: the non-NULL values ranked per group,
    interpolated between ranks ``⌊h⌋`` and ``⌈h⌉`` at ``h = 1 + p·(n − 1)``."""
    is_null = exp.Case(ifs=[exp.If(this=exp.Is(this=value.copy(), expression=exp.Null()),
                                   true=exp.Literal.number(1))], default=exp.Literal.number(0))
    rank = exp.Window(this=exp.RowNumber(), partition_by=[*(k.copy() for k in partition), is_null],
                      order=exp.Order(expressions=[exp.Ordered(this=value.copy(), nulls_first=True)]))
    count = exp.Window(this=exp.Count(this=value.copy()), partition_by=[k.copy() for k in partition])

    def pick(reads: List[exp.Column]) -> Expr:
        rn, n, v = reads

        def position(size: Expression) -> Expression:
            return exp.Add(this=exp.Literal.number(1), expression=exp.Mul(
                this=p.copy(), expression=exp.Paren(this=exp.Sub(this=size, expression=exp.Literal.number(1)))))

        def at(rank_of: type[exp.Func]) -> Expression:
            hit = exp.and_(exp.EQ(this=rn.copy(), expression=rank_of(this=position(n.copy()))),
                           exp.Not(this=exp.Is(this=v.copy(), expression=exp.Null())))
            return exp.Max(this=exp.Case(ifs=[exp.If(this=hit, true=v.copy())]))

        h = position(exp.Max(this=n.copy()))
        fraction = exp.Paren(this=exp.Sub(this=h, expression=exp.Floor(this=h.copy())))
        low = at(exp.Floor)
        return exp.Add(this=low, expression=exp.Mul(
            this=fraction, expression=exp.Paren(this=exp.Sub(this=at(exp.Ceil), expression=low.copy()))))

    return [rank, count, value.copy()], pick


def is_percentile_cont(node: Expr) -> bool:
    return isinstance(node, exp.WithinGroup) and isinstance(node.this, exp.PercentileCont)


def split_percentile_selects(statement: Expression, *, columns: PercentileColumns) -> Expression:
    """``statement`` with every SELECT reading ``PERCENTILE_CONT … WITHIN GROUP`` split as above."""
    for select in list(statement.find_all(exp.Select)):  # NOSONAR(S7504) — each split edits the tree being walked
        if any(is_percentile_cont(n) for n in _own_nodes(select)):
            _split(select, columns=columns)
    return statement


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


def _split(select: exp.Select, *, columns: PercentileColumns) -> None:
    group = select.args.get("group")
    partition = [_grouping_term(k, select) for k in group.expressions] if group is not None else []
    aliases = {e.alias for e in select.expressions if isinstance(e, exp.Alias)}
    nodes = list(_own_nodes(select))
    if any(isinstance(n, exp.Star) and not isinstance(n.parent, exp.AggFunc) for n in nodes):
        raise ValueError("PERCENTILE_CONT beside a * projection cannot be split into a window subquery")
    percentiles = [n for n in nodes if is_percentile_cont(n)]
    inside = {id(d) for p in percentiles for d in p.walk()}
    reads = [c for c in nodes if isinstance(c, exp.Column) and id(c) not in inside
             and (c.table or c.name not in aliases)]
    names: Dict[Tuple[str, str], str] = {}
    projected: List[Expr] = []

    def read(name: str) -> exp.Column:
        return exp.Column(this=exp.to_identifier(name), table=exp.to_identifier(_ROWS))

    for column in reads:
        ident = (column.table, column.name)
        if ident not in names:
            names[ident] = f"{_COLUMN}{len(projected)}"
            projected.append(column.copy().as_(names[ident]))
    for node in percentiles:
        windowed, pick = columns(node.this.this, node.expression.expressions[0].this, partition)
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
