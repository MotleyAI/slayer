"""The distinct projection of an untrusted to-one hop's target subtree (semantics Axiom 4).

A join across a provably but untrusted to-one hop is marked when emitted; :func:`project_untrusted_hops` then rewrites
each SELECT reading one so its subtree is read through ``SELECT DISTINCT <keys>, <dimension values> FROM <subtree>
WHERE <subtree conjuncts>``: a home row meets each distinct grouped value once, however many target rows repeat it.
Any other column the statement reads across the hop comes from one representative row of each grouped value.
"""

from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Set, Tuple, TypeVar

from sqlglot import exp
from sqlglot.expressions.core import Expr

from slayer.sql.zoned_reads import READ_TAG

UNTRUSTED_HOP = "slayer_untrusted_hop"
# Under the save-time reserved ``__slayer_`` prefix: no column or field read through the projection can collide.
_DIMENSION_PREFIX = "__slayer_d"
_COLUMN_PREFIX = "__slayer_c"
_PREDICATE = "__slayer_p"
_MATCHED = "__slayer_m"
_ONE = "__slayer_q"
_ROW = "__slayer_r"
_CARRIED = "slayer_carried"


_E = TypeVar("_E", bound=Expr)


def mark_untrusted(relation: _E) -> _E:
    """Mark ``relation`` as the target of an untrusted to-one hop."""
    relation.meta[UNTRUSTED_HOP] = True
    return relation


def project_untrusted_hops(statement: _E) -> _E:
    """``statement`` with every marked join read through its subtree's distinct projection."""
    for select in list(statement.find_all(exp.Select)):  # NOSONAR(S7504) — each rewrite edits the tree being walked
        _project_select(select)
    return statement


def _alias_of(relation: Expr) -> str:
    return relation.alias_or_name


def _ident(column: exp.Column, name: str) -> exp.Identifier:
    """``name`` quoted as ``column`` is when it keeps the column's name, so a case-folding backend resolves it."""
    return exp.to_identifier(name, quoted=bool(column.this.args.get("quoted")) and name == column.name)


def _scope_columns(node: Expr, shadowed: frozenset[str] = frozenset()) -> Iterator[exp.Column]:
    """The columns ``node`` reads in its own scope, plus a nested scope's correlated reads not shadowed by its sources."""
    if isinstance(node, exp.Column):
        if node.table and node.table not in shadowed:
            yield node
        return
    if isinstance(node, exp.Select):
        shadowed = shadowed | frozenset(_alias_of(s) for s in _sources(node))
    for child in node.iter_expressions():
        yield from _scope_columns(child, shadowed)


def _sources(select: exp.Select) -> List[Expr]:
    from_ = select.args.get("from")
    out = [from_.this] if from_ is not None else []
    return out + [j.this for j in select.args.get("joins") or []]


def _tables(node: Expr) -> Set[str]:
    return {c.table for c in _scope_columns(node)}


def _conjuncts(where: Optional[exp.Where]) -> List[Expr]:
    if where is None:
        return []
    return list(where.this.flatten()) if isinstance(where.this, exp.And) else [where.this]


_STRICT_BINARY = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Like, exp.ILike)


_STRICT_ARITHMETIC = (exp.Add, exp.Sub, exp.Mul, exp.Div)


def _subtree_column(node: Optional[Expr], subtree: Set[str]) -> bool:
    """``node`` is NULL whenever the subtree's columns are: a subtree column under casts and arithmetic."""
    while isinstance(node, (exp.Paren, exp.Cast, exp.Neg)):
        node = node.this
    if isinstance(node, _STRICT_ARITHMETIC):
        return _subtree_column(node.this, subtree) or _subtree_column(node.expression, subtree)
    return isinstance(node, exp.Column) and node.table in subtree


def _rejects_nulls(predicate: Expr, subtree: Set[str]) -> bool:
    """Proven not true on a row whose subtree columns are all NULL; shapes outside this allowlist are not proven."""
    parts = predicate.flatten() if isinstance(predicate, exp.And) else [predicate]
    return any(_strict_part(p, subtree) for p in parts)


def _strict_part(part: Expr, subtree: Set[str]) -> bool:
    if isinstance(part, exp.Paren):
        return _rejects_nulls(part.this, subtree)
    if isinstance(part, exp.Not) and isinstance(part.this, exp.Is) and isinstance(part.this.expression, exp.Null):
        return _subtree_column(part.this.this, subtree)
    if isinstance(part, (exp.In, exp.Between)):
        return _subtree_column(part.this, subtree)
    if isinstance(part, _STRICT_BINARY):
        return _subtree_column(part.this, subtree) or _subtree_column(part.expression, subtree)
    return False


def _carried(expr: Expr, subtree: Set[str]) -> Expr:
    """A copy of ``expr`` whose subtree columns are carried from the representative row, never grouped: on a home row
    the join null-extends they read its null-extended values, as after the subtree's joins."""
    out = expr.copy()
    for column in _scope_columns(out) if not isinstance(out, exp.Column) else [out]:
        if column.table in subtree:
            column.meta[_CARRIED] = True
    return out


def _is_left(join: exp.Join) -> bool:
    return (join.args.get("side") or "").upper() == "LEFT"


def _subtree_joins(joins: List[exp.Join], start: int) -> Tuple[Set[str], List[int]]:
    """The aliases of the marked join at ``start`` and of every later join reading only them whose condition rejects
    their null-extended rows, so it matches the same rows inside the projection as after it."""
    subtree = {_alias_of(joins[start].this)}
    members = [start]
    for i in range(start + 1, len(joins)):
        alias = _alias_of(joins[i].this)
        on = joins[i].args.get("on")
        reads = (_tables(joins[i].this) | (_tables(on) if on is not None else set())) - {alias}
        if reads and reads <= subtree and (on is None or _rejects_nulls(on, subtree)):
            subtree.add(alias)
            members.append(i)
    return subtree, members


def _project_select(select: exp.Select) -> None:
    while True:
        joins: List[exp.Join] = list(select.args.get("joins") or [])
        start = next((i for i, j in enumerate(joins) if j.this.meta.get(UNTRUSTED_HOP)), None)
        if start is None:
            return
        _Projection(select=select, joins=joins, start=start).apply()


class _Projection:
    """One marked join's rewrite inside ``select``."""

    def __init__(self, *, select: exp.Select, joins: List[exp.Join], start: int) -> None:
        self.select = select
        self.joins = joins
        self.root = joins[start]
        self.relation = self.root.this
        self.alias = _alias_of(self.relation)
        self.subtree, members = _subtree_joins(joins, start)
        self.inner_joins = [joins[i] for i in members[1:]]
        self.outer_joins = [j for i, j in enumerate(joins) if i not in members[1:]]
        self.names: Dict[Tuple[str, str], str] = {}
        self.projected: List[Expr] = []

    def _outer_parts(self) -> Iterator[Expr]:
        for key, value in self.select.args.items():
            if key in ("from", "joins", "where"):
                continue
            for v in value if isinstance(value, list) else [value]:
                if isinstance(v, Expr):
                    yield v
        for j in self.outer_joins:
            if j is not self.root:
                yield j
        if self.select.args.get("from") is not None:
            yield self.select.args["from"]

    def apply(self) -> None:
        inside, valued, outside = self._split_where()
        # A home row with no subtree row a filter or an inner member keeps is dropped, as after the subtree's joins.
        inner_only = bool(inside) or not all(_is_left(j) for j in self.inner_joins)
        null_extended = _is_left(self.root) and not inner_only
        dims = self._dimensions(null_extended=null_extended)
        holds, kept = self._valued_filter(valued, null_extended=null_extended)
        where = [*outside, kept] if kept is not None else outside
        self.select.set("where", exp.Where(this=exp.and_(*where)) if where else None)
        group, other = self._project_reads(dim_names={name for _, name in dims} | {_PREDICATE, _MATCHED})
        for expr, name in dims:
            group.append(expr)
            self.projected.append(expr.as_(name))
        if holds is not None:
            group.append(holds)
            self.projected.append(holds.copy().as_(_PREDICATE))
        inner = self._inner(inside=inside, group=group, other=other)
        self.relation.meta.pop(UNTRUSTED_HOP, None)
        self.root.set("this", exp.Subquery(this=inner, alias=exp.TableAlias(this=exp.to_identifier(self.alias))))
        if inner_only:
            self.root.set("side", None)
            self.root.set("kind", None)
        self.select.set("joins", self.outer_joins)

    def _inner(self, *, inside: List[Expr], group: List[Expr], other: List[Expr]) -> exp.Select:
        """One row per distinct grouped value; the other reads taken from one representative subtree row of each."""
        body = exp.select(*self.projected, *other).from_(self.relation.copy())
        for j in self.inner_joins:
            body.append("joins", j.copy())
        if inside:
            body = body.where(exp.and_(*(c.copy() for c in inside)))
        if not other:
            return body.distinct()
        # Any row will do; an ORDER BY over the grouped values (comparable by construction) satisfies dialects that
        # require one without ordering by a value whose type may have no ordering.
        order = [g.copy() for g in group] or [exp.Subquery(this=exp.select(exp.Null()))]
        rank = exp.Window(this=exp.RowNumber(), partition_by=[g.copy() for g in group],
                          order=exp.Order(expressions=[exp.Ordered(this=o) for o in order]))
        numbered = exp.Subquery(this=body.select(rank.as_(_ROW)), alias=exp.TableAlias(this=exp.to_identifier(_ONE)))
        names = [exp.Column(this=p.args["alias"].copy(), table=exp.to_identifier(_ONE)) for p in [*self.projected, *other]]
        first = exp.EQ(this=exp.Column(this=exp.to_identifier(_ROW), table=exp.to_identifier(_ONE)),
                       expression=exp.Literal.number(1))
        return exp.select(*names).from_(numbered).where(first)

    def _split_where(self) -> Tuple[List[Expr], List[Expr], List[Expr]]:
        """The WHERE conjuncts as (filtering inside, evaluated inside as a value, left outside)."""
        conjuncts = _conjuncts(self.select.args.get("where"))
        own = [c for c in conjuncts if (tables := _tables(c)) and tables <= self.subtree]
        inside = [c for c in own if _rejects_nulls(c, self.subtree)]
        valued = [c for c in own if not any(c is i for i in inside)]
        return inside, valued, [c for c in conjuncts if not any(c is o for o in own)]

    def _grouping_parts(self) -> List[Expr]:
        """The parts whose subtree reads tell rows apart: the join keys, the outer filter and grouping, and the
        condition of every other join a grouped value reads through."""
        grouping = [self.select.args.get("where"), self.select.args.get("group")]
        if self.select.args.get("distinct") is not None:
            grouping.extend(self.select.expressions)
        parts = [p for p in [self.root.args.get("on"), *grouping] if p is not None]
        grouped = set().union(*(_tables(p) for p in parts[1:])) if len(parts) > 1 else set()
        pending = [j for j in self.outer_joins if j is not self.root and j.args.get("on") is not None]
        while reached := [j for j in pending if _alias_of(j.this) in grouped]:
            for j in reached:
                parts.append(j.args["on"])
                grouped |= _tables(j.args["on"])
            pending = [j for j in pending if not any(j is r for r in reached)]
        return parts

    def _project_reads(self, *, dim_names: Set[str]) -> Tuple[List[Expr], List[Expr]]:
        """Project every subtree column the statement reads, renaming each read; (grouped values, the other reads'
        projections)."""
        def own(part: Expr) -> List[exp.Column]:
            return [c for c in _scope_columns(part)
                    if c.table in self.subtree and not (c.table == self.alias and c.name in dim_names)]

        grouped = {(c.table, c.name) for part in self._grouping_parts() for c in own(part) if not c.meta.get(_CARRIED)}
        on = self.root.args.get("on")
        reads = [c for part in [*([on] if on is not None else []), *self._outer_parts(), *self._where_parts()]
                 for c in own(part)]
        group: List[Expr] = []
        other: List[Expr] = []
        for column in reads:
            ident = (column.table, column.name)
            if ident not in self.names:
                self.names[ident] = self._name(column)
                source = exp.Column(this=column.this.copy(), table=exp.to_identifier(column.table))
                if READ_TAG in column.meta:  # the read moved inside keeps how the read path read it
                    source.meta[READ_TAG] = column.meta[READ_TAG]
                aliased = source.as_(_ident(column, self.names[ident]))
                if ident in grouped:
                    group.append(source)
                    self.projected.append(aliased)
                else:
                    other.append(aliased)
            self._rename(column)
        return group, other

    def _valued_filter(self, valued: List[Expr], *, null_extended: bool) -> Tuple[Optional[Expr], Optional[Expr]]:
        """``valued`` as a value of each subtree row, and the outer condition keeping a matched row it holds on and,
        when the join can null-extend, an unmatched home row it holds on as after the subtree's joins."""
        if not valued:
            return None, None
        holds = exp.Case(ifs=[exp.If(this=exp.and_(*(c.copy() for c in valued)), true=exp.Literal.number(1))])
        kept: Expr = exp.EQ(this=self._read(_PREDICATE), expression=exp.Literal.number(1))
        if not null_extended:
            return holds, kept
        return holds, exp.or_(kept, exp.and_(self._unmatched(), *(_carried(c, self.subtree) for c in valued)))

    def _unmatched(self) -> Expr:
        """True on a home row the subtree null-extends; projects the match marker once."""
        if not any(p.alias == _MATCHED for p in self.projected):
            self.projected.append(exp.Literal.number(1).as_(_MATCHED))
        return exp.Is(this=self._read(_MATCHED), expression=exp.Null())

    def _read(self, name: str) -> exp.Column:
        return exp.Column(this=exp.to_identifier(name), table=exp.to_identifier(self.alias))

    def _where_parts(self) -> List[Expr]:
        where = self.select.args.get("where")
        return [where] if where is not None else []

    def _name(self, column: exp.Column) -> str:
        ident = (column.table, column.name)
        if ident in self.names:
            return self.names[ident]
        if column.table == self.alias and column.name not in self.names.values():
            return column.name
        return f"{_COLUMN_PREFIX}{len(self.names)}"

    def _rename(self, column: exp.Column) -> None:
        column.set("this", _ident(column, self.names[(column.table, column.name)]))
        column.set("table", exp.to_identifier(self.alias))

    def _dimensions(self, *, null_extended: bool) -> List[Tuple[Expr, str]]:
        """Each grouped value read only from the subtree, computed inside and read back by name; on a null-extended
        home row one not proven NULL there reads as computed on NULLs, as after the subtree's joins."""
        group = self.select.args.get("group")
        if group is None:
            return []
        out: List[Tuple[Expr, str]] = []
        for i, g in enumerate(list(group.expressions)):
            tables = _tables(g)
            if not tables or not tables <= self.subtree or any(g.find_all(exp.AggFunc, exp.Window)):
                continue
            if isinstance(g, exp.Column) and g.table == self.alias:
                continue
            name = f"{_DIMENSION_PREFIX}{i}"
            out.append((g.copy(), name))
            replacement: Expr = self._read(name)
            if null_extended and not _subtree_column(g, self.subtree):
                replacement = exp.Case(ifs=[exp.If(this=self._unmatched(), true=_carried(g, self.subtree))],
                                       default=replacement)
            for part in self._outer_parts():
                self._replace(part, g, replacement)
        return out

    @staticmethod
    def _replace(part: Expr, target: Expr, replacement: Expr) -> None:
        for node in list(part.walk()):  # NOSONAR(S7504) — the loop replaces nodes of the tree being walked
            if node is not part and node.parent is not None and node == target:
                node.replace(replacement.copy())
