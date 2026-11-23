"""The distinct projection of an untrusted to-one hop's target subtree (semantics Axiom 4).

A join across a provably but untrusted to-one hop is marked when emitted; :func:`project_untrusted_hops` then rewrites
each SELECT reading one so its subtree is read through ``SELECT DISTINCT <keys>, <dimension values> FROM <subtree>
WHERE <subtree conjuncts>``: a home row meets each distinct grouped value once, however many target rows repeat it.
Aggregate inputs read across the hop are projected as one matching value per grouped row (``ANY_VALUE``).
"""

from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Set, Tuple

from sqlglot import exp

UNTRUSTED_HOP = "slayer_untrusted_hop"
_DIMENSION_PREFIX = "_slayer_d"
_COLUMN_PREFIX = "_slayer_c"


def mark_untrusted(relation: exp.Expression) -> exp.Expression:
    """Mark ``relation`` as the target of an untrusted to-one hop."""
    relation.meta[UNTRUSTED_HOP] = True
    return relation


def project_untrusted_hops(statement: exp.Expression) -> exp.Expression:
    """``statement`` with every marked join read through its subtree's distinct projection."""
    for select in list(statement.find_all(exp.Select)):
        while _project_first(select):
            pass
    return statement


def _alias_of(relation: exp.Expression) -> str:
    return relation.alias_or_name


def _scope_columns(node: exp.Expression, shadowed: frozenset[str] = frozenset()) -> Iterator[exp.Column]:
    """The columns ``node`` reads in its own scope, plus a nested scope's correlated reads not shadowed by its sources."""
    if isinstance(node, exp.Column):
        if node.table and node.table not in shadowed:
            yield node
        return
    if isinstance(node, exp.Select):
        shadowed = shadowed | frozenset(_alias_of(s) for s in _sources(node))
    for child in node.iter_expressions():
        yield from _scope_columns(child, shadowed)


def _sources(select: exp.Select) -> List[exp.Expression]:
    from_ = select.args.get("from")
    out = [from_.this] if from_ is not None else []
    return out + [j.this for j in select.args.get("joins") or []]


def _tables(node: exp.Expression) -> Set[str]:
    return {c.table for c in _scope_columns(node)}


def _conjuncts(where: Optional[exp.Where]) -> List[exp.Expression]:
    if where is None:
        return []
    return list(where.this.flatten()) if isinstance(where.this, exp.And) else [where.this]


_NULL_TOLERANT = (exp.Is, exp.Coalesce, exp.Case, exp.If, exp.Not, exp.Nullif, exp.Exists)


def _pushable(conjunct: exp.Expression, subtree: Set[str]) -> bool:
    """A conjunct reading only the subtree that rejects its null-extended rows, so it may filter inside an inner join."""
    tables = _tables(conjunct)
    return bool(tables) and tables <= subtree and not any(conjunct.find_all(*_NULL_TOLERANT)) \
        and not isinstance(conjunct, _NULL_TOLERANT)


def _subtree_joins(joins: List[exp.Join], start: int) -> Tuple[Set[str], List[int]]:
    """The aliases of the marked join at ``start`` and of every later join reading only them."""
    subtree = {_alias_of(joins[start].this)}
    members = [start]
    for i in range(start + 1, len(joins)):
        alias = _alias_of(joins[i].this)
        reads = (_tables(joins[i].this) | _tables(joins[i].args.get("on") or exp.null())) - {alias}
        if reads and reads <= subtree:
            subtree.add(alias)
            members.append(i)
    return subtree, members


def _aggregating(select: exp.Select) -> bool:
    if select.args.get("group") is not None:
        return True
    return any(_outside_window(a) for e in select.expressions for a in e.find_all(exp.AggFunc))


def _outside_window(node: exp.Expression) -> bool:
    return not isinstance(node.parent, exp.Window)


def _is_input(column: exp.Column, select: exp.Select) -> bool:
    """Read inside an aggregate (not one computed over a window) of this SELECT."""
    node: Optional[exp.Expression] = column.parent
    while node is not None and node is not select:
        if isinstance(node, exp.Select):
            return False
        if isinstance(node, exp.AggFunc) and _outside_window(node):
            return True
        node = node.parent
    return False


def _project_first(select: exp.Select) -> bool:
    joins: List[exp.Join] = list(select.args.get("joins") or [])
    start = next((i for i, j in enumerate(joins) if j.this.meta.get(UNTRUSTED_HOP)), None)
    if start is None:
        return False
    _Projection(select=select, joins=joins, start=start).apply()
    return True


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
        self.projected: List[exp.Expression] = []

    def _outer_parts(self) -> Iterator[exp.Expression]:
        for key, value in self.select.args.items():
            if key in ("from", "joins", "where"):
                continue
            for v in value if isinstance(value, list) else [value]:
                if isinstance(v, exp.Expression):
                    yield v
        for j in self.outer_joins:
            if j is not self.root:
                yield j
        if self.select.args.get("from") is not None:
            yield self.select.args["from"]

    def apply(self) -> None:
        conjuncts = _conjuncts(self.select.args.get("where"))
        inside = [c for c in conjuncts if _pushable(c, self.subtree)]
        outside = [c for c in conjuncts if not any(c is i for i in inside)]
        dims = self._dimensions()
        self.select.set("where", exp.Where(this=exp.and_(*outside)) if outside else None)
        group: List[exp.Expression] = []
        inputs: List[exp.Expression] = []
        on = self.root.args.get("on")
        dim_names = {name for _, name in dims}
        reads = [c for part in [*self._outer_parts(), *self._where_parts()] for c in _scope_columns(part)
                 if c.table in self.subtree and not (c.table == self.alias and c.name in dim_names)]
        key_reads = [c for c in (_scope_columns(on) if on is not None else []) if c.table in self.subtree]
        input_cols = {(c.table, c.name) for c in reads if _is_input(c, self.select)}
        raw_cols = {(c.table, c.name) for c in [*reads, *key_reads] if not _is_input(c, self.select)}
        aggregating = _aggregating(self.select)
        for column in [*key_reads, *reads]:
            ident = (column.table, column.name)
            name = self._name(column)
            if ident not in self.names:
                self.names[ident] = name
                source = exp.Column(this=column.this.copy(), table=exp.to_identifier(column.table))
                if aggregating and ident in input_cols and ident not in raw_cols:
                    inputs.append(exp.AnyValue(this=source).as_(name))
                else:
                    group.append(source)
                    self.projected.append(source.as_(name))
            self._rename(column)
        for expr, name in dims:
            group.append(expr)
            self.projected.append(expr.as_(name))
        inner = exp.select(*self.projected, *inputs).from_(self.relation.copy())
        for j in self.inner_joins:
            inner.append("joins", j.copy())
        if inside:
            inner = inner.where(exp.and_(*(c.copy() for c in inside)))
        inner = inner.group_by(*(g.copy() for g in group)) if inputs else inner.distinct()
        self.relation.meta.pop(UNTRUSTED_HOP, None)
        projection = exp.Subquery(this=inner, alias=exp.TableAlias(this=exp.to_identifier(self.alias)))
        self.root.set("this", projection)
        if inside:
            self.root.set("side", None)
            self.root.set("kind", None)
        self.select.set("joins", self.outer_joins)

    def _where_parts(self) -> List[exp.Expression]:
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
        name = self.names[(column.table, column.name)]
        quoted = column.this.args.get("quoted", False) if name == column.name else False
        column.set("this", exp.to_identifier(name, quoted=quoted))
        column.set("table", exp.to_identifier(self.alias))

    def _dimensions(self) -> List[Tuple[exp.Expression, str]]:
        """Each grouped value read only from the subtree, computed inside and read back by name."""
        group = self.select.args.get("group")
        if group is None:
            return []
        out: List[Tuple[exp.Expression, str]] = []
        for i, g in enumerate(list(group.expressions)):
            tables = _tables(g)
            if not tables or not tables <= self.subtree or any(g.find_all(exp.AggFunc, exp.Window)):
                continue
            if isinstance(g, exp.Column) and g.table == self.alias:
                continue
            name = f"{_DIMENSION_PREFIX}{i}"
            out.append((g.copy(), name))
            replacement = exp.Column(this=exp.to_identifier(name), table=exp.to_identifier(self.alias))
            for part in list(self._outer_parts()):
                self._replace(part, g, replacement)
        return out

    @staticmethod
    def _replace(part: exp.Expression, target: exp.Expression, replacement: exp.Expression) -> None:
        for node in list(part.walk()):
            if node is not part and node.parent is not None and node == target:
                node.replace(replacement.copy())
