"""Flat-rename wrapper over a composed stage statement.

Shared by the multi-stage CTE chaining, the regroup producer attach and the
query-backed virtual-model wrap: output columns become the flat downstream
bind names, checked against the expected stage schema.
"""
from __future__ import annotations

from typing import List, Optional

from sqlglot import exp

from slayer.sql.naming import STAGE_INNER_ALIAS, flat_name


def _select_source_names(select: exp.Select) -> set:
    """The relation names in ``select``'s own FROM / JOINs (not nested ones)."""
    names: set = set()
    # sqlglot stores the FROM clause under ``from`` or ``from_`` by version.
    frm = select.args.get("from") or select.args.get("from_")
    if frm is not None and frm.this is not None:
        names.add(frm.this.alias_or_name)
    for join in select.args.get("joins") or []:
        if join.this is not None:
            names.add(join.this.alias_or_name)
    return names


def _visible_source_names(select: exp.Select) -> set:
    """``select``'s own sources plus those of ancestor SELECTs reachable across
    expression-subquery boundaries — where SQL permits correlation (a semi-join
    EXISTS). A derived table hides its parent's own sources (siblings need
    LATERAL) but keeps climbing, so one nested in an EXISTS sees the EXISTS's
    ancestors (the semi-join spine); a CTE stops the climb."""
    names: set = set(_select_source_names(select))
    node: exp.Expression = select
    while True:
        parent = node.parent
        siblings_hidden = False
        while parent is not None and not isinstance(parent, exp.Select):
            if isinstance(parent, exp.CTE):
                return names
            if isinstance(parent, exp.Subquery) and isinstance(
                parent.parent, (exp.From, exp.Join),
            ):
                siblings_hidden = True
            node = parent
            parent = node.parent
        if parent is None:
            return names
        if not siblings_hidden:
            names |= _select_source_names(parent)
        node = parent


def _dot_segments(node: exp.Expression) -> Optional[List[str]]:  # pyright: ignore[reportPrivateImportUsage] — sqlglot re-export, pervasive in this file
    """Identifier names along a (nested) ``Dot`` chain; ``None`` for anything else."""
    if isinstance(node, exp.Identifier):
        return [node.name]
    if isinstance(node, exp.Dot):
        left, right = _dot_segments(node.this), _dot_segments(node.expression)
        return None if left is None or right is None else left + right
    return None


def unmangle_dotted_table_refs(node: exp.Expression) -> None:
    """Undo a BigQuery / T-SQL round-trip mis-parse (DEV-1824 hoist).

    Re-parsing a dotted result-key column splits its dots across the column's
    qualifier slots: a bare ``\\`orders.region\\``` becomes ``table=orders,
    this=region`` and a CTE-qualified ``_base.\\`orders.region\\``` becomes
    ``db=_base, table=orders, this=region``. Generated table references are never
    schema-qualified and always name a real FROM source visible to the column's
    SELECT, so this repairs any column whose leading qualifier part is NOT such a
    source: that part (and the ones after it) are really dotted column-name
    segments. When the leading part IS a real source, only the parts after it
    fold back into the column. A no-op for every correctly-parsed AST.

    Visibility is the column's own SELECT plus correlation-legal ancestors
    (:func:`_visible_source_names`): a semi-join EXISTS — and the spine derived
    table nested in it — correlates to the producer body's root alias, which
    must not be folded; a wider scope would fold a dotted result key like
    ``\\`orders.region\\``` back into its qualifier whenever some OUTER query
    happens to have an ``orders`` source."""
    for col in node.find_all(exp.Column):
        prefix = [
            p for p in (col.args.get(k) for k in ("catalog", "db", "table"))
            if isinstance(p, exp.Identifier)
        ]
        # Four-plus segments overflow the qualifier slots into a Dot in ``this``.
        this_segments = _dot_segments(col.this)
        if not prefix or this_segments is None:
            continue
        select = col.parent_select
        sources = _visible_source_names(select) if select is not None else set()
        if prefix[0].name in sources:
            segments = [p.name for p in prefix[1:]] + this_segments
            if len(segments) == 1:
                continue  # a plain <source>.<col> — nothing was split
            real_table: exp.Identifier | None = prefix[0]
        else:
            segments = [p.name for p in prefix] + this_segments
            real_table = None
        # Replace the node wholesale: clearing an existing column's ``table`` slot
        # leaves a dangling empty qualifier (`` ``.col ``) in this sqlglot.
        new_col = exp.Column(this=exp.to_identifier(".".join(segments), quoted=True))
        if real_table is not None:
            new_col.set(
                "table", exp.to_identifier(real_table.name, quoted=real_table.quoted),
            )
        col.replace(new_col)


def build_flat_rename_wrapper(
    *,
    source_relation: str,
    inner: exp.Select,
    expected_columns: List[str],
    dialect: str,
) -> exp.Select:
    """``SELECT <inner col> AS <flat name> ... FROM (<inner>) AS _stage_inner``, with ``inner``'s WITH hoisted.

    A flat name strips ``<source_relation>.`` and ``__``-flattens the rest
    (``orders.customers.region`` -> ``customers__region``); the produced names must
    equal ``expected_columns`` (a planner/generator divergence raises ``ValueError``).
    Names stay canonical: the statement's single finishing pass fits them for ``dialect``.
    """
    del dialect  # names are canonical here; fitting belongs to the finishing pass
    raw_names = inner.named_selects
    produced = [flat_name(name, strip_relation=source_relation) for name in raw_names]
    if sorted(produced) != sorted(expected_columns):
        raise ValueError(
            f"stage {source_relation!r}: rendered output columns "
            f"{produced!r} do not match the expected schema "
            f"{expected_columns!r}.",
        )
    with_ = inner.args.get("with_")
    if with_ is not None:
        inner.set("with_", None)
    select = exp.Select()
    for out_name, flat in zip(raw_names, produced):
        src = exp.Column(
            this=exp.to_identifier(out_name, quoted=True),
            table=exp.to_identifier(STAGE_INNER_ALIAS),
        )
        select = select.select(exp.alias_(src, exp.to_identifier(flat, quoted=True)))
    select = select.from_(
        exp.Subquery(
            this=inner,
            alias=exp.TableAlias(this=exp.to_identifier(STAGE_INNER_ALIAS)),
        ),
    )
    if with_ is not None:
        select.set("with_", with_)
    return select


__all__ = ["build_flat_rename_wrapper"]
