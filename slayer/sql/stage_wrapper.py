"""DEV-1452 Stage B — flat-rename wrapper for rendered stage SQL.

Extracted from ``slayer.sql.generator._stage_rename_wrapper`` (decision B
of the Stage B plan). Both the multi-stage CTE chaining in
``generate_planned_stages`` AND the migrated query-backed virtual-model
wrap in ``SlayerQueryEngine._expand_query_backed_model`` consume the
same flatten contract:

* Strip the ``<source_relation>.`` prefix from every ``named_select``.
* Replace remaining ``.`` with ``__`` to flatten the join-path encoding.
* Assert the produced flat names match the expected StageSchema columns
  exactly — silent divergence between the planner and generator surfaces
  here, not at a downstream bind miss.
"""
from __future__ import annotations

from typing import List, Sequence

import sqlglot
from sqlglot import exp

from slayer.sql.dialects import get_dialect
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


def unmangle_dotted_table_refs(node: exp.Expression) -> None:
    """Undo a BigQuery / T-SQL round-trip mis-parse (DEV-1824 hoist).

    Re-parsing a dotted result-key column splits its dots across the column's
    qualifier slots: a bare ``\\`orders.region\\``` becomes ``table=orders,
    this=region`` and a CTE-qualified ``_base.\\`orders.region\\``` becomes
    ``db=_base, table=orders, this=region``. Generated table references are never
    schema-qualified and always name a real FROM source in the column's OWN
    SELECT, so this repairs any column whose leading qualifier part is NOT such a
    source: that part (and the ones after it) are really dotted column-name
    segments. When the leading part IS a real source, only the parts after it
    fold back into the column. A no-op for every correctly-parsed AST.

    Scoped to the column's own SELECT deliberately: a wider scope would fold a
    dotted result key like ``\\`orders.region\\``` back into its qualifier
    whenever some OUTER query happens to have an ``orders`` source. Generated SQL
    has neither correlated outer references nor schema-qualified ``Column.sql``,
    so the two shapes that own-scope resolution would mishandle never arise; if
    one ever did, its qualifier would be folded into the column name."""
    for col in node.find_all(exp.Column):
        prefix = [
            p for p in (col.args.get(k) for k in ("catalog", "db", "table"))
            if isinstance(p, exp.Identifier)
        ]
        if not prefix or not isinstance(col.this, exp.Identifier):
            continue
        select = col.parent_select
        sources = _select_source_names(select) if select is not None else set()
        if prefix[0].name in sources:
            segments = [p.name for p in prefix[1:]] + [col.this.name]
            if len(segments) == 1:
                continue  # a plain <source>.<col> — nothing was split
            real_table: exp.Identifier | None = prefix[0]
        else:
            segments = [p.name for p in prefix] + [col.this.name]
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
    stage_sql: str,
    expected_columns: List[str],
    dialect: str,
    projection_aliases: Sequence[str] = (),
) -> exp.Expression:
    """Wrap ``stage_sql`` so its output columns are the flat downstream
    bind names a sibling stage (or the wrapped virtual model's outer
    callers) reference.

    The flat names come from ``named_selects`` on the parsed body — the
    actual rendered output aliases. The wrapper:

    1. Parses ``stage_sql`` with the given dialect.
    2. For each ``named_select`` ``X``, strips ``<source_relation>.`` if
       ``X`` begins with that prefix, then replaces remaining ``.`` with
       ``__``. The result is the flat name (e.g.
       ``"orders.customers.region"`` -> ``"customers__region"``).
    3. Emits ``SELECT "<orig>" AS <flat> ... FROM (<stage_sql>) AS _stage_inner``.
    4. Asserts ``sorted(produced) == sorted(expected_columns)`` — a
       planner/generator divergence (hidden hoist leak, multi-alias
       over-projection, ...) raises ``ValueError`` immediately rather
       than masking the issue as a downstream bind miss.

    ``projection_aliases`` (the render's canonical projection keys) marks
    ``stage_sql`` as a length-fitted render (DEV-1756): rendered names are
    decoded through it back to canonical for the schema match, and the
    emitted flat aliases are length-fitted too. Empty (the internal
    stage-CTE caller) keeps output canonical and behavior unchanged.
    """
    inner_alias = STAGE_INNER_ALIAS
    body = sqlglot.parse_one(stage_sql, dialect=dialect)
    # DEV-1824 — repair a BigQuery / T-SQL dotted-alias re-parse before wrapping,
    # so a hoisted producer's ``_base.`orders.region``` references stay bound.
    unmangle_dotted_table_refs(body)
    dialect_obj = get_dialect(dialect)
    raw_names = body.named_selects
    # DEV-1716: on BigQuery / T-SQL the rendered stage SQL carries alias-mangled
    # output names (``orders___status``); decode to the canonical dotted form for
    # the ``source_relation.`` prefix-strip + flat-name computation, but keep
    # referencing the ACTUAL rendered (mangled) name as the inner-column source.
    # ``decode_result_keys`` is identity for every non-mangling dialect and a
    # no-op on already-dotted names (multi-stage internal use), so this is safe
    # for both the query-backed-model wrap and the internal stage-CTE wrap.
    canonical_names = (
        list(dialect_obj.decode_result_keys(
            [dict.fromkeys(raw_names)], aliases=projection_aliases,
        )[0])
        if raw_names
        else []
    )
    # DEV-1713: strip the source-relation prefix + ``__``-flatten via the
    # naming module's single owner.
    produced = [
        flat_name(canonical, strip_relation=source_relation)
        for canonical in canonical_names
    ]
    if sorted(produced) != sorted(expected_columns):
        raise ValueError(
            f"stage {source_relation!r}: rendered output columns "
            f"{produced!r} do not match the expected schema "
            f"{expected_columns!r}.",
        )
    # ``alias_rewrite_map`` fits over-limit flats and raises on collisions.
    fit_map = dialect_obj.alias_rewrite_map(produced) if projection_aliases else {}
    select = exp.Select()
    for out_name, flat in zip(raw_names, produced):
        src = exp.Column(
            this=exp.to_identifier(out_name, quoted=True),
            table=exp.to_identifier(inner_alias),
        )
        select = select.select(
            exp.alias_(src, exp.to_identifier(fit_map.get(flat, flat), quoted=True)),
        )
    return select.from_(
        exp.Subquery(
            this=body,
            alias=exp.TableAlias(this=exp.to_identifier(inner_alias)),
        ),
    )


__all__ = ["build_flat_rename_wrapper"]
