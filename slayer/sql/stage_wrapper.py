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

from typing import List

import sqlglot
from sqlglot import exp

from slayer.sql.dialects import get_dialect
from slayer.sql.naming import STAGE_INNER_ALIAS, flat_name


def build_flat_rename_wrapper(
    *,
    source_relation: str,
    stage_sql: str,
    expected_columns: List[str],
    dialect: str,
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
    """
    inner_alias = STAGE_INNER_ALIAS
    body = sqlglot.parse_one(stage_sql, dialect=dialect)
    select = exp.Select()
    produced: List[str] = []
    raw_names = body.named_selects
    # DEV-1716: on BigQuery / T-SQL the rendered stage SQL carries alias-mangled
    # output names (``orders___status``); decode to the canonical dotted form for
    # the ``source_relation.`` prefix-strip + flat-name computation, but keep
    # referencing the ACTUAL rendered (mangled) name as the inner-column source.
    # ``decode_result_keys`` is identity for every non-mangling dialect and a
    # no-op on already-dotted names (multi-stage internal use), so this is safe
    # for both the query-backed-model wrap and the internal stage-CTE wrap.
    canonical_names = (
        list(get_dialect(dialect).decode_result_keys([dict.fromkeys(raw_names)])[0])
        if raw_names
        else []
    )
    for out_name, canonical in zip(raw_names, canonical_names):
        # DEV-1713: strip the source-relation prefix + ``__``-flatten via the
        # naming module's single owner.
        flat = flat_name(canonical, strip_relation=source_relation)
        produced.append(flat)
        src = exp.Column(
            this=exp.to_identifier(out_name, quoted=True),
            table=exp.to_identifier(inner_alias),
        )
        select = select.select(
            exp.alias_(src, exp.to_identifier(flat, quoted=True)),
        )
    if sorted(produced) != sorted(expected_columns):
        raise ValueError(
            f"stage {source_relation!r}: rendered output columns "
            f"{produced!r} do not match the expected schema "
            f"{expected_columns!r}.",
        )
    return select.from_(
        exp.Subquery(
            this=body,
            alias=exp.TableAlias(this=exp.to_identifier(inner_alias)),
        ),
    )


__all__ = ["build_flat_rename_wrapper"]
