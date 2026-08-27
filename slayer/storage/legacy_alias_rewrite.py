"""DEV-1743 WP4 — rewrite legacy ``__`` split-alias qualifiers to dotted.

Pre-v9 stored models wrote Mode-A join qualifiers as ``__``-delimited aliases
(``customers__regions.name``). v9 makes them dotted-canonical
(``customers.regions.name``). The rewrite runs on load, on the raw dict before
Pydantic validation, so a legacy store keeps working after the flip.

Split cleanly into a sync AST half (here) and an async storage half (the caller
in ``StorageBackend``): :func:`extract_dunder_chains` finds candidate hop-chains
in a SQL string, the caller resolves which ones are real join walks against
sibling models, and :func:`apply_dunder_rewrite` rewrites only those — leaving
an unresolvable ``__`` (a CTE alias, a physical column) byte-verbatim.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp


def _dunder_chain_for_column(col: exp.Column) -> tuple[str, ...] | None:
    """Return the ``__``-split hop chain of ``col``'s qualifier, or ``None``.

    Only a single-identifier qualifier containing ``__`` qualifies — an
    already-dotted / schema-qualified reference (``a.b.leaf``) is never a
    legacy split-alias. A bare identifier (no leaf) is not a qualified
    reference and is skipped.
    """
    parts = list(col.parts)
    if len(parts) < 2:
        return None
    qualifier = parts[:-1]
    if len(qualifier) != 1:
        return None
    token = qualifier[0].name
    if "__" not in token:
        return None
    hops = token.split("__")
    if any(not h for h in hops):
        return None
    return tuple(hops)


def extract_dunder_chains(sql_text: str | None) -> set[tuple[str, ...]]:
    """Return every distinct ``__`` split-alias hop-chain in ``sql_text``.

    Parse failures yield the empty set — an unparseable legacy fragment is
    left untouched by the rewrite.
    """
    if not sql_text:
        return set()
    try:
        parsed = sqlglot.parse_one(sql_text)
    except Exception:
        return set()
    chains: set[tuple[str, ...]] = set()
    for col in parsed.find_all(exp.Column):
        chain = _dunder_chain_for_column(col)
        if chain is not None:
            chains.add(chain)
    return chains


def apply_dunder_rewrite(
    sql_text: str, *, resolvable: set[tuple[str, ...]],
) -> str:
    """Rewrite each resolvable ``__`` qualifier in ``sql_text`` to dotted.

    ``resolvable`` is the subset of :func:`extract_dunder_chains` the caller
    confirmed are real multi-hop join walks. Chains absent from it are left
    exactly as written. Returns ``sql_text`` byte-verbatim when nothing is
    rewritten (never re-serialises an untouched fragment).
    """
    if not resolvable:
        return sql_text
    try:
        parsed = sqlglot.parse_one(sql_text)
    except Exception:
        return sql_text
    root = parsed
    changed = False
    for col in list(parsed.find_all(exp.Column)):  # NOSONAR(S7504) — materialised before in-place col.replace mutation
        chain = _dunder_chain_for_column(col)
        if chain is None or chain not in resolvable:
            continue
        leaf_sql = col.this.sql()
        dotted = sqlglot.parse_one(f"{'.'.join(chain)}.{leaf_sql}")
        if col is root:
            root = dotted
        else:
            col.replace(dotted)
        changed = True
    return root.sql() if changed else sql_text


def _mode_a_surface_refs(data: dict):
    """Yield ``(container, key)`` for each Mode-A free-SQL string surface in a raw
    model dict — each column's ``sql`` / ``filter`` (container = the column dict)
    and each model-level filter (container = the ``filters`` list, key = index).
    ``container[key]`` reads and writes that surface. One traversal shared by the
    read (:func:`mode_a_surface_texts`) and the mutate
    (:func:`apply_dunder_rewrite_to_model_dict`) helpers so they can't drift."""
    columns = data.get("columns")
    if isinstance(columns, list):
        for col in columns:
            if not isinstance(col, dict):
                continue
            for key in ("sql", "filter"):
                if isinstance(col.get(key), str):
                    yield col, key
    filters = data.get("filters")
    if isinstance(filters, list):
        for i, f in enumerate(filters):
            if isinstance(f, str):
                yield filters, i


def mode_a_surface_texts(data: dict) -> list[str]:
    """Every Mode-A free-SQL string in a raw model dict: each column's ``sql``
    and ``filter``, plus the model-level ``filters``."""
    return [container[key] for container, key in _mode_a_surface_refs(data)]


def apply_dunder_rewrite_to_model_dict(
    data: dict, *, resolvable: set[tuple[str, ...]],
) -> None:
    """Rewrite every Mode-A surface in ``data`` in place — each column's ``sql`` /
    ``filter`` and the model ``filters`` — replacing resolvable legacy ``__`` join
    qualifiers with their dotted form."""
    for container, key in _mode_a_surface_refs(data):
        container[key] = apply_dunder_rewrite(container[key], resolvable=resolvable)
