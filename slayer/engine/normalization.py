"""Stage 6 (DEV-1450) — slack normalization layer.

Rewrites tolerant-but-unambiguous agent input to canonical form before the
typed pipeline sees it, returning every rewrite as a typed
``NormalizationWarning`` (P0). Downstream stages never see the slack form.

Three seed rules:

- ``FUNC_STYLE_AGG`` (Mode B only): ``sum(revenue)`` / ``count(*)`` /
  ``percentile(amount, p=0.5)`` → colon syntax. Rewrites Mode-B fields
  (``ModelMeasure.formula``, ``SlayerQuery.measures[].formula``,
  ``SlayerQuery.filters`` entries).

- ``MISPLACED_MEASURE`` (query shape): bare column-looking entries in
  ``SlayerQuery.measures`` that resolve as a column (not a named
  ``ModelMeasure``) move to ``SlayerQuery.dimensions``. Mirrors the
  existing ``_auto_move_fields_to_dimensions`` heuristic but emits a
  structured warning.

- ``DOT_PATH_IN_SQL`` (Mode A only): sqlglot-AST ``Column`` node in root
  scope whose dotted path resolves to a join chain → ``__`` alias form
  (``customers.regions.name`` → ``customers__regions.name``). Stub in
  this stage; full AST-based, scope-aware implementation arrives in a
  follow-up.

Each rule emits a ``SlayerNormalizationWarning`` via ``warnings.warn(...)``
AND appends a ``NormalizationWarning`` payload to the returned result,
so REST / MCP / CLI consumers see the rewrite alongside the response and
``warnings.catch_warnings()`` callers see it via the standard channel.
"""

from __future__ import annotations

import re
import warnings as _warnings_module
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import BUILTIN_AGGREGATIONS
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.refs import IDENT_OR_PATH_RE
from slayer.core.warnings import NormalizationWarning, SlayerNormalizationWarning


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


class NormalizationResult(BaseModel):
    """Output of a normalization pass.

    ``query`` and ``model`` are either the same object the caller passed
    in (if no rewrite fired) or a new instance with the slack form
    rewritten. ``warnings`` lists one ``NormalizationWarning`` per
    rewrite — empty when the input was already canonical.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    query: Optional[SlayerQuery] = None
    model: Optional[SlayerModel] = None
    warnings: List[NormalizationWarning] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Rule: FUNC_STYLE_AGG
# ---------------------------------------------------------------------------


# Aggregation names that are also transform names — the rewrite only fires
# when the inner is a bare identifier, not when it's a colon-form aggregate.
_AMBIGUOUS_AGG_TRANSFORMS = frozenset({"first", "last"})

_STRING_LITERAL_RE = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")


def _find_balanced_close(s: str, open_idx: int) -> int:
    depth = 0
    in_string = False
    string_ch = ""
    i = open_idx
    while i < len(s):
        ch = s[i]
        if in_string:
            if ch == string_ch:
                # Handle '' / "" escapes.
                if i + 1 < len(s) and s[i + 1] == string_ch:
                    i += 2
                    continue
                in_string = False
        elif ch in ("'", '"'):
            in_string = True
            string_ch = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _split_args(s: str) -> List[str]:
    parts: List[str] = []
    depth = 0
    in_string = False
    string_ch = ""
    current: List[str] = []
    for ch in s:
        if in_string:
            current.append(ch)
            if ch == string_ch:
                in_string = False
            continue
        if ch in ("'", '"'):
            in_string = True
            string_ch = ch
            current.append(ch)
            continue
        if ch == "(":
            depth += 1
            current.append(ch)
            continue
        if ch == ")":
            depth -= 1
            current.append(ch)
            continue
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
            continue
        current.append(ch)
    if current:
        parts.append("".join(current))
    return [p.strip() for p in parts if p.strip()]


def _apply_func_style_agg(
    formula: str,
    *,
    location: str,
    custom_agg_names: Optional[frozenset[str]] = None,
) -> tuple[str, List[NormalizationWarning]]:
    """Rewrite function-style aggregations in ``formula`` to colon syntax.

    Returns ``(rewritten_formula, warnings)`` — ``warnings`` is empty when
    nothing changed.
    """
    agg_names = BUILTIN_AGGREGATIONS | (custom_agg_names or frozenset())
    sorted_names = sorted(agg_names, key=len, reverse=True)
    pattern = re.compile(
        r"(?<!:)\b(" + "|".join(re.escape(n) for n in sorted_names) + r")\("
    )

    emitted: List[NormalizationWarning] = []
    max_iterations = 50
    for _ in range(max_iterations):
        literal_spans = [
            (m.start(), m.end()) for m in _STRING_LITERAL_RE.finditer(formula)
        ]

        search_start = 0
        rewritten = False
        while search_start < len(formula):
            match = pattern.search(formula, search_start)
            if not match:
                break

            if any(start <= match.start() < end for start, end in literal_spans):
                search_start = match.end()
                continue

            agg_name = match.group(1)
            open_paren = match.end() - 1
            close_paren = _find_balanced_close(formula, open_paren)
            if close_paren < 0:
                search_start = match.end()
                continue

            inner = formula[open_paren + 1:close_paren].strip()

            if agg_name in _AMBIGUOUS_AGG_TRANSFORMS and ":" in inner:
                search_start = close_paren + 1
                continue

            parts = _split_args(inner)
            if not parts:
                search_start = close_paren + 1
                continue

            first_arg = parts[0]
            if first_arg == "*":
                measure = "*"
            elif IDENT_OR_PATH_RE.fullmatch(first_arg):
                measure = first_arg
            else:
                search_start = close_paren + 1
                continue

            remaining = parts[1:]
            if remaining:
                replacement = f"{measure}:{agg_name}({', '.join(remaining)})"
            else:
                replacement = f"{measure}:{agg_name}"

            original_slice = formula[match.start():close_paren + 1]
            payload = NormalizationWarning(
                rule_id="FUNC_STYLE_AGG",
                original=original_slice,
                normalized=replacement,
                location=location,
                rule_doc_url="docs/agent_input_slack.md#func-style-agg",
            )
            emitted.append(payload)
            _warnings_module.warn(
                SlayerNormalizationWarning(payload), stacklevel=2,
            )

            formula = formula[:match.start()] + replacement + formula[close_paren + 1:]
            rewritten = True
            break

        if not rewritten:
            break

    return formula, emitted


# ---------------------------------------------------------------------------
# Rule: MISPLACED_MEASURE
# ---------------------------------------------------------------------------


def _apply_misplaced_measure(
    query: SlayerQuery,
    *,
    model: Optional[SlayerModel],
) -> tuple[SlayerQuery, List[NormalizationWarning]]:
    """Move bare (no-colon, no-function) entries from ``query.measures`` to
    ``query.dimensions`` when they name a column on the model that isn't
    a ``ModelMeasure`` formula.

    Mirrors the existing ``_auto_move_fields_to_dimensions`` heuristic but
    emits a structured warning. When ``model`` is None we can't classify,
    so the rule is a no-op.
    """
    if not query.measures or model is None:
        return query, []

    measure_formula_names = {m.name for m in model.measures}
    column_names = {c.name for c in model.columns}

    new_measures = list(query.measures)
    moved_dim_strings: List[str] = []
    emitted: List[NormalizationWarning] = []

    kept: List = []
    for i, m in enumerate(new_measures):
        formula = getattr(m, "formula", None)
        if not isinstance(formula, str):
            kept.append(m)
            continue
        if ":" in formula or "(" in formula:
            kept.append(m)
            continue
        # Bare token. If it names a known ModelMeasure formula, keep it as
        # a measure. If it names a column on the model, move to dimensions.
        bare = formula.strip()
        if bare in measure_formula_names:
            kept.append(m)
            continue
        if bare in column_names:
            moved_dim_strings.append(bare)
            emitted.append(NormalizationWarning(
                rule_id="MISPLACED_MEASURE",
                original=bare,
                normalized=f"dimensions += {bare!r}",
                location=f"measures[{i}].formula",
                rule_doc_url="docs/agent_input_slack.md#misplaced-measure",
            ))
            _warnings_module.warn(
                SlayerNormalizationWarning(emitted[-1]), stacklevel=2,
            )
            continue
        # Unknown bare token — leave for downstream resolver to error on.
        kept.append(m)

    if not emitted:
        return query, []

    existing_dims = list(query.dimensions or [])
    # Append each moved bare column name as a dimension entry. We add as
    # plain strings since SlayerQuery.dimensions accepts string entries
    # alongside ColumnRefs (the pydantic union validators handle the
    # coercion).
    new_dimensions = existing_dims + moved_dim_strings
    return (
        query.model_copy(update={"measures": kept, "dimensions": new_dimensions}),
        emitted,
    )


# ---------------------------------------------------------------------------
# Rule: DOT_PATH_IN_SQL (stub for stage 6 — full implementation deferred)
# ---------------------------------------------------------------------------


def _apply_dot_path_in_sql(
    sql_text: str, *, location: str, model: Optional[SlayerModel],
) -> tuple[str, List[NormalizationWarning]]:
    """Stage-6 stub.

    The full implementation walks the sqlglot AST and rewrites
    root-scope dotted refs (``customers.regions.name``) to the ``__``
    alias form (``customers__regions.name``) when the leading segment
    is a known join target. Scope-aware (skips subqueries / CTE-local
    aliases / set-op branches), AST-based, reuses the
    ``column_expansion.py`` precedent.

    Stage 6 ships the slot so downstream wiring can be tested; the
    rewrite itself lands in a later commit (tracked alongside the
    `_rewrite_funcstyle_aggregations` deletion in stage 7b).
    """
    return sql_text, []


# ---------------------------------------------------------------------------
# Top-level entry points
# ---------------------------------------------------------------------------


def normalize_query(
    query: SlayerQuery,
    *,
    model: Optional[SlayerModel] = None,
    custom_agg_names: Optional[frozenset[str]] = None,
) -> NormalizationResult:
    """Apply all enabled slack rules to a ``SlayerQuery``.

    Returns the (possibly rewritten) query and the structured warnings.
    Existing in-tree rewriters (notably
    ``slayer.core.formula._rewrite_funcstyle_aggregations``) continue to
    run during enrichment; in stage 6 they see canonical input and
    silently no-op for any input this layer already rewrote.
    """
    all_warnings: List[NormalizationWarning] = []

    # Rule 1: FUNC_STYLE_AGG over Mode-B fields.
    new_measures = []
    for i, m in enumerate(query.measures or []):
        formula = getattr(m, "formula", None)
        if isinstance(formula, str):
            rewritten, ws = _apply_func_style_agg(
                formula,
                location=f"measures[{i}].formula",
                custom_agg_names=custom_agg_names,
            )
            all_warnings.extend(ws)
            if rewritten != formula:
                m = m.model_copy(update={"formula": rewritten})
        new_measures.append(m)

    new_filters: List[str] = []
    for i, f in enumerate(query.filters or []):
        if isinstance(f, str):
            rewritten, ws = _apply_func_style_agg(
                f,
                location=f"filters[{i}]",
                custom_agg_names=custom_agg_names,
            )
            all_warnings.extend(ws)
            new_filters.append(rewritten)
        else:
            new_filters.append(f)

    query = query.model_copy(update={
        "measures": new_measures,
        "filters": new_filters,
    })

    # Rule 2: MISPLACED_MEASURE.
    query, ws = _apply_misplaced_measure(query, model=model)
    all_warnings.extend(ws)

    # Rule 3: DOT_PATH_IN_SQL (stub in stage 6).
    # Mode-A fields on the query itself are rare — most Mode-A lives on
    # the model. Wiring is preserved so future activations need no
    # plumbing changes.

    return NormalizationResult(query=query, warnings=all_warnings)


def normalize_model(model: SlayerModel) -> NormalizationResult:
    """Apply slack rules to a ``SlayerModel`` before persistence.

    Mode-A rewrites (``DOT_PATH_IN_SQL``) target ``Column.sql``,
    ``Column.filter``, and ``SlayerModel.filters``. Mode-B rewrites
    (``FUNC_STYLE_AGG``) target ``ModelMeasure.formula``. The rewrite
    semantics match ``normalize_query``.
    """
    all_warnings: List[NormalizationWarning] = []

    # FUNC_STYLE_AGG on ModelMeasure.formula entries.
    if model.measures:
        custom_names = frozenset(a.name for a in (model.aggregations or []))
        new_measures = []
        for i, mm in enumerate(model.measures):
            formula = mm.formula
            rewritten, ws = _apply_func_style_agg(
                formula,
                location=f"measures[{i}].formula",
                custom_agg_names=custom_names,
            )
            all_warnings.extend(ws)
            if rewritten != formula:
                mm = mm.model_copy(update={"formula": rewritten})
            new_measures.append(mm)
        model = model.model_copy(update={"measures": new_measures})

    # DOT_PATH_IN_SQL on Mode-A fields — stub call kept for wiring symmetry.
    _, _ = _apply_dot_path_in_sql("", location="(model)", model=model)

    return NormalizationResult(model=model, warnings=all_warnings)
