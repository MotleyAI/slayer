## Context

See proposal.md — Why. A derived field's definition text is spliced into every reading position: the base
select's SELECT and GROUP BY (`generator._build_base_select_for_planned`), aggregate arguments
(`_build_agg_render_spec_from_planned`), the ranked CTE's PARTITION BY / ORDER BY (`_ranked_derived_field_expr`),
order keys, computed join keys (`ir/planned.SemiJoinHop.key_args` / `_computed_text`, rendered by
`_computed_key`), the generic renderer (`ScopeFrame.resolve`), and transitively through
`sql/field_expansion.expand_derived_refs_sync`. An inlined window inherits whatever row set it lands in, so
no position can be right in general. Every FROM, join target, semi-join hop and list parent already reaches
SQL through one relation door, `_emit_relation`, which wraps the backing relation as the dataset's filtered
rows (`_filtered_dataset`) and, for keyless association homes, adds a row number (`_numbered_relation`,
itself a window column over the filtered rows). The session policy rewrites physical tables after the
statement is built, so anything inside the door already sees policy-restricted rows.

Probe outcomes on `main` @ `a652279c7` (SQLite and DuckDB): every grouped position of a dialect-entry window
field emits invalid SQL; raw rows run but compute the window over the query-filtered rows and over a join's
row product; a derived field over it fails grouped; a window over it fails even raw. A portable (Mode B)
window field is refused by the engine save door and by every query, but accepted by the storage door.

Principles that bind the change: semantics Axiom 1 (amended), 2.1, 3.3, 9, 13, Laws 4, 6, 9; engine P1
(typed pipeline), P6 (placement from typing, never text at render time), P9 (algebra type errors raise in the
checker), P10 (dependency closures); sql P1 (AST end to end), P3 (one naming authority), P9 (fail closed),
P13 (one relation door); system P4, P11.

## Goals / Non-Goals

**Goals:**
- One place decides a windowed field's rows: the relation door. No reading position inlines a window.
- Windowed-ness is classified once, from the parsed runnable definition, and travels as typed data.
- The model-filter cycle walk learns one fact — a windowed column reads its own dataset — instead of a
  special refusal.

**Non-Goals:**
- Raw `OVER` written into a query filter, a field `filter`, dataset `filters` or metric text stays refused.
- Mode B / Ossie windowed fields (portable `sql`, `OSSIE_SQL_2026`, documents): DEV-2457.
- `ossie-l3` strict judging is unchanged.
- No exception-hierarchy change.

## Decisions

1. **Target 1, materialise.** A windowed field is legal and its query-filter refusal goes; 2.x, not gating
   2.0. (user, ruling on the issue)
2. **Dialect entries only here.** Portable and Ossie windowed fields stay refused until DEV-2457, which owns
   the Mode B window grammar, renderer, library matrix and Ossie window semantics; the storage save door
   refuses a portable windowed field with the same `IllegalWindowInFilterError` the engine door raises.
   Alternative weighed: make portable windows legal here — absorbs the field half of DEV-2457 and splits its
   acceptance. (user)
3. **Materialise in the relation door.** `_emit_relation` gains a wrapper after `_filtered_dataset` and
   before `_numbered_relation`:
   `(SELECT rows.*, <window> AS <alias> [, …] FROM σ_filters(M) AS rows [to-one joins]) AS <alias>`, one
   nested subquery per dependency depth, AST-built, aliases minted by `sql/naming.py` with the reserved
   prefix so they never meet a source column. Every reading position then reads a column. Alternative
   weighed: patch each position (base select, aggregate args, ranked CTE, order, join keys) — N code paths
   that must agree by hand, the bug class left standing. Alternative weighed: rewrite the dataset into a
   SQL-backed one — puts the dataset's filters outside the window (Axiom 3.3 violated), builds dialect SQL
   text (sql P1), and cannot carry to-one joins of the definition. (default: structural fix)
4. **Model filters: own dataset is a cycle, a joined windowed column is legal.** In the filter-cycle walk
   (`engine/field_dependency._references` / `_filtered_reads` / `_filter_cycle`) a windowed-column reference
   adds its owner to the dependency path, at whatever depth it is reached. A filter reading its own
   dataset's windowed column (directly or through derived columns) becomes the cycle `[A, A]`, refused by
   `DatasetFilterCycleError` — at both save doors and at query time (`generator.py:5806`) — whose message
   names the windowed column; a joined model's windowed column is legal unless the walk finds a real cycle.
   The windowed branch of `elaborate_env.validate_dataset_filter` is deleted. Alternative weighed: compute
   the window over the unfiltered table and allow the own-dataset filter — breaks Axiom 3.3. (user)
5. **Axiom 1 wording.** (user, approved wording) Inserted into `architecture/semantics.arc42.md` Axiom 1
   after the "Revisits are **frame-local**" sentence, before its tags:
   "A **windowed column** — one whose definition is a window function — is a function of its declaring dataset's rows (Axiom 3.3), evaluated over them before any join, query filter or aggregation reads it, and is then read as a column in every position; a dataset filter reading one reads that dataset (3.3)."
   plus the tag `[enforced: test:tests/test_dev2432_windowed_columns.py]` appended to Axiom 1's tag list.
6. **Windows run over σ_filters(M), never over query-filtered rows or a join's rows.** Raw-row queries that
   filter today change value. (default: forced by decision 5, Law 9 and Axiom 13)
7. **Materialise only the windowed fields the query reads.** A pure lowering choice (Law 6). (default)
8. **Only directly windowed fields materialise.** A derived field over one is an ordinary derived field
   whose expansion stops at the materialised column. (default)
9. **Layers, topologically ordered.** The plan carries, per relation occurrence, the windowed fields read
   and the windowed fields they read, ordered by dependency depth; each depth is one nested subquery (a
   sibling alias cannot be read in the same SELECT). Field cycles stay refused by the existing
   `DatasetFieldCycleError`; the row-identity wrapper sits above the last layer. (default; codex)
10. **A window reading to-one joined fields gets those joins inside its layer,** each joined relation
    emitted through the same door (filtered, materialised if needed). (default)
11. **Detection from the parsed runnable entry.** A dialect entry already parses as exactly one sqlglot
    expression at field construction; windowed-ness is "the runnable entry contains an `exp.Window` node",
    classified once at resolution (`engine/definitions._resolved_field`) and carried on the resolved field.
    The regex `has_window_function` remains only for the raw-`OVER` refusals of filter and metric text.
    Alternative weighed: the regex on the resolved text — matches `OVER (` inside string literals, and a
    false positive moves an expression (e.g. a correlated subquery) into a subquery, which is not
    value-neutral. (codex)
12. **Session policy beneath the window,** by construction (the policy rewrites physical tables), pinned by
    an executed test on the policy-enabled engine path. (default)
13. **A field's own `filter` stays a value mask around the materialised column.** The window is materialised
    unmasked; the consumer applies the ordinary `CASE WHEN` wrapper around the column, with the mask's own
    joined dependencies emitted as today. (default; codex)
14. **Typed computed join keys.** `SemiJoinHop`'s computed key strings become a typed key distinguishing a
    physical column, an ordinary computed key and a materialised windowed column; every `_JoinOps` consumer
    renders the materialised column for a windowed key, on either side of a join and in correlated
    semi-joins. (codex)
15. **Binding.** A windowed-field reference binds as a column of its dataset's materialised relation, never
    as an inline expansion; `binding._reject_windowed_field_sql` is removed. `IllegalWindowInFilterError`
    stays for raw `OVER` text. Dependency closures (engine P10) still include the definition's join paths,
    so input safety and attribution are unchanged. (default)
16. **Exception hierarchy unchanged.** `DatasetFilterCycleError` stays a `SlayerError, ValueError`;
    making it a `DatasetFilterError` (`QueryTypeError`) would put a checker-family error outside the checker
    (engine P9) and break `tests/test_dev2210_model_filter_cycles.py::test_error_family`. Reverses the
    subclass proposed in the Q2 discussion. (codex, user)
17. **Existing tests aligned with the plan.** (user)
    - `tests/test_sql_generator.py::TestWindowFunctionInFilter::test_filter_on_windowed_column_raises` → the
      filter is accepted and applies outside the materialised relation.
    - `tests/test_reference_semantics.py::…::test_filter_on_windowed_column_raises_with_actionable_message`
      → the query generates; the class docstring follows.
    - `tests/test_dev2210_model_filter_validation.py::…::test_filter_on_joined_windowed_column` → both doors
      save it.
    - `tests/test_dev2210_model_filter_validation.py::…::test_filter_on_windowed_column_through_derived_column`
      → `pytest.raises(DatasetFilterCycleError)`.
    - `tests/test_binding.py::TestWindowInFilter` → assert the portable field's refusal at resolution, same
      `IllegalWindowInFilterError`.
    - `tests/test_cross_model_derived_columns.py::test_dev1410_local_bare_ref_to_derived_inside_window_over_partition_inlined`
      → mechanical alias-spelling edit only, if the wrapper's inner alias changes `A.raw_a / 10`.
    - `tests/test_dev1450fix_model_filter_derived.py::test_windowed_derived_column_model_filter_raises` stays
      unchanged (`ValueError`, `match="window"`).

18. **Pinned scenario title.** OpenSpec cannot rename a scenario of a MODIFIED requirement, so
    `models/model-filters` › "Saving a filter on a joined windowed column is refused" keeps its title and
    now covers the cycle case; the accepted case is the new scenario "A filter on a joined windowed column
    is accepted and evaluated". (default)

## Risks / Trade-offs

- [Computed join keys and semi-join hops carry strings today] → decision 14 types them; tests cover source,
  target and both-sided windowed keys and a correlated semi-join.
- [The materialised wrapper changes SQL shape that some generator tests pin] → only the approved alignments
  (decision 17); any other contradiction stops for approval.
- [The database cannot push query filters through a window] → inherent to the semantics (decision 6); the
  wrapper is emitted only for datasets whose windowed fields the query reads (decision 7).
- [`expand_derived_refs_sync` inlines transitively from several renderers] → expansion stops at a windowed
  field in the one shared expansion door, so every renderer sees the column.
