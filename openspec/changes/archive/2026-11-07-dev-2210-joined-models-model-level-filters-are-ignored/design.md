## Context

See proposal.md › Why. Today "a model's rows" lives in two places:

- `SQLGenerator._emit_relation` (`slayer/sql/generator.py`) is already the only site a model enters SQL — root FROM (`_build_from_clause_from_planned`), hop join (`_hop_join`), semi-join hop (`_hop_table_expr`), list parent (`_build_from_and_joins`) — but it emits the bare backing relation.
- The filters travel separately: `_emit_planned` (`slayer/engine/compile/stages.py`) lifts them off the root scope only into `PlannedQuery.mode_a_filters`; `_lower_positions` renders them into that statement's WHERE. Producer sub-plans are correct only because each has its own root scope. `mode_a_filters` also gates the empty-base spine (`_plan_empty_base_grain`) and feeds the window `_src` WHERE (`_plan_src_row_filters`).

The structural cause is that a model's filters are a property of the plan's root, not of the model's relation; every non-root read bypasses them. Instances fixed by construction: join hop, semi-join flat/spine hops, list parent, filter- and derived-column-internal joins, query-backed and SQL-backed targets.

## Goals / Non-Goals

**Goals:**
- One door: a model reaches SQL only as its filtered dataset (python.sql P16); the bug class is unrepresentable and pinned by a law test.
- A filtered dataset never multiplies rows, whatever its filter's hop arity.
- Every guaranteed-failing filter definition is refused at save time (system P18), with the query-time check as backstop.

**Non-Goals:**
- Re-fusing a filtered dataset into the enclosing WHERE (no second emission path; Law 6 inlining applies only to the filter's own joined reads, inside the door).
- Changing model-filter syntax (still Mode A) or population inference (model filters still contribute nothing to it).

## Decisions

The decisions log; tags: `(user)` user's choice, `(default)` obvious default, `(codex)` Codex finding folded in, `(user, approved wording)` verbatim approved text.

1. **Structural fix at the door; the root WHERE path is deleted.** `_emit_relation` emits `(SELECT a.* FROM <backing relation> AS a [inlined joins] WHERE <filters>) AS <alias>` for a model with filters (the inner alias is the model's own name, so a filter naming its host — even inside its own subquery — reads that row; the filter's join aliases are minted by the allocator; unfiltered models emit as today). The root uses the same door; `ModeAFilter`, `PlannedQuery.mode_a_filters`, its `_lower_positions` entry and the root-only lift are removed. Alternative — adding the filter at each read site (ON clause, EXISTS body, list parent) — keeps N paths that must agree by hand. (default: structural bias)
2. **Backing relation split from the dataset door.** A private `_backing_relation(model, alias)` holds today's door body (spine, `_gen_stage_reads` guard, query-backed splice checks, list-child refusal); `_emit_relation` is the only caller and wraps it; `_stamp_model` moves to the outermost relation so `read_models` is unchanged. The derived table's inner FROM is the backing relation (never re-entering the door for the same model); the joins its filters read go through the door, so joined models are themselves filtered datasets. (codex)
3. **Recursive semantics.** A filter's (and a derived column's) joined reads see the joined model's filtered dataset — follows from the issue's "wherever a model is a join target" and falls out of the door. (default)
4. **Joined reads restrict by association; inlined only on proven to-one hops.** Each filter entry whose dependency closure crosses only proven to-one hops is rendered with its joins inlined (LEFT JOIN) in the derived table; the other entries are rendered together as one correlated `EXISTS`, exactly as one entry joining them with `and`, over the existing spine shape (the model's own columns as correlated outer references, NULL-extended joins inside), so the dataset can never contain a row twice. This is exactly the lowering of a field-typed query filter (semantics Axiom 14 + Law 6) — no new concept. The arity proof is the same core helper save-time arity uses. Alternatives: always `EXISTS` (one lowering, but regresses ClickHouse < 25.4 for to-one filters that work today); refuse non-proven hops at query time (contradicts Axiom 1's "unproven hop is accepted with a warning"). (user)
5. **Root gating becomes a plan fact.** Every planner decision that consumed `mode_a_filters` as "the host is restricted" (`_plan_empty_base_grain` host gating, window `_src` WHERE ids) consumes one fact — the root model has filters — folded into the existing `host_gated` flag; the `_src` and empty-base relations read the root through the door and are filtered by construction. (codex)
6. **Filter cycles refused, with new `ModelFilterCycleError(SlayerError, ValueError)`.** Mirrors `ColumnCycleError` (a definition error, not a query typing error), carrying the ordered model chain. A model's dataset depends on every model on every path in its filters' dependency closure (engine P10: through derived columns they name); join edges alone never form an edge. Identity is `(data_source, name)`, overlays and stages by their resolved model. The door keeps a recursion-stack backstop (precedent `_gen_splice_chain` / `QueryBackedCycleError`). (default; closure edges (codex))
7. **Cycle refusal at save time too.** (user)
8. **Save-time cycle check reports only cycles through the saved model**, so a pre-existing stored cycle never blocks an unrelated save. (default)
9. **`validate_model_filter` at save time too**, and at query time for every model the query reads. (user)
10. **Validation resolves each reference against its owning model.** The measure / windowed-column check runs per reference site against the model that owns it — qualified joined references and derived-column closures included — reusing the reference-closure machinery, not a second parser. (codex)
11. **The query-time validation set is the query's read set.** `model_reads.models_read_by` plus its transitive `_closure` — the one definition of "what a query reads" that model access already uses — selects the models whose filters are validated and cycle-checked in the checker; `ModelFilterError` keeps its checker raise site. (codex)
12. **Save validation lives in both save doors.** The filter validation and cycle check run in the storage door beside `validate_derived_columns` (`slayer/storage/base.py`) and in the engine save path, reaching the same verdict, as the ambiguity check does. (codex)
13. **List-child roots need no own-filter path.** Rejected (codex): list children are synthesized by `join_walker.list_child` with no filters and are never user-authored; their parent is emitted through the door.
14. **Spec home: new capability `models/model-filters`** (covered by `cross_cutting_specs.models`). (default)
15. **semantics Axiom 3.3** (user, approved wording): "- **3.3 Filtered datasets.** A model's dataset is its rows passing its `filters` (`M ≡ σ_filters(M)`): every read of `M` — root, join hop, semi-join, producer, another model's filter — sees only those rows, filtered before any join, so a null-extended row means no passing row. Filters that reach their own dataset are circular and refused. [enforced: test:tests/test_dev2210_join_target_model_filters.py]"
16. **python.sql P16** (user, approved wording): "16. **One relation door**: a model reaches SQL only through one door, as its filtered dataset (semantics Axiom 3.3) — never as its bare table, and never filtered afterwards by an enclosing WHERE. [enforced: test:tests/test_law_relation_door.py]"
17. **system P18** (user, approved wording): "18. **Fail at save time**: a stored definition guaranteed to fail every query that reads it is refused when saved, wherever the check is affordable; query time keeps the same check as a fail-closed backstop for definitions stored without it. [review]"
18. **Law test shape.** `tests/test_law_relation_door.py` AST-scans `slayer/sql/`: `_to_table` / `_embed_model_sql` are called only from `_backing_relation`, `_backing_relation` only from `_emit_relation`, and a model's `.filters` is read only by the door (plus the variable-surface collector, which emits nothing). (default; shape (codex))
19. **Coverage additions.** Tier-1 emission tests (ClickHouse, BigQuery, SQL Server, Trino, MySQL, Snowflake) for the derived-table, inlined and `EXISTS` shapes; RLS column and join rulesets on the base table and on a table introduced only by a filter, with `read_models` accounting; isolated / windowed / shifted / date-range / spine evaluations on a filtered root. (codex)

## Risks / Trade-offs

- [Association `EXISTS` for Mode-A text is new rendering work — the semi-join builder takes typed keys] → render the filter in a frame whose root columns resolve to the spine's correlated outer columns; pin with the lowering-soundness scenario and scope validation (`SLAYER_VALIDATE_SCOPES`, on in tests).
- [ClickHouse < 25.4 cannot run the `EXISTS` form] → affects only filters across an unproven or fanning hop, the same requirement query filters across to-many hops already have; `docs/database-support.md` gets one sentence.
- [Root SQL changes shape for every filtered model; golden SQL churn] → results unchanged (scenario "A filtered root is unchanged"); re-bless goldens in the same change.
- [Inner/outer alias scoping in nested derived tables] → the inner alias is the model's name, scoped to its derived table; join aliases minted by the allocator (python.sql P3); scope validator backstops.
- **BREAKING** stored models with cross-model filter cycles or invalid filters now fail when read → they were ill-defined or already failed as roots; the error names the cycle or filter.
