## Context

Probe on this branch (SQLite, `tests/_dev2102_fixtures`): stage `s` = `orders`, dimensions `[customers.name]`, measure
`n = count(*)`. Its output column's bind name is the flat `customers__name` (result key `orders.customers.name`).

| Spelling | Stage scope (`source_model: s`) | Model-scoped root joining `s` |
|---|---|---|
| `customers__name` / `s.customers__name` | works | works (stored `addresses`, stored `regions`, inline `customers`) |
| `customers.name` / `s.customers.name` | `UnrelatedDatasetError` / `IllegalScopeReferenceError` | broken chain / wrong "Did you mean" / `CircularJoinPathError` |

The tracker issue assumed the dotted spelling resolves from a stage scope; it does not. The flat `__` spelling is the
documented, specified downstream spelling (`docs/concepts/models.md` "Column naming in query-derived models";
`openspec/specs/mcp/tool-surface`; `queries/computed-dimensions`). `s.nope` names the minted `__slayer_stage_s`.

Applicable principles: engine §3.5 (stages compose only through flat schemas — kept), §3.7 (no new slack rule; the
spelling stays refused), §3.3 (binding is pure — the hint probe records nothing), §3.11 (user spelling in messages),
core §3.5 (typed errors), system §3.10/§3.11 untouched. No arc42 or `.c4` edit. The spec delta attaches to the
existing cross-cutting `queries` group.

## Goals / Non-Goals

**Goals:** every failing dotted spelling of a flattened column ends in an error that names the flat spelling; no
reference error exposes a minted stage identity; the pinned inline-root case and the stored-root variants execute.

**Non-Goals:**
- Making the dotted spelling of a flattened column legal (option A, rejected by the user — see Decisions).
- Per-stage universe construction (DEV-2267) and static-reader behaviour (DEV-2265).
- Pre-empting a single-hop short form that routes (it is a valid read of another model).
- Reconciling the existing "Datasource-scoped and deferred past stage boundaries" requirement text with today's
  stage-scope routing behaviour.

## Decisions

1. **Keep `__` canonical; refuse the dotted spelling with a flat-name hint (option B).** (user)
   Options: A — consume the longest flat column before hops at a stage boundary, in both scope kinds (the issue's
   ask); B — keep the one spelling and fix the errors. A reverses the documented rule, adds a second spelling, makes
   `s.customers.regions.name` ambiguous when `s` also joins `customers` (a column silently shadows a join), and needs
   an engine §3.5 amendment. B fixes the real defect (errors that send agents the wrong way) at the cost of one
   retry after a dotted guess.
2. **The hint fires only at walk-failure doors, over every model, nearest-root first.** (default)
   At a failure the hint is always correct, so no stage predicate (and no name-shape test, engine §3.6) is needed; the
   model nearest the root wins (longest flat name). Doors: the multi-hop unresolvable hop and the circular revisit
   inside the dotted walk (and the dotted star walk). A routing single-hop miss is not a door.
3. **Error types follow why the reference fails.** (default) In a model scope the dotted reference is grammatical
   (dots are hops) and fails to resolve → `UnknownReferenceError` with `scope_summary` naming the model and its
   columns and `suggestion` the flat spelling. In a flat stage scope dots are grammatically illegal → the existing
   `IllegalScopeReferenceError`, its reason naming the flat column. An unrelated short form is a correct binding to
   another model → `UnrelatedDatasetError` kept, suggestion led by the flat column.
4. **The unrelated-read hint gets typed plumbing.** (codex) `_check_product_hops` passes the population
   `SlayerModel` (the product sides' host) to the checker's unrelated-read check, which derives the candidate from
   the bound column key's path tokens and leaf (`ColumnKey` / `ColumnSqlKey` only; any other key form gives no hint)
   and probes the population model's columns. The checker never reconstructs user syntax.
5. **Display spellings are translated at the binding boundary only.** (codex) The shared join walker keeps canonical
   graph identities in its error fields (`root_model`, `revisited`, `via`), which other callers consume. The binder,
   where it raises or re-raises a user-facing reference error, maps each model to its `spelling`
   (`SlayerModel.spelling`; the root via `root_spelling`).
6. **A dedicated non-recording flat-name probe.** (codex) A new probe in `slayer/core/scope.py` returns the one column
   a flat name reaches (exact `match_name`, else a unique respelling) or nothing, records no stale spelling and never
   raises; ambiguity yields nothing, so the original walk error stands. `resolve_generated_column` /
   `resolve_flat_name` keep their recording behaviour for real binding.
7. **Quoting.** (codex) The joined flat candidate is matched exactly when any of its remaining segments was written
   double-quoted, else by the ordinary `match_name` rules; the suggestion spells the column's declared name,
   double-quoted in the exact case.
8. **The broken-chain error names the model the failing hop leaves and its columns.** (default) This applies when no
   flat column matches, for every model. It delivers the issue's "a genuinely unknown field fails with an error
   naming the stage's columns".
9. **Pinned test spelling.** (default, follows decision 1) `test_an_inline_source_is_local_to_its_own_stage[inline-root]`
   drops its strict xfail and reads `s.customers__name` (result key `customers.s.customers__name`). The issue asked
   to flip it; under B its dotted spelling is the refused form, so the canonical spelling is what the test pins.
10. **Executed stored-root tests use joins that match rows.** (default) The issue's `[["id", "n"]]` join yields only
    NULLs; the value tests join on keys that match, so exact rows are asserted on SQLite and DuckDB. The issue's
    exact join shape is kept for the error tests.
11. **Codex: longest-flat test, authored-model test, ambiguous-respelling test, quoted-segment tests added.** (codex)
12. **Docs.** (default) One sentence in `docs/concepts/queries.md`'s sibling-stage paragraph: a joined stage's columns
    keep their flat names (`s.customers__name`).

## Risks / Trade-offs

- [Existing tests pin the old error type or text of a broken chain / circular join / unknown column] → `pr-tests`
  lists each and asks for approval to align it; wording is not asserted in new tests beyond the named flat spelling,
  the model spelling and the absence of `__slayer_`.
- [A flat hint on an authored model with a literal `a__b` column in a genuinely broken chain] → the hint is still
  the one column the spelling names; correct by construction.
