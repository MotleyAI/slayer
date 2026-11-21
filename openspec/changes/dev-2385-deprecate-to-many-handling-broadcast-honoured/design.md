## Context

See proposal.md › Why. Today a deprecated input form (`strict`, the `population` extension object, Mode-B
spellings) is *converted* at construction: `deprecated_input(...)` (`slayer/core/deprecation.py`) emits one
`FutureWarning` and returns a payload kept on the object (`SlayerQuery._deprecated_inputs`), which runs and saves
report via `deprecated_inputs_of`. `"broadcast"` cannot be converted without changing results, survives dumps, and
is re-validated whenever SLayer copies a query internally; `SlayerQuery._migrate_and_rewrite` treats a dict with
`version` (every dump) as a stored document. Stored `"broadcast"` at SlayerQuery v11 is dropped by
`slayer/storage/many_handling_migration.py`. The `broadcast` warning payload is built per
`(stage location, metric alias)` in `_collect_broadcast_warnings` (`slayer/engine/query_engine.py`) from
`RegroupAttachPlan.broadcast_metric` / `broadcast_dimensions` (`slayer/engine/compile/stages.py`), where the
metric is a compiler alias (a composite leaf reads `customers.spend_sum`).

Equivalence probe (SQLite + DuckDB, this branch at `29edfe97c`): `broadcast` vs `partition_by=` over the
attributable dimensions gave identical values, and the `partition_by=` form no warning, for a cross-model sum
(`[]`), mixed grain (`[customers.tier]`), local `last` (`[]`), a local window by status × signup month
(`[customers.signup_at]` — a time dimension is spelled by its field, `month(...)` is refused), and a composite.

Applicable principles: `semantics.arc42.md` Axiom 8 (mode axis), Axiom 10 (explicit coarser grain), Law 5
(dice–slice); `compatibility/removal-tripwire` (one marker, sole `FutureWarning` emitter, 3.0 tripwire);
`system.arc42.md` §3.6 (Pydantic v2). No import-graph change; `model.c4` / `views.c4` untouched. No new
capability: all three deltas modify existing spec groups (`queries`, `compatibility` cross-cutting; `mcp` on its
node).

## Goals / Non-Goals

**Goals:** one canonical source for the broadcast deprecation payload on runs and saves; the concrete
replacement on the `broadcast` warning; agent surfaces stop offering the mode; values and SQL byte-identical.

**Non-Goals:**
- What 3.0 does with stored `"broadcast"` (refuse on load or migrate) — decided by the release that removes the
  marker.
- Intercepting `query.to_many_handling = "broadcast"` assigned after construction (fields are not validated on
  assignment); runs still report it, since reporting reads the effective stage mode.
- Any change to broadcast semantics, to `"error"`, or to the existing stored-v11 migration.

## Decisions

**D1. Run-time reporting is the one source of the payload.** The run computes, from its input stages after
refinement merge, one `deprecated_input` payload per stage whose effective mode is `"broadcast"`, keyed by stage
identity (stage name / position), emitted or pruned — never from the plan's broadcast collector (a broadcast
stage may never broadcast) and never by object identity (internal re-validation would duplicate). Construction
emits only the `FutureWarning` and keeps no payload for this form, so a fresh query is never reported twice.
Saves compute their payloads from the saved document's stages through the same function, never from the save's
dry-run warning set. Alternative rejected: keeping the construction payload and deduplicating (two sources;
inheritance moves the stage it belongs to). (codex)

**D2. `FutureWarning` provenance = fresh input only.** Fresh means given without a `version` and outside a stored
document — the existing `_migrate_and_rewrite` provenance. Stored documents and versioned dicts emit no
`FutureWarning`; their runs still report the payload (D1). Detection runs after schema migration, so a v11
stored default the migration drops never reports. One marked `deprecated_input(…, removal="3.0")` site; it gains
a way to emit the warning without the caller keeping a payload. Alternative rejected: stored-document context
alone as provenance — SLayer re-validates dumped (versioned) queries internally, which would fire spurious
`FutureWarning`s. (rejected (codex): the response payload is unaffected; only a fresh versioned dict loses the
Python warning.)

**D3. Concrete replacement carried from the compiler.** The planned attach gains the aggregate's original call
text and its public `partition_by` spelling (the attributable grain dimensions as the query spells them; a time
dimension by its field; `[]` for none). `BroadcastGrainWarningPayload` gains `partition_by: list[str]`; its hint
names the call and `partition_by=[…]` and keeps `DICE_SLICE_HINT`. Never reverse-engineered from aliases. (codex)

**D4. Schema hides, validation accepts.** `SlayerQuery.to_many_handling` and `QueryRefinement.to_many_handling`
keep `Literal["broadcast", "associate", "error"] | None` for validation; a callable `json_schema_extra` publishes
the enum `["associate", "error"]`; an unrecognized value raises a validation error naming only `associate` and
`error`. (user; validation-error wording (codex))

**D5. Agent surfaces.** Concepts drop `broadcast`; `docs/reference` lists it as deprecated (3.0, `partition_by=`
pointer); the migration guide's "keep 1.x values" entry points to `partition_by=`. Stale DEV-1914 text on the same
surfaces ("falls back to broadcast for `window=` / `first`/`last`") is rewritten here. The comparison probes keep
running the honoured mode; only `matrix.yaml`'s `how` row stops listing it. (default)

## Decisions log

1. Ossie Metrics and Querying WG outcome: none recorded (DEV-2221, DEV-2216, public archives). Options: build
   now and re-check before merge / block until answered. Taken: build now; re-check before merge (tasks.md gate)
   — the deprecation is reversible within 2.x, the 3.0 refusal is the real decision point. (user)
2. Stored `"broadcast"` in 2.x. Options: fresh input only / also report on every run from stored documents.
   Taken: report on every run, no `FutureWarning` on load — a form not convertible without changing values must
   reach stored users before 3.0. (user)
3. Concrete replacement. Options: structured `partition_by` + concrete hint naming the call / generic hint.
   Taken: concrete — the planner already computes it. (user)
4. Schema enum. Options: hide `broadcast` while accepting it / keep it listed as deprecated. Taken: hide;
   reference docs still list it as deprecated. (user)
5. arc42 Axiom 8 (user, approved wording): replace the broadcast clause and its
   `test:tests/test_dev1836_broadcast_strict.py` tag so the mode list reads "…; and error (refuse)
   [enforced: test:tests/test_dev1841_error_mode.py]. A value repeated across a dimension's cells is a coarser
   grain asked for by `partition_by=` (Axiom 10), never a mode: the deprecated broadcast mode (the same value,
   warned) is refused from 3.0 [enforced: test:tests/test_law_removal_tripwire.py]. Every aggregation associates
   — …" (the remainder unchanged).
6. arc42 Law 5 (user, approved wording): "— exact for every aggregation under the default, associate and error;
   the deprecated broadcast mode trades it away and warns."
7. Reference docs list `broadcast` as deprecated rather than dropping it; concepts drop it. (default)
8. Unrecognized-mode validation error names only `associate` and `error` (replaces the planning default that kept
   every accepted value). (codex)
9. Python attribute assignment after construction not intercepted; runs report via the effective stage mode.
   (default)
10. Stale DEV-1914 fallback text on the rewritten surfaces is fixed here. (default)
11. Comparison probes keep running `"broadcast"`; only the matrix `how` row stops offering it. (default)
12. One payload source: run-time per effective input stage; construction emits only the `FutureWarning`. (codex)
13. `deprecated_input.new` is generic ("an explicit `partition_by=` on each aggregate"); the concrete list lives
    only on each aggregate's `broadcast` warning. (codex)
14. Fresh provenance by stored-document context alone — rejected (codex): internal re-validation of versioned
    dumps would fire spurious `FutureWarning`s; the response payload is unaffected either way.
15. Payloads derive from input stages (emitted or pruned) and effective mode, not from the broadcast collector.
    (codex)
16. The planned attach carries the original aggregate call and public `partition_by` spelling. (codex)
17. Saves compute payloads from the saved document's stages via the same function, not the dry-run warning set.
    (codex)
18. Axiom 8 "never a mode" contradicts 2.x honouring — rejected (codex): arc42 is target-state and the sentence
    names the deprecated mode refused from 3.0; wording approved verbatim.
19. What 3.0 does with stored `"broadcast"` is a Non-Goal. (default)

## Risks / Trade-offs

- [Existing tests pin exact warning lists on fresh-`"broadcast"` queries] → they gain the payload; aligning them
  needs the user's approval in the stage that hits them (one request naming each file).
- [Stage identity for pruned / nested stages] → key by the stage's query-local name or position in the input;
  a refinement merges into the stage it refines (one payload).
- [A fresh versioned dict gets no `FutureWarning`] → accepted (D2); its runs still report.
- [Hint text for composite leaves] → requires carrying the call text through planning (D3); tests cover aliased,
  repeated, filter-only and order-only positions.
