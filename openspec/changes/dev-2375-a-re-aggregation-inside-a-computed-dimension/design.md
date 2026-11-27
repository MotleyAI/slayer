## Context

See proposal.md — Why. Today a synthesized producer's value is read through `FieldKey(path=(), leaf="__regroup__<n>__<seed>")`
(`RegroupPlaceholderRegistry` in `slayer/engine/compile/regroup.py`), a host column whose reserved name changes the
key's meaning — the shape `architecture/python.core.arc42.md` P4 forbids. Three failure families follow (probed on
`main` after the DEV-2417 merge, DuckDB and SQLite):

1. **Re-rooting moves it.** A producer rooted at another home re-roots the placeholder as a column of the host
   (`contractors.__regroup__0__max`, `skills.assignments.__regroup__0__max`) and reads a column no table has. Every
   shape of the tracker issue, its `sum(customers.spend)` comment repro, and the seven strict xfails in
   `tests/test_dev2367_list_elements_across_joins.py`.
2. **Carrying is opt-in per synthesis site.** The shifted and cross-dataset producers pass attaches in by hand
   (`carried_attaches`, `_attaches_carrying` matching by exact key); the local partition producer and the trailing-window
   producer do not: `sum(fee, partition_by=band)` on one dataset, and `sum(amount, window='60d')` beside `band`, fail —
   a render-time recovery of a planner decision (engine P6).
3. **Home and attributability read it as host-homed** (semantics Axioms 2.3, 4, 7): `customers` × `band` over
   `orders.region` gives `count(customers.*)` = 2 where 1 is right; `to_many_handling='error'` does not refuse.

Working today, kept as regression guards: `time_shift` / `cumsum` beside `band`, a filter on the re-aggregation beside a
cross-dataset metric (DEV-2417), `band` consumed downstream, `sum(rate)` on the root.

Principles that apply: core P1 (a key answers one question), P3 (total traversal), P4 (one kind per meaning, dispatch
fails closed); engine P1, P3 (no context variables), P4 (one interned producer attached once per phase), P6 (placement
from typing, never key shape at render), P10 (dependency closures); semantics Axioms 2.3, 2.4, 2.5, 2.8, 4, 6, 7, 8,
9, Laws 3 and 7. No arc42 or `.c4` edit: the change brings code into line with existing principles. The spec delta
attaches to the cross-cutting `queries` group (`architecture/index.yaml`).

## Goals / Non-Goals

**Goals:**
- One representation of an attached value, read identically by every plan whatever its root.
- One attach rule for every plan; no synthesis site carries attaches by hand.
- Home, attributability and closures of an attached value from its grain members.
- Golden SQL text unchanged (the alias text `__regroup__<id>__<seed>` stays).
- DEV-2017's scope delivered in full.

**Non-Goals:**
- A 1.x backport (its own issue if wanted).
- Any change to filter disposition (DEV-2417) or to which producers a shape synthesizes.

## Decisions

Decisions log — `(user)` the user's call, `(default)` an obvious default, `(codex)` a Codex plan-review finding folded in.

1. **Structural fix, not per-site patches** — the three families share one cause; patching re-rooting, carrying and
   home separately would leave the reserved-name kind and the hand-carried attaches standing. (user: "go for the
   structural solution, not a band-aid")
2. **DEV-2017 folded in** — the dedicated kind exists to carry exactly the attach and grain semantics this bug needs;
   splitting would design the kind twice. DEV-2017 closes with this change's PR. (user)
3. **The kind: `AttachedKey`** = `attach` (an opaque id naming one interned producer answer) + `on`: the
   `(producer grain slot, member)` pairs, canonically ordered by slot, each member a grain member in the reading
   plan's coordinates. Producer details (plan, answer slot, empty value, alias seed, original key, value type) live in
   the attach table, not the key (core P1). Alternatives: carrying the original key (`of`) in the key — rejected, it
   puts producer detail in identity; an ordered member tuple paired by position — rejected (codex, finding 6): grain is
   a set and producer slot order is physical, so position pairing can silently join the wrong columns. (default, codex)
4. **Phase is the edge's, not the key's** — one value read at ROW (a grain member, a row mask), AGGREGATE (the
   combined SELECT) or as a shifted lookup is one key, attached once per phase that reads it over one interned producer
   (engine P4); the reading position sets the phase. Alternative: phase in identity — rejected, it splits one value
   into unequal keys and defeats slot interning. (codex, finding 1)
5. **Traversal** — `children()` / `map_children()` cover the `on` members only, so re-rooting, prepending and
   substitution rewrite the join and never the identity (core P3); a producer rooted anywhere joins the same producer
   on its own path to the grain (Axiom 6). Every kind dispatch (walkers, substitution, `KIND_POLICY`, typing, scope
   resolution, rendering, projection, shift) handles the kind explicitly or fails closed (core P4). (default)
6. **Classification vs routing** — for operand and source classification an `AttachedKey` is an attached constituent
   (Axioms 2.3–2.4): an aggregation whose source holds only attached values is second-order, homed on its operand
   dataset. For routing it is already routed: discovery never synthesizes a producer for it, so a re-aggregation's
   outer producer never rediscovers its own outer aggregate (DEV-2017's recursion hazard). The driving outer-producer
   shape (`drives`) is kept. (codex, finding 2)
7. **Home and attributability** — a row-level expression holding an `AttachedKey` stands for its `on` members (Axiom
   2.3): home resolution, attributability, dependency closures (engine P10) and source anchors read the members, so
   the existing association and error arms take an unattributable re-aggregation dimension like a plain one. The
   implementation names each home / attributability function receiving the case. (default, codex finding 4)
8. **One attach table and id allocator per generated SQL statement** — shared by every stage and every recursively
   synthesized producer of that statement, carried in one explicit compilation context through `compile_synthesized`,
   routing and `SlotTyping`; entries are registered before recursion. Ids are distinct by construction, so
   `reserved_placeholders` and the registry's `reserved` set retire. An `AttachedKey` never crosses a stage schema: a
   later stage reads a plain column (engine P5). (default, codex findings 8–9)
9. **Identities** — the attach id is per interned producer answer: structurally identical producers, from any root,
   share one id; an edge (phase + join pairs) is per reading plan. A producer is compiled once, where its id is minted,
   from the original key with its settled grain (DEV-2007); readers reuse the compiled plan and never re-synthesize or
   re-default its grain. (codex, findings 5 and 7)
10. **One attach rule** — after a plan's own regroup planning, every `AttachedKey` it reads and does not produce
    is attached from the table, joined on the key's pairs at the reading phase; a plan's own driving operand stays a
    drive (its rows are the plan's FROM). `carried_attaches`, `_attaches_carrying`, `_placeholders_read` and
    `placeholder_originals` retire; the shifted producer keeps its carry-vs-recompute decision. (default, codex
    finding 3)
11. **Classifiers read the table** — every classifier that needs an attached value's original
    (`shift.carried_placeholders`, `_series_mode`, `_re_evaluated`, discovery, the projection phase) reads it from the
    table; `_regroup_substituted_composite_phase` becomes plain phase propagation. (codex, finding 10)
12. **Plan-time invariant** — before a `PlannedQuery` is handed off: each `AttachedKey` read has exactly one edge at
    its reading phase, every member resolves in the reader's coordinates, the pairs cover the producer grain exactly
    once, and the answer slot exists; else an internal error at plan time, never a render-time scope miss. A
    producer-graph acyclicity check — rejected (codex, finding 11): an id is minted only after its producer compiles,
    so a cycle is unrepresentable. (codex, finding 11)
13. **Rebuilt-aggregate pairs are recorded by the substitution** — `_carried_homes` reads the record; the
    `_rebuilt_aggregates` / `_ordered_parts` inference (a partition key paired only when exactly one member changed)
    retires, so `partition_by=[band, band2]` plans. (default; the tracker issue's notes)
14. **Typing** — the `minted_placeholders` context variable retires; typing an `AttachedKey` reads the statement's
    table (engine P3). (default)
15. **Rendering** — the generator scope keys attached fields by `AttachedKey`; the alias text `__regroup__<id>__<seed>`
    comes from the table, so golden SQL text is unchanged; the five `REGROUP_LEAF_PREFIX` checks become kind dispatch;
    `REGROUP_LEAF_PREFIX` survives only in the renderer's alias naming. (default)
16. **The reserved-prefix field guard retires** — `check_reserved_regroup_prefix`, `reserved_prefix_fields` and their
    raise-ledger row: a real field can no longer shadow a placeholder. (default, DEV-2017 scope)
17. **The miscount is fixed here** — it is family 3 of the same cause and decision 7 removes it; affected queries now
    associate (or refuse in error mode) instead of multiplying. (default)
18. **No 1.x backport in this change.** (default)
19. **Tests** — unit ratchets for the kind (equality and hash, reroot through members only, atomic substitution, total
    traversal, fail-closed dispatch, interning across roots, dual-phase reuse, dependency closure through a derived
    grain member) on top of the spec scenarios, plus association and error cases across reverse-to-many, list-child,
    windowed and keyless homes, operand-only cells under a driving producer, explicit / defaulted / degenerate outer
    grains, nested re-aggregation, and two stages each minting attaches. (codex, findings 3–5, 12)

## Risks / Trade-offs

- [The kind touches every kind dispatch] → explicit cases plus fail-closed tails, and a test enumerating
  `VALUE_KEY_TYPES` against each dispatch.
- [Classification flip in decision 6 changes how an outer aggregate over attached values is routed] → keep the
  driving shape, and pin today's outer-producer SQL with the existing goldens (byte-identical).
- [Threading the compilation context replaces a context variable across many signatures] → one context object,
  registered before recursion; an interleaved-compilation test.
- [Results change for queries that multiplied before] → stated as BREAKING in the proposal; each is the Axiom 4 value.
- [~14 existing tests construct reserved-leaf placeholders or pin the retired guard] → aligned with the user's approval
  in the tests stage.
