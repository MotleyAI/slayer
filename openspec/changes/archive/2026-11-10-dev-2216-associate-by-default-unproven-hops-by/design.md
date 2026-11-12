## Context

See proposal.md › Why. Current state the approach builds on:

- `SlayerQuery.to_many_handling: Literal["broadcast", "associate", "error"] = "broadcast"`
  (`slayer/core/query.py`), copied onto `PreboundQuery` at bind; several compiler and
  checker sites compare the raw string (`stages.py` cross-model/product association,
  re-aggregation; `elaborate_env.py` explicit outer keys and partition keys).
- Query time draws no line between unproven and fanning hops: both make a dimension
  unattributable (`join_safety.py` two-way predicates), resolved per mode; both make an
  aggregation input unsafe (`UnsafeJoinInputError`, every mode, broadcast included).
- Association is the two-level `AssociationProducerKernel` (`ir/planned.py`, rendered by
  `generator.py::_render_association_producer_body`): level 1 groups grain × entity key
  and picks each input once, level 2 aggregates. The entity key is the home's first
  unique-key set; a home without one fails with `AssociationError`, except the keyless
  `list_rows` path (every associated dimension a list child of the root).
- Association-only refusals: `window=` / `first`/`last` (`AssociationError`), the unique
  key, and row-valued parameters not determined by the entity grain
  (`ParameterGrainError`).
- Broadcast warnings merge by `(location, measure)` (`query_engine.py`).
- Stored queries are dumped with defaults (`without_none(model_dump(mode="json"))`), so
  every stored query carries `to_many_handling: "broadcast"`.

## Goals / Non-Goals

**Goals:**
- One resolved mode policy read by every compile and check site.
- Association eligibility decided before the association arm, so the default never
  raises on a shape broadcast handles.
- A per-query row identity that is a first-class entity key.

**Non-Goals:**
- The association kernel's ranked/windowed arm — DEV-1914. Until it lands those shapes
  are the fallback set.
- DEV-1932 Case 1/1b (fanning partition keys) — they stay mode-invariant input-safety
  errors.
- Relaxing input safety on unproven hops. The issue brief listed "an aggregation input
  that crosses an unproven hop" as a broadcast-fallback case; it is dropped: such an input
  is not single-valued at its home, so broadcast has no value to repeat (it already raises
  under explicit `broadcast`), and treating the hop as to-one would break No double
  counting (Axiom 2.8).
- Marking associated measures as slice-5 row sources (DEV-2212 adds that with its
  consumer).
- Surfacing engine warnings through the MCP query tool (DEV-1951).

## Decisions

The planning decisions log, tags preserved.

1. **The fallback set is the association-arm gaps only** — Options: (A) only shapes the
   association arm cannot serve; (B) also relax unproven-hop inputs to to-one; (C)
   implement the kernel gaps here. Chose A: broadcast cannot produce a value for an input
   that would multiply rows, and B breaks No double counting. (user)
2. **Kernel gaps are tracked on DEV-1914**, which already covers `window=` / `first`/`last`
   in the kernel; a comment there replaces a new issue. (default; the user asked for an
   issue, the existing one was proposed instead)
3. **DEV-1932 Case 2 is a dispatch bug fixed here**: an ungrained re-aggregation inner
   resolves per the mode like its bare form. (user, accepted with 1)
4. **The default is `None`, not a fourth value** — rejected `"auto"`: it would become a
   dead public value once DEV-1914 lands and need a deprecation cycle to leave saved
   queries. `QueryRefinement`'s `None` keeps meaning "inherit"; the field leaves
   `_NON_NULLABLE_SETTINGS`. (user)
5. **Stored `"broadcast"` migrates away** (SlayerQuery v9 → v10, `stored_only`) rather than
   being kept — a stored `"broadcast"` almost always means "was the default"; 2.0 meaning
   changes are announced in the release notes. Fresh input naming `"broadcast"` is never
   rewritten. (user)
6. **The fallback warning reuses the `broadcast` kind** with
   `association_unsupported: str | None` and a fallback hint, rather than a new kind — one
   event, one warning; `kind` keeps describing what happened to the values. (user)
7. **The ordinary broadcast hint is reworded** for the new default ("…omit
   to_many_handling (or set 'associate') to reconcile the two."). (default)
8. **Row identity**: only for homes without a unique key; in every associating mode; bag
   semantics (identical physical rows count separately, only join fan-out copies dedup);
   the `ORDER BY` quirk in `dialects/`; reusable for slice 5's raw-row mode. (default)
9. **Row supply**: no new plan structure; one scenario pins that an associated
   producer's cells are the tuples its home rows reach. (user)
10. **Spec scenarios** describing broadcast mechanics become explicit `broadcast`; those
    defining the default flip to associated outcomes. (default)
11. **Input-safety errors stay mode-invariant** (follows from 1). (user)
12. **Axiom 8 wording** (user, approved wording): "8. **Mode axis**: an unattributable
    dimension resolves per the query-level `to_many_handling` mode — associate (the
    default: per-cell aggregation over the distinct associated home rows, a keyless home's
    rows told apart by a per-query row number)
    [enforced: test:tests/test_dev1841_association_exec.py]
    [enforced: test:tests/test_dev1910_home_rooted_association.py]
    [enforced: test:tests/test_dev2216_keyless_association.py]; broadcast (the value
    repeats across the dimension's cells, with a self-announcing warning; spec:
    `queries/semantics` › Loud degradation)
    [enforced: test:tests/test_dev1836_broadcast_strict.py]; and error (refuse)
    [enforced: test:tests/test_dev1841_error_mode.py]. Every aggregation is
    **association-eligible** [target: DEV-1914]; an ineligible one broadcasts under the
    default, warning with the reason, and is refused under an explicit associate
    [enforced: test:tests/test_dev2216_associate_default.py]; an explicit partition_by …
    (rest unchanged)"
13. **Law 5 wording** (user, approved wording): "5. **Dice–slice**: filtering to `d = v`
    equals slicing the `v` cell of a groupby on `d` — exact for every association-eligible
    aggregation under the default and associate; broadcast consciously trades this law
    away and must warn." (tags unchanged)
14. **The Ossie upstream question is a merge gate** (DEV-2221), not a planning blocker.
    (default)
15. **One resolved mode policy** — `None` is resolved once at bind into a typed policy
    (association requested; fallback allowed; explicit mode), carried on the prebound
    query; every compiler and checker site reads the policy, none compares the raw field.
    Includes the re-aggregation association, explicit outer keys, explicit `partition_by=`
    unattributable from a further root (associates by default), and cross-model / product
    association. (codex)
16. **Eligibility is classified before the association arm** — every association-only
    refusal (`window=`, `first`/`last`, a row-valued parameter the entity grain does not
    determine) becomes a typed ineligibility reason: the default broadcasts with it,
    explicit `associate` raises the typed error. (codex, user)
17. **The row id is a real entity key** — an IR entity key in `entity_grain`, determining
    every column of its own home row, so parameter determination works on a keyless home.
    (codex)
18. **The row id is minted before any association join** — in a subrelation of exactly
    the home's filtered physical rows (the relation door, python.sql P13), carried
    through the association joins and query-filter evaluation; never numbered in the
    level-1 SELECT, where fan-out copies would get distinct ids, and never matched across
    two references to a numbered relation. (codex)
19. **Fallback reason joins the warning merge identity** — broadcast warnings merge by
    `(location, measure, association_unsupported)`, so an ordinary broadcast and a
    fallback of one measure stay separate, each with its own hint. (codex)
20. **`list_rows` is untouched** — the keyless list-child association keeps its key-free
    kernel; the row id applies to ordinary home rows only. (codex)
21. **The migration reaches nested stages** — stored `source_queries` stages inside
    stored models, with and without their own `version`, non-final stages included;
    tested on YAML and SQLite storage. (codex)
22. **The fallback covers only the implicit grain** — an ineligible aggregate whose
    explicit `partition_by=` key would need association fails under the default with the
    same typed error as under `"associate"`: requested grain is never broadcast (Loud
    degradation; explicit keys already refuse broadcasting under `"broadcast"`). Surfaced by
    the spec sweep. (default)
23. **Stored `strict: false` follows decision 5** — it always meant "the default", so after
    the v3 → v4 step it loads with the mode omitted (the new default). (default)
24. rejected (codex): "restore the brief's third ineligible bullet (unproven-hop inputs
    fall back to broadcast)" — the user ruled it out (decision 1); the compiler's own
    broadcast arm already refuses such inputs, so there is no broadcast value to fall
    back to.
25. **Determined grain members are pruned before attribution** — a grain member the rest of the implicit grain determines (e.g. an aggregate grained within it) is not a producer grain key, so it neither associates nor broadcasts; reuses the local regroup's rule. (user)

## Risks / Trade-offs

- [Saved queries change values silently on upgrade] → the `associated` warning appears on
  every affected response; the release notes name the change and the opt-out
  (`to_many_handling: "broadcast"`).
- [Association costs more than broadcast (a two-level producer per measure)] → accepted:
  correctness over speed; explicit `broadcast` remains for callers who want the cheaper
  repeat.
- [`ROW_NUMBER()` over an unordered relation is nondeterministic across evaluations] →
  the id is minted and consumed inside one relation, so only its distinctness matters.
- [A dialect rejects `ROW_NUMBER() OVER ()`] → a `dialects/` hook supplies the required
  `ORDER BY`; point-checked live on the Tier-1 backends.
- [The default refuses a shape broadcast handled] → decision 16 classifies eligibility
  first; default-mode parity tests cover local ranked aggregates, product/unrelated
  datasets, list children, explicit cross-model `partition_by=`, aggregation parameters
  and null-safe re-aggregation.

## Migration Plan

SlayerQuery v9 → v10 (stored-only): drop `to_many_handling: "broadcast"` from stored
queries and from stored stages nested in models. DEV-2212's slices also bump stored
versions; whichever merges second renumbers its migration. No rollback beyond re-setting
`to_many_handling: "broadcast"` on a query.
