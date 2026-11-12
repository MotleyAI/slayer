## Context

See proposal.md (Why). Current state:
- `StorageBackend._repair_query_filters` (`slayer/storage/base.py`) finds sites with `legacy_literal_sites`
  (`slayer/storage/legacy_time_literals.py`, a `sqlglot.parse_one` of the Mode-B filter) and types the compared column
  with `_stored_column_type`, a raw-dict walk of forward joins, one adjacent hop per qualifier.
- DEV-2102 (merged into this branch) added the static resolver with binder semantics: `engine.extensions.query_scopes`
  builds per-stage `QueryScope`s (extensions overlaid and merged into the join graph, sibling-stage hosts, respelling)
  and `QueryScope.resolve` / `resolve_path` resolve hops as the binder does (own qualifier dropped, walk with incident
  edge-name precedence in either declared direction, a lone unmatched hop short-form routed). Drift, the memory
  resolver and `model_reads` already read through it. It ignores quoting today.
- A model load deliberately reads peers raw (`_cached_raw_model_dict`, never mutated, memoised per `load_models` pass)
  so the load pipeline never recurses.
- The engine's bundle builder skips unloadable peers (`DocumentLoadFailures.skip`).

Principles that apply: system §3.8 one reader of Mode B; §3.5 async-first; §3.6 Pydantic, never dataclasses; §3.2 model
truth (storage → engine, storage → core, engine → ir arrows exist; no `.c4` change expected); storage §3.3 versioned
persistence (the repair stays part of the load-time upgrade) and §3.3.1 (target) a load never writes — views write
nothing.

## Goals / Non-Goals

**Goals:**
- One reference resolver: the repair, drift, the memory resolver and `model_reads` share `resolve_path`, and it agrees
  with the binder, quoting included.
- One Mode-B reader: storage stops parsing Mode-B text.
- One dict-upgrade path: a typed view of a stored model equals what its load produces, bar live refinement.

**Non-Goals:**
- Live refinement of below-v8 peers inside views: their untyped columns stay verbatim, as today.
- Time-dimension `date_range` repair: unchanged.
- Widening axiom 8's enforcement test to sqlglot (telling Mode-A from Mode-B sqlglot uses apart is out of scope).

## Decisions

1. **Relation to DEV-2102 (#583): merge its branch into this one now; PR base stays `main`.** Weighed: wait for #583 to
   merge; test against main's root-only extension. Taken because the user is merging #583 now and case (c) needs its
   `extensions`. (user)
2. **The repair is a static reader on `query_scopes` / `QueryScope.resolve`; `_stored_column_type`, `_query_host` and
   the raw `_extended` helper are deleted.** Weighed: patching the raw walk with reverse edges and routing. Rejected:
   it keeps a second resolver that must agree with the binder by hand — the bug class itself. (default)
3. **Axiom 8: site detection moves into a `core.syntax` span reader; storage stops parsing Mode-B with sqlglot.**
   Weighed: keep the sqlglot parse and replace only the resolution. Taken: the resolved reference must be the one the
   binder reads, quoting included; sqlglot's reading of Mode-B (`==`, Mode-B functions, quoted segments) can differ.
   The reader follows `legacy_spans` / `call_spans`: it reads through the shared `_read`, yields nothing when the text
   does not parse, and each string-literal token records its offset outside node identity (as `Placeholder.start`).
   Each item: the literal's value and source span `[start, end)`, and the other operand's reference parts (`Ref` → one,
   `DottedRef` → its parts) with per-segment quoting. Covered: `=`/`==`, `!=`/`<>`, `<`, `<=`, `>`, `>=` in either
   operand order (parenthesised too), and each element of an all-literal `IN` / `NOT IN` list. (user)
4. **Typed models come from in-memory views, built by one dict-upgrade method shared with `_model_from_stored`** (stamp,
   custom-bucket rewrite, migration, `__`-alias rewrite, join-key canonicalisation and declaration, exact-inverse
   dedup), then validated into `SlayerModel`; views skip live refinement, write-back and filter repair. Weighed:
   loading peers typed through `get_model` / `load_models`. Rejected: short-form routing needs the whole datasource,
   so a typed load recurses into the model being loaded — the raw-read design exists to prevent that. (default)
5. **Every view starts from a deep copy of the cached raw document**, so building views never mutates the shared
   `_RAW_PASS` cache nor another view's working copy. (codex)
6. **Views run the per-model pipeline against raw peers, not in datasource-wide phases.** Rejected (codex): migrating
   every document first, then running the cross-model steps on the normalised snapshot. Reason: a view must equal what
   the model's own load produces, and that load runs per model against raw peers; phases would let the repair and the
   runtime binder see different models. Raw peers are load-order independent. (rejected, codex)
7. **A view that fails to build or validate is skipped**, as the engine's bundle builder skips an unloadable peer, so
   the repair routes over the graph the query will run on. Rejected (codex): marking routes through a failed peer
   unresolved — stricter than the binder. A test pins that an unloadable peer does not block an unrelated repair.
   (default; test from codex)
8. **Memories use the same views**, so a memory load causes no model write-backs. (default)
9. **Datasource choice: a query-backed model's stages read its own datasource; a memory's query reads the datasource
   storage identity resolution picks for its source (priority applied).** `query_scopes` gets that one datasource; a
   genuinely ambiguous identity stays verbatim. Replaces the plan's "other than exactly one scope → verbatim".
   (codex)
10. **`resolve_path` / `QueryScope.resolve` take optional per-hop quoting**, threaded into the root-qualifier drop and
    the walk; the repair reads the leaf with its quote flag. Existing callers pass none, so their behaviour is
    unchanged. Weighed: leave any quoted reference verbatim. Taken: cleaner, and it brings the shared resolver to the
    binder's semantics. (codex)
11. **Repair flow.** Only when some filter has a legacy-literal site: the stored query migrates as today; the stage list
    (a query-backed model's `source_queries`, or a memory's single query) validates into `SlayerQuery` objects
    (failure → no repair); per stage, each site resolves through its scope (host when no hops); only DATE / TIMESTAMP
    repair. A sibling-stage host has untyped columns, so a bare reference on it stays verbatim; hops off it through
    extension joins resolve normally. Only `filters` change; every other character of a repaired filter is kept.
    (default)
12. **Operator coverage is pinned by tests** asserting the selected spans and the exact repaired text for `==`, `<>`,
    every ordering operator, parenthesised comparisons, and all-literal versus mixed `IN` / `NOT IN`. (codex)

## Risks / Trade-offs

- [Views cost a dict upgrade of every datasource model] → built only when a filter has a legacy-literal site, once per
  load pass per datasource (memoised with the pass).
- [Factoring `_model_from_stored`'s dict steps could change a load] → the existing stored-upgrade suites and the
  0.10.2 / 1.8.2 corpus tests must pass unchanged.
- [Recording literal offsets in the shared parser could change parsed-node identity] → offsets sit outside equality
  and hashing, as `Placeholder.start`; the full suite guards it.
- [A below-v8 peer lacks column types without refinement] → its literals stay verbatim, as today (non-goal).
