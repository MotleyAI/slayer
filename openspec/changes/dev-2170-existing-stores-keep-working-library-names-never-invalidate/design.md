## Context

See proposal.md (Why). Tracker: DEV-2170. Base: the DEV-2137 tip; the stack is not on main yet, so `origin/main` is merged in (never rebased) once it lands.

Current state that shapes the approach:
- Save-time bans: `Aggregation._reject_transform_names` and `_reserved_granularity_names` (`slayer/core/models.py`) refuse every library name, so each library addition retroactively invalidates stored names.
- The parser picks a scalar over an aggregation at parse time (`_dispatch_call` in `slayer/core/syntax.py`). It canonicalises spellings (`strpos` → `instr`, `date_trunc`, `json_extract*`) and refuses scalar-named calls with kwargs, the wrong arity or method form. Engine P2 forbids the parser from seeing scope.
- Built-in aggregation semantics are chosen by name alone at several sites: `elaborate_env` list refusal, `keys.py` list typing, `generator.py` element typing, `empty_value`, and others. A formula-backed override is honoured only at render time.
- Drift attribution scopes to read *models* only (`rendered.reads`), and `_maybe_raise_schema_drift` wraps on any drift in them.
- Memories are serialised without dropping `None`, while models and datasources drop it.
- `sql_quote_strings` (the SlayerModel v15 / SlayerQuery v6 / Memory v4 step) trusts the version stamp alone. The v16/v7/v5 time-dimension step only turns column objects into dotted text and rewrites no text, so it needs no gate.

Principles that apply: system 10, 11, 11.1 (still targeted at DEV-1844), 11.2, 14; engine P2, P3, P10; core P5.

## Goals / Non-Goals

**Goals:**
- Make library-name collisions structurally harmless: model-local ones are resolved by scope, wider ones are migrated, and future growth is pinned by a test.
- Make built-in aggregation semantics a function of the owner's definition, not of the name.
- Element-level drift attribution derived from the plan.

**Non-Goals:**
- Lazy write-back on load (11.1, DEV-1844).
- Migrating unversioned client payloads (MCP/REST create, the query tool, `slayer models create -f`); these are release notes only.
- Rollback readability of text already migrated to the new grammar, `||`/`::`/`CAST`/`FILTER`/`BETWEEN` on the previous release, or `ARRAY` columns there; these are release notes only.
- Importers typing list columns (DEV-2171).
- Shadowing transforms, date-part aliases, granularities or built-in aggregations by a stored definition.
- Validating an author's declared type against the data (SQLite sampling).

## Decisions

### D1. Model-local shadow, resolved at bind (user)
- A custom aggregation may take any name in the scalar/list function set or the parser spellings.
- The parser stays pure syntax (P2) but stops destroying information:
  - a parsed call keeps its callee **as written**, as well as the canonical scalar it maps to;
  - a call that is ill-formed as a scalar or spelling (kwargs, arity, method form, a spelling's shape check) but well-formed as an aggregation call is emitted as a deferred aggregation candidate with the scalar's error attached, rather than raised.
- Binding is a pure function of (parsed, scope, bundle) (P3). It resolves the owning model of the value argument with the same owner rule as `aggregation_definition`. If that model defines the name, the call is the aggregation; otherwise it is the library function, or the attached scalar error is raised unchanged.
- Alternative rejected: moving all call dispatch to bind. Downstream parse-time utilities depend on early classification, and nothing is gained over carrying the callee as written.
- Bans that stay for aggregations: transform-only names, date-syntax names, granularity names.

### D2. One aggregation-definition resolver, case-aware (default + codex)
`aggregation_definition`, eligibility (`allowed_aggregations`) and binding share one resolver:
- an exact-case match wins;
- otherwise a unique case-insensitive match;
- otherwise, with several case-insensitive matches, a typed ambiguity error naming every candidate.

A save-time ban on case-folded duplicates was rejected, because a new ban could itself invalidate stored models (11.2).

### D3. Built-in semantics iff no formula-backed definition (user)
One predicate decides whether a call has built-in semantics: the owner has no formula-backed definition of that name. Every name-classification site uses it: list typing and refusal, element typing, empty value, ranked dispatch, and parameter order. The `collect` regression falls out. No warning, since this is the documented override (`docs/concepts/models.md` aggregations section).

### D4. Shadow warnings are structured and accumulate during binding (user + codex)
- A typed `LibraryNameShadowWarning` payload carries the model, the aggregation, and the shadowed function or spelling as written.
- It is emitted on model save and on load.
- At query time, binding records each shadow resolution into the prepared warnings, next to `slack_warnings`, so executed, dry-run, EXPLAIN and cache-hit responses all carry it (core P5: a Python warning plus `SlayerResponse.warnings`).

### D5. Datasource-wide collisions: a deterministic, crash-convergent rename (user + codex)
- DatasourceConfig v2→v3, stored-only; the rename itself runs on every datasource load, whatever its version, and is a no-op when nothing collides, so a granularity colliding with a later library addition is renamed too.
- The rename map is a pure function of the stored datasource's granularity set and the library vocabulary: `<name>_granularity`, else `<name>_granularity_<n>` with the smallest free `n` ≥ 2, compared case-insensitively.
- In the storage load path (precedent: the v10/v11 cross-document steps), the datasource's dependants are rewritten and persisted **before** the datasource itself. A load interrupted at any point therefore recomputes the same map on the next load and converges. Once DEV-1844 makes loads non-writing, the same pure map is simply recomputed in memory on every load.
- References are rewritten in every position the grammar accepts a granularity name. The pr-tests stage enumerates them from the grammar.
- Every call by such a name in these documents is the granularity: the rename runs on the first load after the name becomes reserved, before any document can call the new function, so no document mixes the two.

### D6. Library-growth guard (user)
`tests/test_library_growth.py` vendors a frozen snapshot of every reserved vocabulary. A name outside it fails unless it is a function or spelling (model-shadowable; a colliding stored granularity is renamed by D5 on load) or a registered migration covers it. The test also generates per-name stored-aggregation and stored-granularity cases. 11.2 is tagged `[enforced: test:tests/test_library_growth.py]`, and `docs/development.md` gets a one-sentence rule.

### D7. Rollback-safe serialisation (default + codex)
- One shared operation, recursive `None` removal, is applied after each backend's own shape conversion for all four kinds. The YAML memory layout (id as file name, learning as body) is unchanged.
- The test vendors the v1.4.1 field sets of the persisted classes. v1.4.1 is the last release; it accepts forward versions, and only `extra="forbid"` classes reject unknown keys.

### D8. Element-level drift attribution (default + codex)
- Rendering returns a typed per-model element read set alongside the model read set: columns via their dependency closure (engine P10), join keys, joins crossed, measures and aggregations used.
- An entry is attributed iff one of these holds:
  - it is a `WholeModelDelete` of cause `schema_drift` on a read model (`invalid_sql` stays excluded);
  - it is an `EditModelDelete` on a read model that removes a read column, join, measure or aggregation, or any model filter (filters apply to every read);
  - it is a re-type finding on a read column.

### D9. SQLite temporal-over-TEXT (user)
A dialect capability, `temporal_text_compatible` (true only for SQLite), makes persisted DATE/TIMESTAMP over live TEXT a non-conflict in the bucket rule. No sampling: drift means "the live schema moved", and on SQLite it didn't.

### D10. Re-type any non-list column (default)
The drift finding and `_retyped_list` cover every non-`ARRAY` persisted type, UNKNOWN included. Principle 14's wording is amended (user, approved wording).

### D11. Old-grammar gate on the string migration (user)
- Before `sql_quote_strings` rewrites a field, the text must be old grammar: the old parser's colon pre-pass, then `ast.parse(…, mode="eval")`. Otherwise the field is left unchanged and a warning names the document and field.
- `''`, `"…"` and backslash literals are valid old grammar with a different meaning, so they stay governed by the version stamp; this residue is documented. Making them no-ops was rejected (codex finding): that would change the meaning of every genuine old store.

### D12. `agg(distinct)` rewrite (user)
- SlayerModel v17→18, SlayerQuery v8→9, Memory v6→7, stored-only: a bare `distinct` followed by `)` or `,` as an aggregation's first argument becomes `"distinct"`.
- It is unconditional because `agg(distinct)` is always a parse error in the new grammar, so no valid text changes meaning. Gating it on provenance was rejected (codex finding) for that reason.
- `count(DISTINCT x)` is untouched.

### Decisions log
1. **Collision handling:** shadow vs migrate. Shadow, with a loud warning every time it happens. **(user)**
2. **Shadow scope:** model-local, for functions and spellings only. Datasource-wide and cross-kind collisions are migrated now; future ones are pinned by principle and test. "Every vocabulary" was rejected as allowing drastic self-inflicted shadows. **(user)**
3. **11.2 wording:** "Library growth never invalidates a stored artefact: a newly reserved name yields to a model's own definition of it inside that model; any other collision ships with a migration." **(user, approved wording)**
4. **The `agg(distinct)` keyword break** is in this change; no existing issue covered it. **(user)**
5. **Rollback test:** a vendored v1.4.1 field-set snapshot plus a no-`None` invariant behind one shared operation. **(default)**
6. **SQLite temporal-over-TEXT** is compatible without sampling. **(user)**
7. **Item 6:** old-grammar gate plus documented residue. **(user)**
8. **Principle 14 wording:** "…and a non-list column re-typed to the list its live column is". **(user, approved wording)**
9. **Formula overrides of built-in aggregations** are silent; only function shadows warn. **(user)**
10. **Case-insensitive shadow** of every spelling the library accepts. **(default)**
11. **Granularity rename scheme** `<name>_granularity[_<n>]`. **(default)**
12. **Re-type** covers every non-`ARRAY` type, UNKNOWN included. **(default)**
13. **Element-level attribution** from the plan; model filters always count as read. **(default)**
14. **Library-growth rule** in `docs/development.md`. **(default)**
15. **Spellings:** the parsed call keeps its callee as written; a spelling's shape errors are deferred like an ill-formed scalar's; each spelling is tested. **(codex)**
16. **Shadow resolution is case-aware:** exact, then a unique case-fold, else an ambiguity error. No save-time duplicate ban. **(codex, modified)**
17. **Attribution:** `WholeModelDelete` counts only with cause `schema_drift`; `invalid_sql` stays excluded. **(codex)**
18. **Shadow warnings** accumulate during binding; tests cover every spelling, container, and execute/dry-run/EXPLAIN/cache-hit path. **(codex)**
19. **Rename** is deterministic, with dependants persisted before the datasource, and failure-injection tests on YAML and SQLite. **(codex)**
20. **Serialiser:** `None` removal after each backend's shape conversion; YAML memory layout kept. **(codex)**
21. **Rejected (codex):** treating `''`, `"…"` and backslash text as no-ops in the old-grammar gate. They are valid old grammar with a different meaning, so the version stamp decides and the residue is documented.
22. **Rejected (codex):** gating the `agg(distinct)` rewrite on provenance. The input is always a parse error in the new grammar, so the rewrite can't change valid text.
23. **The old-grammar gate guards only the string migration:** the time-dimension step rewrites no text. **(default)**

## Risks / Trade-offs

- [The built-in function is unreachable by name inside a shadowing model] → loud warning on save, load and every response.
- [A renamed granularity breaks references from outside SLayer (client code, prompts, BI tools)] → the rename is logged, documented in `models.md` §Schema versioning, and goes in the release notes.
- [The rename writes on load, since 11.1 is not done] → deterministic map plus dependants-first persistence make every crash point converge.
- [Hand-edited old-version text using only `''`, `"…"` or backslashes is still rewritten] → documented residue; undetectable by construction.
- [The element read set misses a read path and wraps too little] → the closure is computed from the plan (P10); scenarios cover derived columns, join keys, query-backed stages and semi-joins.

## Migration Plan

- Versions bump to SlayerModel 18, SlayerQuery 9, DatasourceConfig 3, Memory 7. All new steps are stored-only.
- Rollback to v1.4.1: documents written by this release omit unset new fields. Grammar-level differences are listed in the release notes (non-goals above).
