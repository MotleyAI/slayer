## Context

See proposal.md for why. Starting state on `main` after DEV-2101:

- **Date parts.** `month(x)` and its siblings parse to an `AggCall` named after the granularity. Binding turns it into a `TimeTruncKey` (`binding._bind_granularity_call`). `date_trunc` re-dispatches its unit as a callee (`syntax._date_trunc`). `date_part`, `EXTRACT` and the date-part aliases build a `date_part` `ScalarCallKey`.
- **Functional time dimensions.** `core.query._split_functional_dimensions` moves built-in `gran(x)` dimension strings into `time_dimensions` at construction. Custom ones move at binding (`bind_inputs._resolve_granularity_calls`).
- **Names.** Every lookup is exact-case: `get_column` / `get_measure`, the join walker, stages, aliases. The exceptions are model aggregations (exact, then a unique case-insensitive match) and custom granularities (casefold). `DottedRef.quoted` records only whether *some* segment was quoted.
- **`=` inside calls.** `_KWARG` turns `name = value` into a keyword argument at parse time. Built-in value-first calls force a positional first argument.
- **`first` / `last`.** They rank by the dialect's native NULL order (`sql/render/ranked.py`), so results differ by dialect.
- **Legacy spellings.** They are accepted silently. Stored migrations share the traversal helpers in `storage/rank_direction_migration.py`. The DEV-2170 rename and DEV-2211's settling handle library-colliding custom granularities in the storage load path.

Principles that apply:

- system §3 P8 (one reader of Mode B) and P11 (fail at save time);
- core P5 / P6 (typed errors; warnings surfaced twice);
- engine P2 (parsing is pure syntax) and P8 (amended, see Decisions);
- storage P3 / P3.2 (amended, see Decisions);
- semantics §4 P1 (canonical spelling), §4 P2.1 (one meaning per dialect) and Axiom 4 (NULL inputs are skipped);
- sql P1–P3.

## Goals / Non-Goals

**Goals:**

- One dispatch path per meaning. The eight names become entries of the date-part alias table. `date_trunc` is the only text that builds a `TimeTruncKey`. One helper produces the `date_trunc` remedy for every position that needs a bucket.
- One name matcher in `core`, used by every namespace lookup and pinned by a law test, so no lookup compares names on its own.
- One deprecation sink for the legacy spellings, and one span rewriter shared by save and the stored migration.
- Result keys stay stable for migrated stored queries.

**Non-Goals:**

- The colon spellings `x:agg` and `col:gran`. DEV-1920 removes them in 2.0: the refusal, the stored migration, the colon producers and the colon spec changes. This change leaves colon handling as it is on `main`. Approved scope cut, decision 23.
- Additive Ossie gaps: `OVER (...)`, `DATEADD`, `IFF`, `TIME`, the `MILLISECOND` and `DAYOFWEEK` parts. These are post-2.0.
- The rank family's NULL handling stays as it is (decision 4).
- Mode-A text: `Column.sql`, `sql_table`, model `filters`, custom-aggregation `formula`, `AggregationParam.sql`.

## Decisions

### Structure

- **Flip.** `year` … `second` join the date-part alias table (`DATE_PART_ALIASES`), each mapping to its own part. Granularity names leave the call dispatch.
  - `date_trunc` parses to a dedicated node carrying the literal unit and the expression. Binding validates the unit, built-in or one of the datasource's granularities, and builds `TimeTruncKey` directly. The unit is never resolved as a callee.
  - The functional-dimension rewrite recognises only `date_trunc` calls.
  - A call named after a granularity that resolves to no aggregation raises a typed error with the `date_trunc('<g>', x)` remedy.
  - One remedy helper serves the positions that need a bucket: the time-dimension entry, the transform axis, the spine, `whole_periods_only`, and the date-shaped-literal comparison. Given a `date_part` value over a temporal operand, it returns `date_trunc('<part>', <operand text>)`.
- **Case.** `core` gets one matcher. Given the declared names and a reference (spelling plus a quoted flag), it returns the exact match, else the unique case-insensitive match, else a typed `AmbiguousReferenceError` (or none). Every lookup goes through it: `get_column` / `get_measure` / `get_aggregation`, `join_walker.matching_edges`, stage resolution, the alias map, model lookup, and transform and function names.
  - Quoting is kept per path segment: `DottedRef` carries a quoted flag per segment, preserved through `drop_root_qualifier` and every path rewrite.
  - Result keys carry the declared spelling.
  - The save-time check compares the namespaces of the submitted document with the persisted raw document of the same identity.
- **`=` inside calls.** In every non-scalar call, an unparenthesised `bare = expr` argument parses as an ordered neutral node. Once the callee resolves, binding classifies each one as a keyword argument or a comparison (decision 18). Duplicate-keyword and positional-after-keyword checks run after that.
- **`first` / `last`.** The ranked renderer orders by a null flag first, so non-NULL keys come first in both directions. It carries the picked row's key through the projection and returns NULL when that key is NULL. The ordering is built in the dialect-neutral renderer as AST (sql P1), never from native defaults.
- **Legacy spellings.** The reader records each legacy span with its canonical twin. A `ContextVar` sink, drained by the engine, emits `warn_deprecated(..., removal="3.0")` and a structured payload once per spelling per query or save. Save rewrites only the recorded spans. The stored migration uses the same span rewriter.
- **Stored versions.** One step: SlayerModel 18→19, SlayerQuery 9→10, Memory 7→8 (`stored_only`). It applies built-in bucket calls → `date_trunc`, legacy spans → canonical, and `agg` → `agg_aggregation`.
  - One recursive walker visits the Mode-B fields: query measures, filters, dimensions, time dimensions, order, `main_time_dimension`, saved-measure formulas and `Column.filter` if it is Mode B. It recurses into `source_queries`, inline source models, model extensions and memories.
  - Custom-granularity calls are rewritten in the storage load path. The raw version is captured before migration, the datasource's names are settled (DEV-2170 / DEV-2211), pre-bump documents' custom calls are rewritten, then the versioned migrations run. The whole sequence is idempotent.
- **Facade.** `translator._detect_time_grain` keeps `DATE_TRUNC`, `TIMESTAMP_TRUNC` and the Sunday-week wrapper. A new extraction-projection path maps `MONTH(col)` and its siblings, `EXTRACT` and `date_part` to a `date_part` expression dimension under one canonical alias.

### Decisions log

1. Saving with case collisions already stored. Options: refuse only new collisions, or refuse any colliding save. Taken: **refuse only new collisions**. Grandfathered models stay editable (drift refresh) without forced renames. (user)
2. An unquoted name in a colliding store. Options: exact spelling first, then a unique case-insensitive match; or strict ambiguity. Taken: **exact first**. Existing queries keep working, and the one rule also covers function names, which can't be quoted. (user)
3. Scope of the `agg` reservation. Options: function name only, or measures too. Taken: **function name only**, matching Ossie ("does not prevent a field, metric, or column from being named agg"). This departs from the issue's "or metric". (user)
4. NULLs in the rank family. Options: keep NULL → NULL rank, or align with Ossie's window placement. Taken: **keep**. Top-N stays sensible, and Ossie defines no `rank(x)` form. (user)
5. NULL keys in `first` / `last`. Options: Ossie's window default (NULL is largest), or never pick a NULL key. Taken: **never picked**. This follows Axiom 4, matches decision 4, and keeps "latest" meaningful. (user)
6. More legacy spellings. Options: add bare `None`, the method-call form and `col:gran` to the pattern, or only the issue's four. Taken: **add all three**. `col:gran` later moved to DEV-1920 with decision 23. (user)
7. Facade `MONTH(col)`. Options: add extraction projections (including `EXTRACT` and `date_part`), or refuse it in SELECT. Taken: **add extraction projections**, an extension beyond the issue's text. (user)
8. Fresh legacy spellings on save versus engine P8. Options: store verbatim, or canonicalise on save with a P8 edit. Taken: **canonicalise**. (user)
9. `architecture/python.engine.arc42.md` P8: "8. **Models persist verbatim**: `save_model` stores the author's spelling unchanged, a deprecated spelling excepted, which is stored as its canonical twin; normalization applies to queries at execute time only. The one exception: a saved query stores each stage's inferred population as its `source_model`." (user, approved wording)
10. `architecture/python.storage.arc42.md` P3.2: "**3.2** Library growth never invalidates a stored artefact: a new library function yields to a model's own definition of it inside that model; any other newly reserved name ships with a migration." (user, approved wording)
11. Ingested physical columns that differ only in case get a `_<n>` suffix, with `sql` naming the physical column. (default; stated in the interview, no objection)
12. Internal producers stop emitting the deprecated spellings, so SLayer never warns on its own text. Colon producers belong to DEV-1920. (default; stated in the interview, no objection)
13. Quoted names match the declared spelling exactly, not Ossie's upper-cased normalised form (`"ID"` does not match `id`). SLayer names are declared spellings, and Ossie leaves the quoted-versus-unquoted comparison to the implementation. (default)
14. Case folding uses `casefold`, as custom granularities already do. (default)
15. A "date-shaped literal" is an instant, a period literal other than a bare `YYYY`, or a relative token. A bare `YYYY` and numeric strings stay legal against an extraction. (default)
16. Model `filters` are Mode A, so they aren't migrated, although the issue lists them under stored text. (default)
17. `date_trunc` units include custom granularities and `week_sunday`, as the issue rules. The corpus spec said built-in units only. (default)
18. The `=` rule covers transforms too and drops the first-argument exemption, giving one rule for every non-scalar call. (default)
19. Custom-granularity bucket calls are converted in the storage load path using the datasource's names. A per-document migration can't tell a custom granularity from a custom aggregation. (default)
20. Fresh-input deprecation warnings fire once per spelling per query or save. Stored-migration conversions are only logged. (default)
21. Transform names become case-insensitive (Ossie: function names are case-insensitive). (default)
22. Result-name collisions in fresh queries are refused on every dialect, not only on case-insensitive ones, because the alias namespace is now case-insensitive. (default)
23. The colon spellings (`x:agg`, `col:gran`) are removed in 2.0, not deprecated, and moved to DEV-1920 (rescoped, with its own worktree). This supersedes the colon part of decision 6. (user)
24. DEV-1956 is a duplicate of DEV-1920. Its importer half had already landed, and its facade half moved there. (user)
25. Quoting is tracked per path segment through every rewrite. (codex)
26. Stored-text conversion is driven by a Mode A/B field matrix and one recursive walker. Custom-aggregation `formula` (a SQL template), `AggregationParam.sql`, `Column.sql`, `sql_table` and model `filters` are Mode A and aren't migrated. This corrects the first draft, which listed aggregation formulas and a `Column.expression` that doesn't exist on `main`. (codex)
27. `date_trunc` is a dedicated parsed node, and binding validates the unit and builds the bucket directly. (codex)
28. The custom-granularity conversion has an explicit load pipeline: capture raw versions, settle the datasource's names, rewrite pre-bump calls, then run the versioned migrations. It is idempotent. (codex)
29. `name = value` arguments of non-scalar calls parse as ordered neutral nodes, classified at binding, with the keyword checks after classification. (codex)
30. "New collision" is judged against the persisted raw document of the same identity, and the warning is tested on query and facade responses. (codex)
31. SQL shape for `first` / `last`: non-NULL keys before NULL keys in both directions, the picked key carried through, and a NULL result when it is NULL. (codex)
32. An explicit result-key table: `month(x)` and `date_part('month', x)` share a key, and a migrated `date_trunc` bucket keeps the old bucket key. (codex)
33. Ingestion suffix: the smallest `_n` not already used by persisted, incoming or generated names, deterministic. (codex)

## Risks / Trade-offs

- [Risk] The meaning change is silent for fresh `month(x)` used as a dimension: it returns month-of-year instead of buckets. → It's a typed INT dimension. The date-shaped-literal error catches the most common misuse. The release notes call it out (comment on DEV-2220).
- [Risk] Case-insensitive lookups could change which name a reference reaches on stores with collisions. → The exact spelling wins (decision 2), so a reference that resolved before resolves to the same name. The only new outcome is the typed ambiguity error.
- [Risk] Concurrent stored-version bumps (DEV-2102, DEV-2021, DEV-1607, DEV-1920). → Whichever merges second renumbers its step. The migration tests pin source versions symbolically.
- [Risk] Concurrent edits to `compatibility/removal-tripwire` and its test pins (DEV-2231 empties the 2.0 pins; DEV-1920 drops the colon sentence). → The second to merge reconciles.
- [Risk] The stored `agg` rename rewrites `agg(` calls store-wide. If one model defines both `agg` and `agg_aggregation`, other documents' calls would reach `agg_aggregation`. → This is an accepted edge case: one store would have to define both names. The model itself keeps `agg` unrenamed and logs a warning.
- [Trade-off] Fresh deprecated spellings are canonicalised on save, so "verbatim" gets one meaning-preserving exception (engine P8 as amended).

## Migration Plan

- One versioned step, plus the load-path rewrite of custom-granularity calls. Both are idempotent, and an interrupted load completes on the next load.
- A store written by this release still opens on the previous release, because `date_trunc` and the canonical spellings already parse there.
- There is no rollback beyond restoring the store, since stored text is upgraded on save.
