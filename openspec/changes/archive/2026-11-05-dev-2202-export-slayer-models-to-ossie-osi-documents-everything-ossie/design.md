## Context

The importer (`slayer/osi/`: `parser`, `converter`, `expression`, `dialects`, `source`, `models`) turns Ossie documents into SLayer models and reports what it cannot express. This change adds the reverse direction and teaches the importer to read back what the exporter writes. Measures are Mode-B formulas; the engine binder (`slayer/engine/binding.py` `bind_expr(..., allow_measures=True)`) already turns a formula into a `ValueKey` tree (`slayer/core/keys.py`) with measure references inlined. Aggregate SQL is built inside `SQLGenerator._build_agg` (`slayer/sql/generator.py`) from free helpers in `slayer/sql/render/aggregates.py` and per-dialect builders in `slayer/sql/dialects/`.

Ossie semantics (apache/ossie, current `0.2.0.dev0` drafts): the core spec defines no cross-dataset evaluation; the Layer 3 query interface (merged 2026-11-04) evaluates each aggregate call of a model-scoped metric as a component over the rows of the one dataset its arguments read, joins outward only over unambiguous many-to-one `LEFT JOIN`s, stitches components on the dimension tuples, and rejects aggregates reading several datasets (`E_AMBIGUOUS_MEASURE_GRAIN`) and ambiguous paths (`E_AMBIGUOUS_PATH`). A probe (DuckDB; a customer with no orders) confirmed SLayer evaluates an `orders` measure `count_distinct(customers.id)` / `sum(customers.discount)` over all customers, grouped and ungrouped — the same as Layer 3. Grouped by an `orders` dimension SLayer broadcasts where Layer 3 refuses; that is a query-capability difference, not a metric-meaning one.

Architecture: all new code lives in `slayer/osi/` (the `importers` bucket); its edges to `engine`, `sql` and `core` are already licensed, so no model or arc42 change. Applicable principles (`architecture/system.arc42.md` §3): 3.6 every emitted SQL is a sqlglot AST; 3.9 extension payloads store canonical dotted paths; 3.10 Mode-A `Column.sql`/`filter`/model `filters` become field and source SQL, Mode-B formulas go through the binder and the canonical scalar renderer (no forked allowlist); 3.11 the extension payload is versioned; 3.12 Pydantic only; 3.13 payload-kind dispatch fails closed; 3.15 access control fails closed (access-tagged models are not exported without extensions); 3.16 field names are `Column.name`, physical spelling applied once when field SQL is rendered; 3.17 any metadata the export needs goes through the engine. The new capability `importers/osi-export` is covered by the `importers` node's existing `specs ['importers']`.

## Goals / Non-Goals

**Goals:**
- Every Ossie entity the exporter writes means in Ossie exactly what it means in SLayer.
- SLayer → Ossie → SLayer is lossless, and an Ossie edit by a third party always wins over stale extension content.
- Executed and exported aggregate SQL come from one code path.

**Non-Goals:**
- MCP or REST export/import surfaces.
- Rendering joined-path model filters into a standalone query source.
- Restoring extension content that disagrees with an edited document.
- Exporting datasource connection details, memories, or sampled values.

## Decisions

Tags: `(user)` user's choice, `(default)` taken as the obvious default, `(codex)` Codex finding folded in.

1. **Lossless round trip through a `SLAYER` custom extension the importer reads, plus `--no-extensions`.** Alternative: plain Ossie only, reporting everything it loses (INNER joins, off-home measures, named joins, display attributes). The extension carries only what Ossie has no home for. (user; the flag: default from the recommendation)
2. **CLI + Python API only.** Alternative: also REST and MCP. Import is CLI-only; export is an admin action. (user)
3. **Query-backed models: SQL snapshot `source` + saved queries and variable defaults in the extension, restored on import.** Alternative: snapshot only (freezes the model and its variables). (user)
4. **Row filters: own-model filters become a wrapped query `source`; a model with a joined-path filter is reported.** Alternative: a second, hand-written join renderer for to-one paths — rejected as a duplicated code path. (user)
5. **Measures: the extension carries the formula; the importer restores it only when it agrees with the document's SQL, otherwise converts the SQL.** Alternatives: SQL only (formulas come back inlined); formula restored unconditionally (overrides third-party edits). (user)
6. **The same agree-or-fall-back rule applies to sources.** (user)
7. **The importer learns `STDDEV_*`, `VAR_*`, `CORR`, `COVAR_*`, `APPROX_COUNT_DISTINCT`.** Alternative: leave it, and report exported-but-unreadable metrics. (user)
8. **Field and source expressions: portable `sql` only when exact, else dialects-only with no `sql`.** Exact = the portable rendering read back with the importer's portable reader and rendered in the datasource dialect equals the datasource rendering. Deviates from the issue ("also a dialects entry"), because the importer and the schema both treat a present `sql` as authoritative. (user)
9. **`Column.filter` exports into the field as `CASE WHEN p THEN x END`**, replacing the issue's metric-level `FILTER`, because the mask applies in every position; the extension keeps the value/filter split. (user)
10. **Undetermined join cardinality is exported only when the declared keys prove a direction, otherwise reported.** Alternative: assume many-to-one (asserts an unproven uniqueness). (user)
11. **Columns reading joined models are SLayer-only (extension), not Ossie fields**, because Ossie fields are row-level attributes of their own dataset. Alternative: an Ossie field holding a SLayer join path. (user)
12. **Transform, ranked and windowed/partitioned measures, plus custom aggregation definitions, travel in the extension** and are restored on import. Alternative: reported, as the issue first listed. (user)
13. **`--no-extensions` + access tags → the model is reported, not exported** (§3.15 fail-closed: re-import would make it public). (default)
14. **One aggregate dispatch.** A pure function over a fully resolved aggregate specification (aggregation, value AST, bound parameters, input type, template formula, dialect) replaces the body of `SQLGenerator._build_agg`; the generator keeps scope, join and parameter resolution and builds that specification; the exporter builds the same specification over logical field ASTs. (default; shape from codex)
15. **`--models` is a strict subset**; anything needing an excluded model is reported with "add it". (default)
16. **Generated names come from one document-wide allocator** (relationships, model-scoped and relocated metrics), reserving existing names first; identifiers ambiguous under case-insensitive matching are double-quoted. (default + codex)
17. **Metric expressions are always standard portable SQL built from the dialect-neutral builders**; only an approximate distinct count is dialect-only. Decision 8's exactness test applies to fields and sources only, because a metric is a semantic tree, not free SQL, and SLayer's own per-dialect lowerings (the SQLite percentile UDF, MySQL/ClickHouse statistics) would otherwise make standard metrics unreadable. (user)
18. **Restore-if-agrees is a fingerprint check.** Each payload stores a versioned hash over the normalized sqlglot ASTs of the exact Ossie expressions/source it was written with (each parsed with its own reader) and over the payload's restorable content. The importer recomputes it from the document: equal → restore; different → Ossie content + info entry. No re-rendering or query compilation is needed to decide. Alternative: re-render at import and compare normalized ASTs (needs the exporter and an import-time compile against a half-built graph). (user, codex finding 4)
19. **Relationships always carry Ossie's FK→unique direction**; the extension records the original declarations (model, cardinality, type, name), restored after their endpoints and key pairs validate; without extensions, one-to-one and reversed one-to-many joins are exported and reported lossy. (codex)
20. **Import is two-phase**: decode and validate every payload; build plain models; then restore extension content per entity on fresh, fully validated models (query-backed models after the models their queries read, through the engine's create-from-query validation), isolating each failure to its entity with a fallback to the Ossie content. (codex)
21. **Database access**: export reads metadata only through the engine; table and sql-mode exports are tested against a client that fails on every call. (codex)
22. **One datasource-type ↔ Ossie-token ↔ sqlglot-reader map** in `slayer/osi/dialects.py`, used by export, by import's target-native selection, and by native-verbatim detection; every supported datasource alias is tested. (codex)
23. **`is_time` round trip**: the importer records an explicit `is_time` value (true or false); the exporter writes `is_time` only where Ossie's datatype default differs from the recorded role. (codex)
24. **Keys** are normalized, deduplicated and validated (every member an exported plain, unfiltered field); rejected sets are reported individually; the same key inventory proves join cardinality. (codex)
25. **Masking**: with the mask in the field, `COUNT(*)` is unaffected, every column-reading aggregate reads the masked value, and parameter columns (`other=`, `weight=`) read their own masked fields; tested per aggregate against NULL predicates and empty or all-NULL groups. (codex)
26. **Foreign extensions** are validated against the schema's extension shape and re-emitted byte-for-byte; malformed ones are reported; each entity carries exactly one `SLAYER` entry. (codex)
27. **Report contract**: every entry has entity, reason and workaround; `dropped` = not exported; `info` + `extension_only` = SLayer-only; `info` + `lossy` = degraded under `--no-extensions`. A `ConversionReport` base (entries, `render_report`, `tally`) is extracted in `slayer/ingest_report.py` and shared by import and export results. (codex)
28. **Round-trip test matrix**: grouped and ungrouped across every exported dimension, empty and all-NULL inputs, every relationship direction and cardinality, case-sensitive identifiers, filtered booleans, the count family, two-argument statistics, and each measure added next to all dimensions (cardinality invariance, system §3.8). (codex)
29. **Ossie component semantics.** A measure exports as a metric only when every aggregate reads exactly one model (each a Layer 3 component) and every model it reads is reachable from the measure's model by one route; a measure whose aggregates all read one model D exports as a dataset-scoped metric on D (its SLayer model restored from the extension); aggregates reading several models (including over SLayer-only columns) are SLayer-only. This amends decision 11's original "expand into model-scoped metrics". (user, codex finding 2)
30. **Rejected (codex finding 2, part):** restricting export to aggregates whose population a naive join preserves. Ossie Layer 3 aggregates each component over its own dataset's rows, which is SLayer's rule (probe above), so the restriction would drop correct metrics.

## Risks / Trade-offs

- [Ossie Layer 2/3 are drafts and may change] → the exporter targets the pinned schema and the Layer 3 component reading; the decisions above name the clauses relied on, so a spec change maps to a known decision.
- [Fingerprint mismatch after a purely cosmetic third-party edit] → the fallback is the exact Ossie reading, plus an info entry; nothing is lost.
- [A restored query-backed model regenerates SQL with a newer engine] → intended: SLayer's saved queries are the source of truth for SLayer; the snapshot stays for foreign consumers.
- [Lifting `_build_agg` touches the hot query path] → the full existing suite covers it; the generator's behaviour must not change.
- [Name quoting for case-fold ambiguity makes some expressions noisier] → quoting only where needed.
