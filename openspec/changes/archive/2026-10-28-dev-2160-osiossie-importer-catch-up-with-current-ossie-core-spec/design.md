## Context

See proposal.md (Why) and `specs/importers/osi-import/spec.md` (behaviour). Upstream pin: `open-semantic-interchange/OSI@89e2e17` (2026-10-27), `core-spec/ossie-schema.json` `version: 0.2.0.dev0`.

Current state that shapes the approach:
- `slayer/osi/models.py` requires `semantic_model: list[...]` and a closed `OSIDialect` enum; `extra="ignore"` silently drops `datasets[].metrics`, `datatype` and `Expression.sql`.
- `slayer/osi/converter.py` flattens all documents into one model dict, exposes every introspected column plus field overlays, resolves names against all of them, and forces any `is_time` field to TIMESTAMP. Dialects: ANSI stored verbatim, SNOWFLAKE/DATABRICKS re-rendered to sqlglot's *default* dialect — yet `Column.sql` is later parsed in the target datasource's dialect (`slayer/sql/column_expansion.py`).
- `slayer/osi/expression.py` turns a metric's SQL into a formula and materializes every non-bare aggregate operand as a hidden DOUBLE column. Main's formula language already accepts expression operands (`sum(amount - cost)`, `sum(amount - customers.discount)`) and CASE/`iif` inside aggregates (`docs/concepts/formulas.md`).
- `dialect_for_ds_type` (`slayer/sql/dialects/__init__.py`) is the canonical datasource-type → sqlglot dialect mapping.
- Principles that govern this change (`architecture/system.arc42.md` §3): P6 (SQL as sqlglot AST), P10 (two expression layers; one Mode-B allowlist, extended never forked), P12 (Pydantic, no dataclasses), P16 (logical names, physical once: join pairs and keys name `Column.name`; the base column's bare `sql` is the physical spelling).

## Goals / Non-Goals

**Goals:**
- One internal document shape; one expression path (select → parse once → render per surface); one resolution scope per document.
- Pinned upstream schema with an offline validation + model-coverage test and an online scheduled drift check.

**Non-Goals:**
- Re-import overwriting user-edited models (all importers) — DEV-2162.
- Changing `KNOWN_OSI_VERSIONS` (upstream has cut no release; only `osi-0.1.1-rc1` is tagged).
- Emitting DEV-2101's native `FILTER` / SQL-grammar formulas (2101 is unmerged; the CASE lowering is semantically identical and stays valid after it merges).
- Mapping Ossie `unique_keys` onto `Column.unique` (kept in model meta as today).

## Decisions

Tags: `(user)` = chosen by the user; `(default)` = obvious default taken while planning; `(codex)` = Codex plan-review finding folded in; `rejected (codex)` = Codex finding rejected, with reason.

**D1. Visibility and name resolution — strict for current-spec, legacy unchanged. (user)**
Options: (A) current-spec models expose only declared fields and resolve metric/key/relationship names against declared fields with the spec's qualification rules, legacy-wrapper documents keep today's lenient behaviour; (B) strict resolution but every introspected column stays visible. Took A: the spec defines fields as a dataset's public interface, so the model mirrors it (no `order_id` beside `o_orderkey`), resolution agrees with OSI#486/#649, and the document shape tells us which rules the author wrote against.

**D2. Normalization — parse once, render for the target. (user)**
Options: (A) parse the selected expression once with a reader picked by its dialect (`OSSIE_SQL_2026`/`ANSI_SQL` → sqlglot's Snowflake reader, vendor token → its sqlglot dialect) and render every raw-SQL surface in the target dialect, storing target-native expressions verbatim; (B) store `OSSIE_SQL_2026`/`ANSI_SQL` verbatim and fix only the vendor path. Took A: a planning probe showed the default reader leaves `IFF`, `ZEROIFNULL`, `DATEADD` as anonymous functions while the Snowflake reader transpiles them correctly to Postgres, DuckDB, MySQL, BigQuery and T-SQL; verbatim storage makes portable Ossie SQL fail at query time on most backends (`||` is OR on MySQL).

**D3. Build on main, not stacked on DEV-2101. (default)**
The user asked whether this fits DEV-2101's SQL-compatible Mode-B grammar. Fields stay Mode A either way (no Mode-B column kind exists). For metrics, main already accepts expression operands and CASE, and `agg(x) FILTER (WHERE p)` → `agg(CASE WHEN p THEN x END)` is exactly 2101's own lowering and the spec's identity for NULL-ignoring aggregates (every Ossie aggregate). Nothing needs rework when 2101 merges beyond its one-line `parse_formula` → `parse_expr` swap.

**D4. Metric `datatype` → measure meta only. (user)**
Options: (A) `meta.osi_datatype`, never `ModelMeasure.type`; (B) map to `ModelMeasure.type`. Took A: `ModelMeasure.type` drives an outer CAST — `Decimal`→DOUBLE loses exactness, `Integer`→INT can reject sums beyond 32 bits — while Ossie's `datatype` describes the logical value, not a cast request.

**D5. Drift guard. (user)**
Options: (A) vendored schema + validation test + weekly drift workflow; (B) none. Took A.

**D6. One internal flat shape. (default)**
The parser checks the raw mapping and yields flat documents only; a legacy wrapper (array or object) becomes one flat document per model, flagged `legacy`. The converter never sees the wrapper.

**D7. Dialect selection order. (default)**
`--dialect` override → top-level `sql` → `OSSIE_SQL_2026` → `ANSI_SQL` → target-native → other readable SQL dialect (info note). Keeps today's vendor-dialect fallback; `--dialect` default becomes unset.

**D8. Inline operands, hidden raw-SQL column only as fallback. (default)**
Replaces "always materialize a hidden DOUBLE column": inline is what main's formula language already expresses (including cross-dataset operands); the fallback keeps today's coverage for operands outside the formula allowlist (P10: the allowlist is not forked — non-expressible operands go to Mode A).

**D9. `datatype` mapping, precedence, cross-check; `is_time` as a role. (default)**
Database type wins, declared `datatype` is the fallback (as the tracker issue asks), disagreement is an info entry. `is_time` follows the spec's defaulting and never retypes a column.

**D10. Identifier case. (default)**
Unquoted references match case-insensitively, quoted exactly; multiple matches are ambiguous; emitted references use the declared name.

**D11. Empty keys reported and ignored. (default)**

**D12. Current-spec relationships resolve within their own document. (default)**
The spec defines no cross-document references.

**D13. PK inheritance for a renamed field. (default)**
With no declared `primary_key`, a bare field renaming an introspected PK column keeps the flag — otherwise field-only visibility would lose the key and with it join-safety proofs.

**D14. `KNOWN_OSI_VERSIONS` unchanged. (default)**

**D15. `jsonschema` as an explicit dev dependency. (default)**
It is only present transitively today.

**D16. Field references validated against the live source. (codex)**
Omitting undeclared columns from the model must not let typos through: column expansion passes unknown names through physically without proving they exist (`column_expansion.py`), so field expressions keep being checked against the introspected schema before pruning.

**D17. DISTINCT with FILTER. (codex)**
`COUNT(DISTINCT x) FILTER (…)` → `count_distinct(CASE …)`; `SUM/AVG/MIN/MAX(DISTINCT …)` stay reported, with or without FILTER (no SLayer form exists).

**D18. FILTER predicates must be row-level. (codex)**
A predicate with an aggregate, window or subquery is reported, as is one that reads another dataset.

**D19. Inline operands go through a SQL-AST → formula renderer and the canonical formula parser. (codex)**
Emitting `sqlglot.sql()` text directly can produce operators/literals the formula grammar rejects; each inline operand is rendered from the AST into formula syntax and validated by the formula parser, falling back to the hidden raw-SQL column on failure.

**D20. One datasource-type → dialect mapping. (codex)**
`dialect_for_ds_type` drives target-native matching, reading, rendering and probing; OSI tokens match sqlglot dialect names case-insensitively.

**D21. Importer-side raw-SQL parsing uses the target dialect. (codex)**
The converter's own validation parses (cross-model ref checks, single-ref typing, operand ownership) parse in the target dialect, not sqlglot's default.

**D22. Key presence tracked separately from emptiness. (codex)**
An empty `primary_key` is reported and does not count as absent, so it never triggers D13's inheritance.

**D23. Computed or filtered key fields are reported as not representable. (codex)**
SLayer join pairs need unfiltered base columns (`join_key_error`); a declared computed field resolves by name but cannot key a join.

**D24. Per-document resolution scope. (codex)**
Each flat document resolves relationships, joins and metrics within its own datasets; duplicate dataset names across the import set stay refused (model names are unique within a datasource).

**D25. Deterministic candidate order within a selection step. (codex)**
First entry in document order wins; later duplicates get an info note; blank tokens are invalid.

**D26. Explicit raw-mapping shape detection. (codex)**
Mixed, empty, null and scalar wrappers are detected before Pydantic parsing (`extra="ignore"` would otherwise discard one shape's keys silently).

**D27. Schema-to-model coverage test. (codex)**
Every property in the vendored schema is either modelled by the OSI Pydantic models or on an explicit ignore list, so a future schema bump surfaces unmodelled fields; legacy fixtures are asserted to be outside the current schema.

**D28. Robust drift workflow. (codex)**
Compare canonicalized JSON, print the upstream commit SHA, and fail distinctly on fetch errors vs schema differences.

**D29. End-to-end import test. (codex)**
An in-process DuckDB test runs a flat current-spec document through the CLI import path into storage, reloads it and executes queries.

**D30. Re-import overwrite deferred to DEV-2162. (codex, user)**
Codex flagged that every importer (`import-dbt`, `import-cube`, `import-osi`) saves unconditionally, which conflicts with P14. Pre-existing and shared by all importers; the user chose a separate issue.

**rejected (codex): render target-native SQL from the AST too.** Conflicts with honouring author SQL: an expression already in the target's dialect is valid there and is stored verbatim; only the importer's validation parsing changes (D21).

**rejected (codex): case-insensitive collision checks among declared names.** SLayer's column/measure namespace is exact-case, so names differing only by case are distinct and cannot clash; references to them are ambiguity-checked (D10).

**rejected (codex): declared `datatype` overrides the database type.** The tracker issue asks for fallback + cross-check, and `Column.type` drives SQL casts against the real database; the docs state the deviation from the spec's consumer guidance.

**rejected (codex): make `Time` eligible as the default time dimension.** SLayer's time grains need DATE/TIMESTAMP; a time-of-day cannot be bucketed by month or week, so UNKNOWN plus the role in meta is right.

## Risks / Trade-offs

- [sqlglot transpilation is imperfect (e.g. `DATE_TRUNC` to SQLite renders an invalid `TIMESTAMP_TRUNC`)] → derived fields are type-probed against the live database at import, so a mis-transpiled expression fails the probe; parse failures are reported per entity.
- [ANSI expressions that used to be stored verbatim may be rewritten] → only into what sqlglot considers equivalent; target-native expressions stay verbatim.
- [Current-spec imports no longer expose undeclared source columns] → intended (D1); documented in `docs/osi/osi_import.md`.
- [A red scheduled drift job needs an owner] → the job prints the upstream SHA and the diff, so updating the vendored copy is mechanical; it never gates PRs.
- [DEV-2101 merges while this is in flight] → the only overlap is the `parse_formula` call in `converter.py`; merge, not rebase.

## Migration Plan

No stored-model migration: the change affects only what a new import produces. `--dialect`'s default changes from `ANSI_SQL` to unset; documents that relied on ANSI being picked over `OSSIE_SQL_2026` now get the portable form, which the spec makes authoritative.
