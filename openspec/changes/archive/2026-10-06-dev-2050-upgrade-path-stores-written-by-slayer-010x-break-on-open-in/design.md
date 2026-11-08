## Context

See proposal.md for the motivation. The load pipeline is `StorageBackend._migrate_and_refine_on_load` (`slayer/storage/base.py`). It runs these steps on the raw dict:

1. stamp;
2. the dict migration chain;
3. the legacy `__` rewrite;
4. join-key canonicalisation;
5. exact-inverse dedup;
6. (pre-gate only) live refinement;
7. `model_validate`;
8. write-back at the current version, without re-validation.

Steps 2 to 8 run only when the stored version is below the current one. Memories are migrated in `Memory`'s before-validator and are never written back.

| Tag | SlayerModel | SlayerQuery | Memory |
|---|---|---|---|
| 0.10.2 | 10 | 4 | 2 |
| 1.0.0 | 11 | 4 | 2 |
| 1.0.1 / 1.0.2 | 12 | 4 | 2 |
| `main` | 13 | 5 | 3 |

The rank-direction steps (model 12→13, query 4→5, memory 2→3) are unreleased, but `main` builds have already written v13 docs with the `raw_formula` miss baked in.

Applicable arc42 principles:
- system §3.6 (AST), §3.11 (versioned persistence), §3.16 (logical names, physical once);
- core §3.4 (typed errors), §3.5 (structured warnings surfaced twice);
- sql §3.9 (fail closed);
- engine §3.1 (typed pipeline), §3.8 (models persist verbatim; migration write-back is the existing non-author exception);
- storage §3.1 (one sqlite door).

No arc42 or `.c4` edit is needed.

## Goals / Non-Goals

**Goals:**
- Make the whole bug class (a tightening that breaks released stores) fail CI, through a committed corpus written by the real release.
- Have one code path per concern: one load-error boundary, one enumeration helper, one placeholder engine.

**Non-Goals:**
- Reserved-name rejections (`time_spine` model, `window` param, scalar-function aggregation names) stay errors.
- Expression and filtered join keys stay errors.
- A sub-day bound against a DATE column stays a type error.
- #456's whole-day semantics for date-only bounds on TIMESTAMP columns stay.
- DEV-2000's time zones are out of scope. A repaired literal is naive wall-clock time, and DEV-2000 has a comment explaining the interaction.

## Decisions

### D1. Upgrade corpus over hand-written fixtures alone
`tests/fixtures/upgrade/v0_10_2/` holds:
- a YAML store and a SQLite store produced by `motley-slayer==0.10.2`;
- the DuckDB data file;
- `expected.json`, the 0.10.2 query results;
- `generate.py`.

`generate.py` runs in a throwaway `uv` venv and is not run in CI. It builds every shape the issue names:
- an ingested DuckDB model;
- `import-cube` of a project whose FK is not a dimension;
- a multi-stage query-backed model ordering by `rank(...)`;
- an aggregation with `'{n}'`;
- legacy `date_range` values and filters;
- memories with queries.

The per-item tests still use raw dicts in the exact 0.10.x shape, so every fix also has a focused regression test. Alternative considered: hand-written fixtures only. That is how the original misses happened, because a fixture is only as good as its author's model of what the release wrote.

### D2. Refinement gate = 8
- DOUBLE→INT narrowing at ingest arrived in v5 (8f9a0435, v0.5.0).
- The SQLite INT probe arrived in v0.7.2, while v7 was written from v0.6.10 to v0.9.12, so v7 is mixed.
- v8 (v0.9.13+) is the first version written only by fully refining ingest.
- The previous `11` was the then-current version when DEV-1934 exempted its own dict-only bump, and has no other rationale.
- Importers never refined at any version, so a v8+ import keeps its stored types exactly as the releases that wrote it did.

### D3. New stored-only steps rather than editing the unreleased ones
Steps model 13→14, query 5→6 and memory 3→4 are each stored-only:
- The model step stamps `source_queries` and re-runs the order rewrite.
- The query step re-runs the rank rewrite and repairs `date_range`.
- The memory step stamps `query`.

The rank rewrite is idempotent, because calls that already carry `direction=` are skipped. `_rewrite_order` additionally rewrites a dict item's string `raw_formula`; the 0.10.2 sweep found this to be the only persisted placeholder-plus-text shape.

The storage-side repairs (join keys, filter literals) are gated on "stored version < current" for models, and on the raw stored memory version < 4 for memories. Because memories are not written back, their check repeats on every load. It is cheap: filters with no quoted literal are skipped before any parse.

Alternative considered: folding the repairs into the unreleased steps. That leaves the v13 docs that `main` builds wrote permanently broken. Rejected.

### D4. Undeclared join keys: each document repairs only itself
Both repairs run after canonicalisation, in the load pipeline.

- **Source keys** are appended to the loading document.
- **Target keys** are appended when the target document itself migrates. It scans the raw sibling documents in its datasource (`_list_all_model_identities` + `_load_raw_model_dict`) for joins targeting it, after canonicalising their pairs against both column sets.
- **Hidden column type:** the opposite key's stored `type` when it is a valid current `DataType`, else TEXT. Probes showed that a TEXT-typed key emits a cast-free `ON`.
- **Siblings:** a sibling that can't be read or isn't a mapping is skipped by the scan; it is reported only by its own load.
- **Query-backed targets** are untouched.

Alternative considered: the source document writing the hidden column into its peer. That is a cross-document write during a read, and it depends on load order. Rejected.

### D5. Typed load boundary + one enumeration helper
- `StoredDocumentLoadError(SlayerError, ValueError)` wraps every failure inside the single-document load: decode, migration, refinement (driver errors included), validation, and a malformed version. Its message is `<ds>.<name>: <cause>`, with the original error as `__cause__`.
- One helper returns `(models, failures)`, plus a memory twin. They catch only this error, so programming errors outside the boundary escape.
- Each call site picks its policy explicitly:
  - **enumeration sites** skip and warn;
  - **`validate_models`** reports the failure;
  - **answer-picking sites** (population inference, bare-name scoping, `recommend_root_model`, detection scope, memory entity resolution, all via `_all_models_in_datasource`) re-raise.

Alternatives considered:
- Catching an enumerated exception list misses driver, `TypeError` and decode errors, and drifts over time.
- Catching `Exception` masks loader bugs as document failures.

### D6. Warning de-duplication by an explicit accumulator
A small failures accumulator is created per operation and passed by keyword through the internal calls. Entry points such as `save_model`, an ingest run and a listing create one when none is given. Ingest creates one for the whole run and passes it to every save it performs.

Each document is warned once, as the `UnloadableDocumentWarning` payload through its `UserWarning` carrier. The warning is copied onto the operation's response where it has one:
- `SlayerResponse.warnings`;
- the ingest report's `IngestionError` entries, which blame the bad model;
- `SearchResponse.warnings`, as its human message.

Never a context variable (engine §3.3). The save-time peer checks (`_find_edge_named`, `_load_join_peers`) share one datasource load per save.

### D7. Legacy time-literal repair
- **Shared normaliser** for both surfaces (`date_range`, filters):
  - strip `Z` / `±HH:MM` and keep the wall-clock time;
  - `YYYY/MM/DD[ HH:MM[:SS]]` becomes ISO;
  - anything else is left alone.
- **Offset handling.** Strip rather than convert to UTC: probes showed DuckDB 1.5.2 and Postgres 16 both read `'…02:00:00+05:00'` against a naive TIMESTAMP as `02:00:00`.
- **Filter repair** runs in the storage load path, because column types are needed.
  - Each stored filter is parsed with sqlglot.
  - The repair touches only string literals that are direct comparison operands: either operand order, or every element of an all-literal `IN`.
  - The other operand must be a column resolving to DATE or TIMESTAMP through raw dicts, including dotted join paths. Stage-local names are unresolvable.
  - Each repaired literal is emitted through sqlglot and spliced at its source position; the rest of the filter stays verbatim.
- **`date_range` repair** is a pure dict step in query 5→6.

### D8. Quoted placeholders in `SqlTemplate`
- **Collection.** Placeholders are collected inside ORDINARY string-literal tokens (`{name}` with `_NAME_RE`; `{{` / `}}` escape), so `placeholder_names`, `aggregation_reads` and the never-referenced check all count them.
- **Rendering.** A literal holding placeholders is rebuilt with `exp.Literal.string(<text with values spliced, braces collapsed>)`. Escaping happens in sqlglot's dialect generator.
- **Binding.** One literal classifier, shared by render and `check_aggregation_definition`, accepts a number, a string literal, or a negated number. It yields the value text: `2` gives `2`, `'x'` gives `x`.
- **Errors.** Any other binding raises `SqlTemplateError`: at save time for defaults, at bind or render time for kwargs. So does a placeholder inside a non-ordinary literal kind (national, escape, raw, byte, dollar/heredoc), so the dialect literal kind is never silently changed.
- **Unchanged.** Unquoted placeholders and `_check_positions` stay as they are.
- **Compatibility.** 0.10.2 made only numeric values meaningful inside quotes: bare-word defaults rendered as qualified column text, and string kwargs were limited to column names and numerals. This rule reproduces every case that worked there.

## Risks / Trade-offs

- [The corpus generator needs PyPI or the uv cache for 0.10.2] → It is committed but not run in CI. The generated artefacts are committed, so CI only reads them.
- [The target-side scan reads every sibling raw doc on the target's one-time migration] → This happens once per document (write-back at the current version). Raw reads only, with no validation.
- [A skipped peer's edge names aren't checked at save] → Accepted, as the issue decided. There is a terse comment at the site, and the warning makes the skip visible.
- [Answer-picking sites stay blocked by a residual bad document] → This is intended: fail closed. The error names the document and its cause.
- [`get_model`'s raised class changes to `StoredDocumentLoadError`] → It subclasses `ValueError`, and tests asserting an inner class assert it on `__cause__`.
- [DEV-2000 version collision] → A comment on DEV-2000 records the new `CURRENT_VERSIONS` and the stripped-literal semantics.
