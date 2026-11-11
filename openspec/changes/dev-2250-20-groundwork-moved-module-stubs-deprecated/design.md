## Context

See proposal.md › Why. The branch is stacked on DEV-2231 (PR #569) and merges it forward until it lands; the `compatibility/removal-tripwire` delta modifies DEV-2231's ADDED "One marker for code scheduled for removal".

Current state:
- `slayer/core/deprecation.py` holds `warn_deprecated` and `raise_moved`, the only tagged emitters; `tests/test_law_removal_tripwire.py` scans their calls (`HELPERS`, `PINNED`) and bans `FutureWarning` outside that module.
- `SlayerQuery._migrate_and_rewrite` refuses fresh `strict`; `storage/migrations.py::_query_v3_to_v4` migrates stored `strict` with `_STRICT_TRUE_TOKENS` / `_STRICT_FALSE_TOKENS`.
- `QueryRefinement` is `extra="forbid"`, so `refine={"strict": …}` fails today.
- Save warning channels: MCP model saves pass `warnings_out` to `engine.save_model`; REST `POST/PUT /models` pass none and return `{"status", "name"}`; memory saves return `SaveMemoryResponse.warnings: list[str]`.
- `SlayerClient` validates mappings into `SlayerQuery` locally and sends their dump.

Principles that apply: `python.core` P5 (typed errors), P6 (structured warnings surfaced twice); `system` P1/P2 (stubs sit in claimed packages; `engine→core`, `importers→core` are licensed), P11 (a bad `strict` token is refused at save). No arc42 or `.c4` edit. Spec attachment: the existing cross-cutting `compatibility`, `queries`, `mcp` and `models` groups.

## Goals / Non-Goals

**Goals:**
- One mechanism every later 2.0 slice (DEV-1607, DEV-2215) uses for its deprecated input forms.
- `strict` converts on every surface, reported on both channels, persisted only as `to_many_handling`.
- The migration guide covers every break already on the 2.0 line.

**Non-Goals:**
- A stub for `slayer.storage.legacy_backends` (removed, not moved; the guide covers it).
- Structured memory-save warnings (memory saves keep `warnings: list[str]`).
- DEV-2249's two list-column items in the guide, until that issue is decided.
- Rejecting non-string, non-bool `strict` values beyond today's truthiness.

## Decisions

### Moved-module stubs
Each stub is `raise_moved(old, new, removal="3.0")`, shaped like `slayer/engine/schema_drift.py`:
- `slayer/engine/syntax.py` → `slayer.core.syntax` (`parse_filter_expr` is gone; `parse_expr` reads SQL forms everywhere);
- `slayer/core/formula.py` → `slayer.core.syntax` (not drop-in: `parse_formula(named_measures=…)` expanded sibling measures, `parse_expr` returns plain `Ref`s; `ALL_TRANSFORMS` moved to `slayer.core.enums`);
- `slayer/osi/expression.py` → `slayer.importers.aggregates` (`convert_expression` lost `percentile_unsupported`).
The guide explains the non-drop-in cases. `PINNED` and `MOVED_MODULES` gain all three.

### The deprecated-input mechanism
- `slayer.core.deprecation.deprecated_input(old, new, *, removal) -> DeprecatedInputWarning` emits one `DeprecatedInputFutureWarning` (a `FutureWarning` subclass defined in `core/deprecation.py`, carrying the payload; `str()` is the payload's `human_message()`) and returns the payload. The caller performs the conversion.
- Attribution: the warning points at the first stack frame outside the `slayer` and `pydantic` packages (frame walk; the 3.11 floor has no `skip_file_prefixes`).
- `DeprecatedInputWarning(SlayerWarning)` in `core/warnings.py`: `kind="deprecated_input"`, `old`, `new`, `removal`; member of `AnySlayerWarning`.
- Carrying: a `wrap` model validator on `SlayerQuery` and on `QueryRefinement` converts and stores payloads in a private attribute of the instance.
- Harvest, queries: `_normalize_input()` exposes every query object it consumed (root, list stages) plus `SavedQueryRun.refinement`; `execute` appends their payloads, each once by identity, to `SlayerResponse.warnings` after normalization and before pipeline copies.
- Harvest, saves: the public `save_model()` entry collects payloads from the model's `source_queries` before pinning, `model_copy` or storage transforms, and appends them to `warnings_out`.
- Harvest, client: `SlayerClient` (sync and async) keeps payloads from the objects it validated locally and appends them to each returned `SlayerResponse`.
- `_emit_python_warnings` has no branch for the kind, so the payload is never re-emitted.
- Tripwire: `HELPERS` gains `deprecated_input`; `PINNED` gains `("slayer/core/query.py", "strict")` → 3.0 (the message is read from the `old` argument).

### One `strict` converter
- The token sets and conversion logic move into `core` (next to `SlayerQuery`); `_query_v3_to_v4` imports it (`storage → core` is licensed), and `core.query` stops using the `#legacy` door into `storage.migrations` for it.
- Only the fresh path calls `deprecated_input`. Fresh = no `version`, an integer version ≥ 4, or a version that is neither an int nor an integer string.
- An unrecognized token raises a dedicated `SlayerError` subclass naming `to_many_handling`, on both paths.

### Save responses
- REST `POST /models` and `PUT /models/{name}` pass `warnings_out` and return `{"status", "name", "warnings": [<payload dicts>]}` (deprecations and narrowed-access warnings).
- MCP model saves keep `warnings_out` → `_warning_lines`.
- Memory saves (MCP and REST share `SaveMemoryResponse`) append each payload's `human_message()` to `warnings`.

### Migration guide
`docs/upgrading/2.0.md`, `nav` entry "Upgrading to 2.0" after Getting Started in `zensical.toml`. Part 1: every break on the 2.0 line by surface, each with before/after, before fences marked `<!-- docs-guard: skip -->`:
- Mode-B text (DEV-2101, 2120, 2169, 2177, 2185, 2187, 2119): `"…"` is an identifier (silently compares against a same-named column); no backslash escapes (only `''`); `--` comments; `|` concatenation rejected; variables supply only values; bare `current_date` / `current_timestamp` are the clock; one LIKE pattern language (SQLite case-sensitive, `\` escapes everywhere, SQL Server `[` literal); `greatest`/`least` ignore NULLs on MySQL, BigQuery and ClickHouse; text vs number compares numerically via `TRY_CAST` (refused on Tier-2); out-of-domain math returns NULL; every branch of `CASE` / `iif` / `coalesce` / `ifnull` has one type on every dialect; `first`/`last` without a ranking key, grouped by a computed date, rank by that expression; a root qualifier is resolved at bind time, not stripped from the text.
- Lists (DEV-2209, 2172): `length` counts distinct non-NULL elements; opaque lists lose `length`.
- Models (DEV-2170, 2230): granularities with newly reserved names renamed on load to `<name>_granularity`; custom aggregations shadowing a library name warn; Postgres behind a transaction-mode pooler needs `transaction_pooler: true`.
- Query JSON (DEV-2119): `TimeDimension.dimension` serializes as text.
- Result values (DEV-2172): REST, MCP, CLI and facade encodings changed (Decimal, NaN, timestamps, bytes, intervals, NULL, booleans); a plain facade `SELECT` returns one row per record.
- Python API: moved modules (DEV-2182 plus the three stubs) and logger names; deleted modules; the DEV-2231 removals (EXPLAIN goes through the client's `explain`; a statements-only delegating client's EXPLAIN is served by the wrapped client); `execute` refuses session/transaction statements (DEV-2230); `TimeDimension.dimension` is `str`; smaller removals and signature changes (DEV-2187, 2173, 2101, 2172, 2189, 2188, 2198, 2230).
- MCP, CLI, env: `inspect_model` gone; `--models-dir` gone; `SLAYER_MODELS_DIR` silently ignored; Cube and dbt imports read types from a reachable `--datasource` (OSI already did), store unquoted mixed-case names in lowercase on Postgres / Redshift / Snowflake / Oracle, refuse such names when the datasource is not configured, and drop more constructs with a report. Ossie export writes `0.3.0.dev0` (1.x: `0.2.0.dev0`); Ossie import drops, with a report, a field reading a label its query source outputs more than once, a field transforming the source column of its own name, or a field reading a source column whose name another field holds.
- Install: slimmed `[postgres]` / `[mysql]` / `[all]` extras; `sqlalchemy>=2.0.43`; `pyparsing>=3.1`.
Part 2 (deprecated in 2.0, removed in 3.0): `strict`.
Every entry is re-verified by reproduction (1.x vs the 2.0 line) before it is written; entries not reproducible locally (BigQuery `TIMESTAMP` → `DATETIME`; pg-wire / Flight value text forms) get credentials checked first and are omitted if still unverifiable. Outcome: BigQuery omitted (no credentials); pg-wire / Flight verified in process; the planned "facade catalog no longer advertises list × custom-aggregation metrics" entry omitted, since 1.8.2 already left those metrics out.

### Decisions log
1. DEV-2220 is split: this groundwork issue now, DEV-2220 as the release slice; each slice writes its own guide section and its 3.0 pins. (user)
2. The 1.x-vs-main list-column divergences go to DEV-2249 and stay out of the guide until decided. (user)
3. Guide Part 1 covers every break on the 2.0 line, not only DEV-2231's. Options: full set vs this slice + DEV-2231 only; full set taken so later slices append only their own sections. (user)
4. One shared `deprecated_input` mechanism, not ad-hoc warnings. (default, structural)
5. The stubs use plain `raise_moved`; the guide explains the non-drop-in cases. (default)
6. No stub for `legacy_backends`: removed, not moved. (default)
7. An explicit `to_many_handling` wins over `strict`, as in the stored migration. (default)
8. `refine` converts `strict`; run-by-name top-level `strict` stays rejected. (default, consistency)
9. A malformed version carrying `strict` is fresh input: converted, never silently migrated. (default)
10. Save surfaces without a channel get one: REST model saves return structured `warnings`, memory saves append strings. Options: A (this); B (structured memory warnings — breaks that response shape); C (existing channels only — REST clients never see it). A taken. (user)
11. Payloads ride a private attribute on the converted query/refinement and are harvested per run. (default, structural)
12. One `strict` converter in `core`, shared by the stored and fresh paths. (default, structural)
13. An unrecognized `strict` value raises a dedicated `SlayerError` subclass on both paths. (default; dedicated subclass per codex, see 19)
14. `strict: false`, and `strict` overridden by an explicit `to_many_handling`, still warn: the deprecated form was used. (default)
15. `deprecated_input` attributes the warning to the first frame outside `slayer` and `pydantic`. (default)
16. The new requirement lives in `compatibility/removal-tripwire`; its "One marker…" delta modifies DEV-2231's ADDED wording. `attribution-modes` replaces "The strict flag is retired" by REMOVED + ADDED (a MODIFIED delta cannot retitle its scenarios). (default)
17. `SlayerClient` keeps locally converted payloads and appends them to each returned response, sync and async. (codex)
18. Refinement payloads are harvested from `SavedQueryRun.refinement` explicitly, since `refine_query` builds a fresh query. (codex)
19. A dedicated typed error for an unrecognized `strict` value. (codex) Rejecting ints, floats and containers too: rejected (codex) — it would split the shared converter's semantics and could refuse stored v3 data that loads today.
20. The `FutureWarning` is a payload-carrying carrier class defined in `core/deprecation.py`. (codex)
21. Query harvest happens in `_normalize_input()`, after dicts become query objects and before pipeline copies. (codex)
22. Save harvest happens at the public `save_model()` entry, before pinning, `model_copy` and access-filtered storage transforms. (codex)
23. Codex's claim that `openspec validate --strict` fails on a MODIFIED delta against DEV-2231's unarchived requirement: rejected (codex) — reproduced in a scratch copy, validation passes; only archive resolves against the base. Its concern is kept as a tasks.md gate (archive only after DEV-2231's archive is merged forward, then re-validate).

## Risks / Trade-offs

- [Private attributes are dropped by `model_dump` → `model_validate` round trips] → harvest at the boundaries named above, never later; tests cover staged models, access-filtered storage and the HTTP client.
- [`FutureWarning` shows once per location under Python's default filter in a long-running server] → acceptable: the response channel carries every occurrence.
- [Archive ordering with DEV-2231] → the tasks.md gate.
- [Guide entries going stale as later slices land] → each slice owns its own section; Part 1 is verified by reproduction at write time.
