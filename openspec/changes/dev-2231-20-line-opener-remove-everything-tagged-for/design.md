## Context

18 `warn_deprecated(..., removal="2.0")` sites on `main` (pinned in `tests/test_law_removal_tripwire.py`'s
`PINNED`). The tripwire compares only major versions, so `2.0.0.dev0` already trips every one of them.
Architecture grounding:

- system §3.7 (async-first): `ingest_datasource_idempotent` / `validate_datasource` were plain functions
  returning a coroutine only so the deprecation could name the caller's line; they become `async def`.
- system §3.23 (one database door): every remaining form reads through a client owned by an engine; the
  removed no-client forms were the only callers of a per-call fallback engine.
- sql §3.14 / §3.15 (one engine owner / one driver per client): `SlayerSQLClient(sa_engine=)` is public,
  documented and untagged and stays; the principles' wording is unchanged. No arc42 or `.c4` edit.
- Spec attachment: existing capabilities only (cross-cutting `compatibility/*` plus the deltas listed in the
  proposal).

## Goals / Non-Goals

**Goals:**

- Remove every `removal="2.0"` site and all code that only they kept alive; `git grep 'removal="2.0"' slayer/`
  finds nothing.
- `slayer.__version__ == "2.0.0.dev0"`.
- The tripwire keeps its mechanism (helper scan, literal `removal`, major comparison, only the helper emits
  `FutureWarning`) with an empty `PINNED`, ready for `removal="3.0"` forms.
- Every removed public form fails loudly rather than silently changing meaning (except `SLAYER_MODELS_DIR`,
  decision 2).

**Non-Goals:**

- Stored-data upgrade paths: versioned migrations, the legacy `__` alias rewrite, layout upgrades, type
  refinement, the 1.6.0-typed stored-column drift handling (`models/schema-drift` "typed by 1.6.0" scenarios).
- `SlayerSQLClient(sa_engine=)` and the caller-owned-engine client path.
- The mcp 2.x `MCPServer` migration (DEV-1760).
- The rest of the 2.0 release work (DEV-2220: 3.0 tripwire migration guide, release).

## Decisions

1. **The bundled-driver removal honours its promise** (user). The `postgres` extra becomes `["psycopg"]`, `mysql`
   becomes `["pymysql"]`, `all` drops `psycopg2-binary` / `asyncpg` / `aiomysql`, and the three leave the optional
   dependencies; `asyncpg` and `aiomysql` join the dev group (`psycopg2-binary` already is). `extra_drivers`
   shrinks to the default driver (`{"psycopg": "psycopg"}`, `{"pymysql": "pymysql"}`), keeping its meaning
   "the drivers the extra installs" so the pyproject↔dialect consistency tests still guard it; a missing
   non-default driver gets the non-bundled hint. `_warn_bundled_driver` and `DriverFacts.bundled_non_default` go.
2. **`SLAYER_MODELS_DIR` is ignored after removal, not refused** (user). Options: (a) ignore — what removal
   means, no compatibility code survives; (b) refuse with a typed error naming `SLAYER_STORAGE` — fails closed
   but is compatibility code nothing would ever retire. Taken: (a); the DEV-2220 migration guide carries the
   rename.
3. **`SlayerSQLClient(sa_engine=)` stays** (default): public, documented (`docs/configuration/sql-clients.md`),
   untagged; sql §3.14/§3.15 keep their wording.
4. **Code dead after the removals is deleted** (default): `marked_coroutine_function`;
   `run_over_caller_engine`, `run_sync_on_caller_thread`, `on_caller_thread`, `_caller_calls` / `_CallerCall`
   (a caller-owned engine's blocking work runs inline — what `on_caller_thread` already did outside
   `run_sync_on_caller_thread`, whose only user was the removed converter path); `_ingest_idempotent_short_lived`,
   `_validate_short_lived`, `_UNSET`, `_run_through`'s no-client branch; `type_categories`,
   `_CategoryProbeClient`, `_CATEGORY_TYPES`, `adapt_client`, `LegacyTypeCategories`, `_SLAYER_TYPE_CATEGORIES`,
   the `legacy_type_categories` field and tables; `WarningLocation` and `warn_deprecated`'s `location=`.
   `warn_deprecated` (no callers left) and `deprecations_on_one_line` stay as the 3.0 mechanism.
5. **Converter parameters after `data_source` become keyword-only** (default). Without it, removing the third
   slot would silently bind a 1.5.0 positional `sa_engine` to dbt's `include_hidden_models`. Every positional
   call reaching a later parameter passes through the removed slot, so no valid untagged form is lost.
6. **`slayer/storage/legacy_backends.py` goes entirely** (default), including the `_list_memories_rows`
   wrong-shape `TypeError`; the abstract method's annotated type is the contract.
7. **The tripwire's 2.0.0 test is generalised to the next major** (default): it patches the version to
   `{major+1}.0.0` and asserts the named sites equal `PINNED`, instead of becoming vacuous. The
   `all(s.removal == "2.0")` assertion goes.
8. **New tests cover `warn_deprecated` / `deprecations_on_one_line`** (default): their only tests (the
   `--models-dir` CLI and per-surface warning tests) are deleted with the surfaces.
9. **Stored-data upgrade paths are untouched** (issue).
10. **Removed-form tests are deleted, incidental users ported** (issue): the full lists are tasks 4.x; each
    deletion or logic change needs the owner's per-test consent in pr-tests.
11. **The `run_over_caller_engine` test callers are enumerated and ported** (codex):
    `tests/test_cube_js_e2e_duckdb.py:71` `_import_and_engine` (file-backed DuckDB → `engine.client_for(ds)`),
    `tests/_dev2171_fixtures.py:103` `collect_over_engine` and `tests/test_importer_source_names.py:166` `_cube`
    (→ an engine whose `client_factory` returns `SlayerSQLClient(datasource=ds, sa_engine=caller_engine)`).
12. **The conformance suite keeps failing a 1.6.0-only client** (codex): the scenario moves, verbatim, from
    the removed 1.6.0 requirement into "The client contract is executable";
    `tests/test_sql_client_conformance.py::TestA160ClientIsCheckedAgainstTheCurrentContract` stays unchanged.
13. **Retained-API thread-affinity tests for caller-owned engines** — rejected (codex): deleting the
    caller-thread machinery changes no surviving path (`on_caller_thread` already ran inline whenever
    `run_sync_on_caller_thread` was not active, and only the removed converter path activated it); the helpers
    are not public.
14. **Keep dbt's positional order after removing `sa_engine`** — rejected (codex): see decision 5.
15. **Spec scenario titles naming `inspect_model`** (default): a MODIFIED requirement cannot rename a scenario,
    so `models/column-granularity` and `models/sample-profiling` restate the affected requirement under a new
    name (REMOVED + ADDED); `models/list-columns`, the other `models/sample-profiling` requirement and
    `queries/time-spine` MODIFY bodies only.
16. **Stale capability Purposes are rewritten in the main specs** (default): a delta's `## Purpose` is ignored
    for an existing capability, so task 7.3 edits `openspec/specs/{compatibility/legacy-call-forms,
    compatibility/removal-tripwire,storage/custom-backends}/spec.md` directly with the wording given there.

## Risks / Trade-offs

- [A user with `SLAYER_MODELS_DIR` set silently gets the default storage] → recorded in the DEV-2220
  migration notes (decision 2).
- [Hosts depending on `motley-slayer[postgres]` for asyncpg / psycopg2, or `[mysql]` for aiomysql, lose them]
  → announced by the 1.x warning and docs; the non-bundled hint names the driver to install.
- [~250 test edits risk weakening assertions] → ports keep their assertions (category strings become the
  equivalent `ResultColumnType` checks); every deletion or logic change is listed and consented in pr-tests.
- [`poetry.lock` churn from the extras change] → `poetry lock` without updating dependencies.
