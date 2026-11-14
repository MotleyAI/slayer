## 1. Tests (pr-tests stage; new file `tests/test_dev2286_deprecated_input_surfaces.py`, helpers from `tests/_deprecated_input_helpers.py`; all must fail before implementation except the pinned-boundary regressions)

- [ ] 1.1 REST `POST /query` with `{**CROSS, "strict": null}` returns 200 with rows equal to the same body without `strict`, and exactly one `deprecated_input` warning (`old == "strict"`, `removal == "3.0"`); verify by running the test.
- [ ] 1.2 REST run-by-name `{"name": SAVED, "strict": null}` (and `"strict": false`) returns 400; verify by running the test (pins decision 3).
- [ ] 1.3 A query carrying `strict: true` and an extension object in `source_model` emits exactly two `DeprecatedInputFutureWarning`s (one per form) and its response carries exactly two distinct `deprecated_input` warnings, in process (`engine.execute`) and over REST; verify by running the test.
- [ ] 1.4 Fresh-versus-stored boundary regressions (`SlayerQuery.model_validate` and the REST request): `strict: true` with no version, the current version and a malformed version each convert to `to_many_handling: "error"` with exactly one payload; a v3 stored query with `strict: true` upgrades to `"error"` with no payload and no `FutureWarning`; a legacy root extension validated inside `migrating_document(..., stored=True)` converts with no payload; verify by running the tests.
- [ ] 1.5 MCP `query` dry run of `strict(CLEAN)`: `format="json"` parses as an object with `sql` and exactly one `deprecated_input` in `warnings`; `format="csv"` has a leading `# warning: <human_message>` line before `SQL:`; `format="markdown"` starts with `SQL:` and carries the `human_message` in its `Warnings:` block; verify by running the test.
- [ ] 1.6 MCP `query` dry run of a query calling a shadowed library aggregation (the `tests/test_library_name_shadow_warnings.py` fixture) carries the `library_name_shadow` warning under `format="json"`; verify by running the test.
- [ ] 1.7 A clean MCP json dry run is `{"sql": …}` with no `warnings` key; existing `tests/test_dev1858_mcp_query_tidy.py::test_dry_run_returns_sql_only` and `tests/test_mcp_row_cap.py::test_dry_run_never_truncates` stay unchanged and green.
- [ ] 1.8 MCP model saves: `create_model(query=strict(...))` reply and `edit_model(source_queries=[strict(...)])` reply each contain the payload's `human_message()` exactly once; verify by running the test.

## 2. One fresh-input converter (`slayer/core/query.py`, `slayer/api/server.py`)

- [ ] 2.1 `prepare_query_input` converts a fresh `strict` (`_has_fresh_strict` → `_convert_strict`), then a legacy root extension, then migrates the schema, returning `(data, list[DeprecatedInputWarning])`; `SlayerQuery._migrate_and_rewrite` calls it in place of its own sequence; verify 1.3 and 1.4 pass in process.
- [ ] 2.2 `QueryRequest._prepare_query_input` keeps every payload from `prepare_query_input`; drop the "deprecated `strict` rides extra=allow" comment; verify 1.1 and 1.3 pass over REST.
- [ ] 2.3 The REST run-by-name gate also refuses a request with `deprecated_inputs`; verify 1.2 passes.

## 3. MCP dry run (`slayer/mcp/server.py`)

- [ ] 3.1 The dry-run branch renders per `format` via the existing warning helpers: json → `encode_json({"sql", "warnings"?})`, csv → `_csv_warning_comments(result) + "SQL:\n…"`, markdown → `"SQL:\n…" + _format_warnings(result)`; verify 1.5–1.7 pass.

## 4. Docs

- [ ] 4.1 `docs/reference/mcp.md` `dry_run` row: SQL plus the run's warnings, `{"sql", "warnings"}` under `format="json"`; verify the page renders the row.
- [ ] 4.2 Re-read `docs/upgrading/2.0.md` Part 2 intro against the amended spec (MCP model saves report the message; dry runs report too) and adjust only if it falls short.

## 5. Gates

- [ ] 5.1 Local test scope (per `/la:pr`), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors), `poetry run la-arch-check` all pass.
