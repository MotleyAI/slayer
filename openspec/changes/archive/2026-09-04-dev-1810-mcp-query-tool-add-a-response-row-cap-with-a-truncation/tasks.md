# Tasks

## 1. Failing test suite (spec-tests stage)

- [x] 1.1 Write tests for the default cap on `query` (no limit → 20 rows + notice; exactly 20 → no notice; 21 → 20 + notice) and verify they fail against current code
- [x] 1.2 Write tests that an explicit `limit` is trusted verbatim: limit=5 and limit=25/30 cases; mocked engine returning more rows than the limit → no slice, no notice; `explain=True` with explicit limit → plan untouched
- [x] 1.3 Write push-down tests: no-limit structured path emits `LIMIT 21` (show_sql and dry_run); run-by-name leaves stored SQL untouched
- [x] 1.4 Write run-by-name capping tests: stored query >20 rows → 20 + notice; exactly 20 → no notice
- [x] 1.5 Write `query_nested` tests: root without limit → 20 + notice with root-query hint; non-root limit ignored; root limit trusted; caller `queries` dicts not mutated
- [x] 1.6 Write notice-rendering tests: markdown `Warnings:` block, csv leading `#` line with uniform column count, json `{"data","warnings"}` with kind `"truncated"`; coexistence with an existing warning (truncation last); union round-trip of `ResponseTruncationWarning`
- [x] 1.7 Write explain/dry_run tests: explain without limit and >20 plan rows → capped + notice; dry_run never carries a notice

## 2. Implementation (spec-implement stage)

- [x] 2.1 Add `ResponseTruncationWarning` (kind `"truncated"`, `returned_rows`, `hint`) to slayer/core/warnings.py and the `AnySlayerWarning` union; verify union round-trip test passes
- [x] 2.2 Add default-cap constant and cap helpers to slayer/mcp/server.py (compute cap/pushed-down limit from caller args; slice + append warning); verify helper-level tests pass
- [x] 2.3 Wire the cap into `query`: push-down on the structured no-limit path, response-side slice for run-by-name and explain; verify tasks 1.1–1.4 and 1.7 tests pass
- [x] 2.4 Wire the cap into `query_nested` keyed on the root stage, copying the root dict; verify 1.5 tests pass
- [x] 2.5 Update `limit` docstrings on both tools (concise, one line each); verify rendered tool schema mentions the default cap
- [x] 2.6 Run the full non-integration suite (`poetry run pytest -m "not integration"`) and ruff; fix all failures

## 3. Documentation

- [x] 3.1 Update docs/reference/mcp.md: default cap, notice, root-stage rule for query_nested, json shape change on truncation; verify by proofread
- [x] 3.2 Update .claude/skills/slayer-query.md with a one-line mention of the cap; verify by proofread
