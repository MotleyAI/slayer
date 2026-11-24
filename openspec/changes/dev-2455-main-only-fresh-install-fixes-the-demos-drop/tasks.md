## 1. Tests (pr-tests)

- [x] 1.1 Demo: a test wraps the DuckDB connection passed to `load_data` (jafgen fixture), records every executed statement, and asserts the load succeeds and no statement creates or drops a view; verify it FAILS on the current code (it executes `CREATE TEMPORARY VIEW` / `DROP VIEW`)
- [x] 1.2 CLI query: drive `slayer.cli.main` with a query using `source_model`, `measures`, `filters`, `to_many_handling: "broadcast"` and `==`; assert each deprecation appears exactly once on stderr (count by substring per deprecation, no wording pins); verify it FAILS today
- [x] 1.3 CLI query by name: a saved query stored with `"broadcast"` on two stages prints the broadcast deprecation exactly once
- [x] 1.4 CLI `memory save --query` with `source_model` + `measures`: each deprecation exactly once in the combined output; verify it FAILS today
- [x] 1.5 CLI `search --cypher-filter` with `:Model` (text): the deprecation exactly once; `--format json`: the JSON `warnings` still carries it
- [x] 1.6 Python warnings filtered (`ignore`, `once`) and a second `main()` call in the same process: each deprecation still prints exactly once
- [x] 1.7 A query response warning that is not a deprecation still prints
- [x] 1.8 Carrier: for every Mode-B spelling kind (`==`, `is None`, `is not None`, bare `None`, list after `IN`, method-call form) the `FutureWarning` is a `DeprecationFutureWarning` carrying a `DeprecatedSpellingWarning`, and `str(warning) == payload.human_message()`; `DeprecatedInputFutureWarning` is a `DeprecationFutureWarning`
- [x] 1.9 Existing tests pinning the old `deprecated_spelling` payload text or `FutureWarning` class/text: list each file and the exact change, get user approval, then align

## 2. Demo loader (pr-implement)

- [x] 2.1 `load_data` reads the `read_csv_auto('<path>')` table source directly in `DESCRIBE`, `_require_store_regions` and the `INSERT … SELECT`; remove the staging view and its `exp.Drop`; verify 1.1 and the existing demo tests pass

## 3. One wording per deprecation (pr-implement)

- [x] 3.1 `slayer/core/deprecation.py`: add a `DeprecationFutureWarning` carrier base (payload + `str` = `human_message()`); make `DeprecatedInputFutureWarning` its subclass; add `DeprecatedSpellingFutureWarning`; verify 1.8
- [x] 3.2 `DeprecatedSpellingWarning.human_message()` renders today's per-kind wording from `spelling`/`written`/`canonical`/`removal`; `legacy_spellings._emit` warns with the carrier through the one deprecation door (keep the removal tripwire's static read working); verify 1.8 and `tests/test_law_removal_tripwire.py`

## 4. CLI shows each deprecation once (pr-implement)

- [x] 4.1 `deprecations_on_one_line()` records the text of each `FutureWarning` it shows in a set captured by the block; expose one helper that, given candidate lines, yields only those not yet shown and records the deprecation lines it lets through
- [x] 4.2 `main()` uses one outer block covering argv respelling, parsing and execution
- [x] 4.3 Route `_print_query_warnings`, the `memory save` warnings printer and `_print_search_response_text`'s warnings through the helper; leave JSON output and `inspect` untouched; verify 1.2–1.7

## 5. Gates

- [x] 5.1 Docs: check `docs/` for text quoting the old spelling-deprecation wording or the doubled CLI output, and update
- [x] 5.2 Run the local test scope (`la-affected-tests --base main` + demo/CLI/deprecation tests), `poetry run ruff check slayer/ tests/`, `la-typecheck`, `poetry run la-arch-check`; all green
