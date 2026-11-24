## Context

See proposal.md — Why. Verified on main `69a6e2048`.

- **Demo.** `load_data` (`slayer/demo/jaffle_shop.py`) stages each CSV as `CREATE TEMPORARY VIEW _<table>_raw AS SELECT * FROM read_csv_auto('<path>')`, reads it in `DESCRIBE`, `_require_store_regions` (stores only) and `INSERT … SELECT`, then drops it with `exp.Drop(this=…)`. From sqlglot 30.18 `Drop` keeps its target in `tables` and ignores `this`, so the drop renders bare `DROP VIEW`. `pyproject.toml` allows `sqlglot>=30.17`; `poetry.lock` pins 30.17.0, so CI never saw it. Probed: `DESCRIBE SELECT * FROM READ_CSV_AUTO(...)` renders identically on 30.17.0, 30.18.0 and 30.34.0.
- **CLI.** `main()` wraps argv respelling and command execution in two separate `deprecations_on_one_line()` blocks (`slayer/core/deprecation.py`), which print each `FutureWarning` as `Deprecated: …` on stderr. Independently, `_print_query_warnings` prints `result.warnings` as `warning: …`, `memory save` prints `response.warnings` (strings) as `Warning: …`, and search's text renderer prints them as `[warning] …`. Nothing correlates the two channels, so each deprecation shows twice (reproduced for query keys, `"broadcast"`, `==`, `memory save --query`, `search --cypher-filter (:Model)`).
- Input deprecations already warn with `DeprecatedInputFutureWarning`, whose `str` is its payload's `human_message()`. Mode-B spelling deprecations (`legacy_spellings._emit`) warn with free text via `warn_deprecated`, worded differently from `DeprecatedSpellingWarning.human_message()`.
- `"broadcast"` in a stored document deliberately emits no `FutureWarning` (`broadcast_deprecations` uses `emit=False`) but is reported on the response.

**Structural reading.** Fix 1's cause is a dependency on an AST node whose shape changed; removing the staging view removes the dependency rather than working around it. Fix 2's symptom (three keys printed twice) is one instance of a class: every CLI command that both shows the warning channel and prints the payload channel doubles every deprecation. The fix sits at the CLI's one display path, so no command or deprecation kind can reintroduce it. `python.core` arc42 principle 6 ("warnings surfaced twice") governs the library and stays intact — the library still emits both channels; the CLI is one consumer showing each once.

**Principles that apply.** system §3 #2 model truth (no new cross-node imports: `slayer.cli` and `core.legacy_spellings` already import `core.deprecation`), #4 SQL built as sqlglot AST (the demo statements stay AST-built); `python.core` #6 (kept), #7 core imports no other node (the record lives in `core.deprecation`). No arc42 or `.c4` edit.

## Goals / Non-Goals

**Goals:**
- The demo loads on every allowed sqlglot, with a test that fails without the fix on every sqlglot version.
- One line per deprecation per CLI command, with nothing lost.

**Non-Goals:**
- Changing the `sqlglot` floor or the lock (DEV-2454 owns the dependency policy and latest-deps CI).
- REST, MCP and the Python client: their two channels reach different audiences (principle 6).
- `slayer inspect`: its warnings are part of the rendered response document, not CLI advisory lines.
- Structured JSON output (`search --format json`): never filtered.
- Collapsing duplicate warnings that are not deprecations.

## Decisions

1. **Fix 1 removes the staging view, so there is no `DROP`.** `DESCRIBE`, the region check and the `INSERT … SELECT` read the `read_csv_auto('<path>')` table source directly. The view was non-materialised, so CSV reads are unchanged at any `--years`. Alternatives: raise the floor to 30.18 and bump the lock (pulls every other 30.18+ change in here; dependency policy is DEV-2454's); `exp.Drop(this=t, tables=[t])` (relies on each version ignoring the other's argument). (user)
2. **No `sqlglot` floor or lock change.** Follows from 1. (default)
3. **Fix 2 covers every CLI command and deprecation kind**, not only the three query keys the issue names. (user)
4. **A spelling deprecation keeps today's per-kind `FutureWarning` wording**, which becomes `DeprecatedSpellingWarning.human_message()`, rendered from the payload's `written`, `canonical` and `removal`. The payload's text changes on every surface; its structured fields do not. Alternative: keep the generic payload template "the <spelling> spelling `<written>` …" (reads awkwardly). (user delegated → default)
5. **Identity for skipping is the line's text.** A carrier's `str` is its payload's `human_message()` by construction, and the memory and search responses carry plain strings. (default)
6. **The record of shown deprecation lines lives in `slayer.core.deprecation`**, as a set captured by the `deprecations_on_one_line()` block (so a warning shown from any thread inside the block counts); printers run in the invoking thread and read it through one helper. (default, codex)
7. **A payload-only deprecation prints with the printer's existing prefix.** No prefix changes. (default)
8. **REST, MCP and the Python client are out of scope** (principle 6). (default)
9. **The requirements attach as ADDED to `cli/commands` and `demo/jaffle-shop`**, rather than editing `compatibility/removal-tripwire`. Both groups are already mapped (`cli` cross-cutting; `demo` owned by `python.surfaces`). (default)
10. **Identical deprecation lines collapse; no occurrence counting.** A broadcast payload per stage carries no stage name, so N identical lines add nothing; the helper records the deprecation lines it prints itself as well. Codex proposed counting occurrences. rejected (codex)
11. **`slayer inspect` is out of fix 2's scope.** Its warnings are embedded in the rendered Markdown/JSON document (the same document MCP shows); filtering it would change the response. Narrows decision 3. (codex)
12. **The demo test is version-independent**: it records every statement `load_data` executes and asserts none creates or drops a view, plus the load succeeds — failing without the fix on 30.17 too, so default CI guards it. (codex)
13. **Structured JSON output is never filtered**; only lines the CLI prints as advisories are skipped. `query --format json` already sends warnings to stderr only; `search --format json` keeps them in its `warnings`. (codex)
14. **One outer `deprecations_on_one_line()` block in `main()`** covers argv respelling, parsing and execution. (codex)
15. **Text identity is kept despite collision risk**: a non-deprecation warning would need a deprecation's exact text, and two identically worded deprecations are the same message to the reader. rejected (codex)
16. **Tests cover Python warning filters and repeated `main()` calls in one process**: recording happens inside the display hook, so only displayed warnings suppress a response line. (codex)
17. **A generic `DeprecationFutureWarning` carrier base**; `DeprecatedInputFutureWarning` stays as its subclass for input payloads, and a `DeprecatedSpellingFutureWarning` subclass carries spelling payloads. (codex)
18. **The cross-surface spelling wording change is intended and not pinned by wording tests**; tests assert the two channels match. Codex proposed keeping payload wording and pinning every message. rejected (codex)

## Risks / Trade-offs

- [The new demo test pins the absence of a staging view rather than sqlglot ≥ 30.18 behaviour] → It fails without the fix on every version; the latest-deps job (DEV-2454) additionally exercises ≥ 30.18 end-to-end.
- [Existing tests pin the old `deprecated_spelling` payload text or the free-text `FutureWarning`] → Mechanical alignment, approved per file in `pr-tests`.
- [A long-running server command runs inside the block, so the record grows] → Bounded by the number of distinct deprecation texts; servers do not use the CLI printers.
