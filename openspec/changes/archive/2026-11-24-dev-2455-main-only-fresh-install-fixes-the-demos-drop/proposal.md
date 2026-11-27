## Why

A fresh install of main resolves sqlglot ≥ 30.18, under which the demo build fails (`Parser Error: syntax error at or near "VIEW"`), breaking `slayer datasources create demo`, `slayer mcp --demo` and `slayer serve --demo`. Separately, the CLI prints every deprecation twice — once as the `Deprecated: …` line of its `FutureWarning` and again from the command's response warnings — and a Mode-B spelling deprecation in two different wordings. The 1.x → main forward merge (DEV-2454) is blocked on the first fix.

## What Changes

- The demo loader reads each generated CSV directly instead of staging it in a temporary view, so it issues no `DROP VIEW`, whose sqlglot AST shape changed in 30.18. No dependency floor or lock change.
- A Mode-B spelling deprecation's `FutureWarning` carries its `deprecated_spelling` payload and shares its wording; that wording becomes today's per-kind `FutureWarning` text ("The comparison `==` …", "A bare None …", "The method-call form `…` …"). The payload's human-readable text changes on every surface; its structured fields do not.
- The CLI shows each deprecation once per command: a response warning whose text the command already showed as a `Deprecated: …` line, or already printed, is not printed again. Applies to `slayer query`, `slayer memory save` and `slayer search` text output. Structured JSON output and `slayer inspect`'s rendered document are unchanged.

## Capabilities

### New Capabilities
<!-- none -->

### Modified Capabilities
- `cli/commands`: adds the requirement that the CLI prints each deprecation once.
- `demo/jaffle-shop`: adds the requirement that the demo builds on every sqlglot version the package allows.

## Impact

- `slayer/demo/jaffle_shop.py` (`load_data`, `_require_store_regions`).
- `slayer/core/deprecation.py` (carrier base class, shown-deprecation record), `slayer/core/legacy_spellings.py`, `slayer/core/warnings.py` (`DeprecatedSpellingWarning.human_message`).
- `slayer/cli.py` (`main`, `_print_query_warnings`, memory save and search text printers).
- Human-readable text of `deprecated_spelling` warnings on REST, MCP and Python-client responses.
