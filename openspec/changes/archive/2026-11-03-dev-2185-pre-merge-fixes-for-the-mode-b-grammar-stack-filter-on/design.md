## Context

See proposal.md (Why). Current state:

- **Type grammar.** `slayer/core/syntax.py` `_TYPE_SPEC` (~:631-657) is one type word, then extra words, then an optional `(digits,…)` at the end only; no `[]`. `::` shares it (~:870). The parsed type is an uppercase string `Cast.type_text`. `_bind_cast` (`slayer/engine/binding.py` ~:555) checks it through `build_cast_type` / `cast_target` (`slayer/sql/dialects/base.py` ~:302): `exp.DataType.build(type_text, dialect=<target>)` with a Postgres fallback, rejecting `USERDEFINED` and vendor-only types. `cast_data_type` (`slayer/core/keys.py` ~:733) reads only the first word. `CastKey` carries `data_type` but no element type, and `list_element_type` (`keys.py` ~:1746) has no `CastKey` branch.
- **sqlglot loses parts of types silently**: `TIMESTAMP WITH TIME ZONE(6)` → `TIMESTAMPTZ` (precision gone), DuckDB `TIMESTAMPTZ(6)` → `TIMESTAMPTZ`, `INT[]` on SQLite → `INTEGER`, `DECIMAL(10,2) UNSIGNED` on Postgres → `UDECIMAL(10, 2)` (not a type).
- **Comments.** None handled. The only pre-pass is `_regions` / `_mask_placeholders` (~:205-274), which keeps offsets length-preserving so `Placeholder.start/end` and error columns index the original text.
- **TRY_CAST.** Rendered text-first everywhere (`base.py` ~:167 `text_try_cast`; SQLite UDF `slayer_try_cast` with `_try_number` ~:353-366 that truncates; Postgres 16 `pg_input_is_valid`; MySQL regex guard on the shared `NUMBER_TEXT_PATTERN` ~:197-212; Snowflake ~:257, BigQuery ~:154 native). The shared number predicate accepts fractions and exponents for every numeric target.
- **Canonical text.** `canonical_measure_text` (~:1918) emits no grouping parentheses (`(a + b) * c` → `a + b * c`, `-(-b)` → `--b`) and is used for messages and for time-dimension identity (`slayer/core/query.py` ~:652).
- **Stored-text upgrade.** `slayer/storage/sql_string_migration.py` re-emits Python-tokenised legacy text token-exactly (offset-based edits, ~:59-91). DEV-2101 is unreleased, so its version step is extended rather than a new one added.

Architecture constraints: system §3.10.1 / 10.2 (one meaning per dialect; a dialect that cannot express a construct refuses it with a typed error), §3.11.2 (collisions ship with a migration), §3.18 (one reader of Mode B — comment handling lives only in `core.syntax`); engine P2 (parsing is pure syntax); python.sql §3.2 (dialect quirks only in `dialects/`), §3.9 (fail closed).

## Goals / Non-Goals

**Goals:**
- Parse every SQL type spelling the specs list, reject lossy renderings per dialect, type array casts as lists.
- SQL comments with exact source offsets.
- One integer-target `TRY_CAST` validity rule on all nine Tier-1 dialects.
- A canonical printer that round-trips.

**Non-Goals:**
- Multi-dimensional arrays (no list-of-lists type).
- `TRY_CAST` to an array type (refused).
- Making `CAST` portable: its value semantics remain the database's own.
- The other findings of the same review: FILTER on custom aggregations (DEV-2186), the source-model prefix strip and other Mode-B regex scans (DEV-2187), f-string SQL (DEV-2188), re-parse of emitted SQL (DEV-2189).

## Decisions

Decisions log. Each entry: decision — options weighed — taken, why — who.

1. **Five PRs, each in its own worktree, all stacked on the DEV-2101 branch**; this change keeps the grammar & cast items (user).
2. **Array casts** — accept `T[]` everywhere and refuse where unsupported (A) vs refuse arrays everywhere (B). Taken A: the casts spec already says "render the author's type unless the dialect cannot express it", and SLayer has a list type (user).
3. **TRY_CAST to an array type** — refuse everywhere (A) vs each database's own array validity (B). Taken A: arrays have no portable text form, and B breaks system §3.10.1. Plain `CAST` to arrays is allowed wherever the database allows it (user).
4. **`--`** — support `--` and `/* */` SQL comments (A) vs a hinted parse error (B). Taken A: SQL-honest; B refuses valid SQL (user).
5. **TRY_CAST to an integer type** — integer text only, else NULL (A, Postgres reference) vs round half away from zero (B). Taken A: B invents a conversion no database has; authors round explicitly with `TRY_CAST(round(x) AS INT)` (user).
6. **Canonical printer** — make it precedence-aware (A) vs special-case `-(-b)` only (B). Taken A: one structural change fixes both the comment collision and the identity conflation of differently grouped expressions (user, on a Codex finding).
7. **Type grammar shape**: words, an optional integer parameter list between words, more words, an optional `[]` (default). It implements the existing spec rule and is the SQL-standard / Postgres / MySQL type-name shape.
8. **Lossless-render check by normalized cast signature** (codex). Build the author's type in the source reading (target dialect, Postgres fallback as today), render it for the target, re-read the rendered type in the target dialect, and compare a normalized signature: base family, precision/scale, time-zone modifier, signedness, array rank, element signature. A raw sqlglot structural compare was rejected because spelling aliases (`INT4` vs `INTEGER`) legitimately differ.
9. **Expressibility is a per-dialect hook** (codex, python.sql §3.2). `base.py` holds the generic signature comparison; each `dialects/*.py` declares what it can express beyond what the comparison already catches: unsigned families (MySQL, ClickHouse only), array support, vendor types. There is no central table.
10. **`CastKey` carries an element type** (codex). It is populated in `_bind_cast` from the parsed element type, read by `list_element_type` and by slot metadata (`slayer/engine/key_metadata.py` ~:150), so an array cast types like a list column. `cast_data_type` takes the parsed type, not its first word.
11. **Comments are masked, not deleted** (codex). A region-aware pass in `core.syntax` replaces each comment character with a space (newlines kept), before placeholder validation, so offsets stay exact and braces in comments are ignored. Nesting and the unterminated-`/*` error follow Postgres, the reference dialect (default).
12. **Integer-text predicate** (codex). A distinct predicate, selected from the cast's target type, is applied before every dialect's integer conversion. The existing number predicate stays for DOUBLE / DECIMAL. Numeric (non-text) input to an integer target is valid exactly when it is integral and in range. This is a default: it keeps the rule dialect-independent where float-to-text forms differ (`2.0` vs `2`) and matches the approved `TRY_CAST(1.5 AS INT)` → NULL.
13. **Migration** (codex). A space is inserted only when one `-` token's `end` equals the next one's `start`. Text already separated is untouched.
14. **TRY_CAST execution coverage** (codex). SQLite, DuckDB and Postgres (pytest-postgresql) run in the unit suite. MySQL, ClickHouse, SQL Server and Trino scenarios go in their path-gated integration suites. BigQuery and Snowflake scenarios go in their live suites, which skip without credentials. MySQL's is part of acceptance, not follow-up.
15. **Audit for Mode-B text concatenation** that a trailing `--` comment could swallow (default). Anything found outside DEV-2187's scope is raised to the user, not fixed silently.

## Risks / Trade-offs

- [The lossless check rejects a type some user relies on today, because sqlglot quietly changed it] → that SQL was already wrong (silently a different type). The typed error names the type and dialect; tests pin the accepted aliases.
- [An agent writes `a --b` on one line meaning double negation and silently gets `a`] → it is SQL's own meaning; the docs sentence states it.
- [Snowflake / BigQuery behaviour verified only in live CI] → emission tests run locally; their live scenarios gate CI when credentials are present.
- [The precedence-aware printer changes canonical text, and therefore any identity or message derived from it] → result-key aliases are unaffected (the alias slug drops operators). The time-dimension identity becomes strictly more precise. Goldens are re-blessed only where message text changes.
- [Masking comments in stored text whose old meaning was double negation] → the stored-upgrade spaces adjacent minus signs first.
