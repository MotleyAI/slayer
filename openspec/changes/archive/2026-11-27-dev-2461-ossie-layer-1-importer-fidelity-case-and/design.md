## Context

See proposal.md for the motivation. Probes ran on `origin/main` at `470a8d3f8`:

- **Scalars and `CASE` already evaluate.** `slayer import-osi` stores documents as written and reports `not_evaluable` from each entity's load warning. Fifteen `CASE` / scalar-around-aggregate forms already load without warnings and match raw SQL on DuckDB and SQLite once fields are typed. The corpus import has no warning for these constructs; the corpus also contains no scalar-around-aggregate metric.
- **Quoted names.** `slayer/core/names.py` is the one name matcher: quoted means exact, unquoted folds. `sum("AMOUNT")` against `amount` fails.
- **SQLite types.** `slayer/engine/unknown_types.py` types untyped fields with a zero-row probe (`WHERE 1 = 0`). SQLite types results by a census of stored values, which reports String when it sees no values. Ingestion (`slayer/database/sqlite_introspect.py`) instead starts from the declared column type and refines it with a bounded sample. So the same column gets two different types.

Principles that apply:

| Element | Principles |
| -- | -- |
| system | P8 (one reader of Mode B), P10 (one typing), P11 (fail at save time) |
| core | P5 (typed errors) |
| engine | P2, P3, P8 (models persist verbatim) |
| sql | P2 (dialect quirks only in `dialects/`), P9 (fail closed) |
| database | P1 (one database door) |
| importers | P1 (importers preserve meaning, up to documented NULL conventions) |
| semantics | P4 (Ossie superset) |

No arc42 or LikeC4 edit is needed. The spec deltas modify existing capabilities only, so there is no new spec attachment.

## Goals / Non-Goals

**Goals:** see the specs.

**Non-Goals:**

- SQL's meaning for `ARRAY_AGG` (NULL element kept, NULL on no rows, unspecified order). That is DEV-2499.
- `FIRST_VALUE` / `LAST_VALUE` frame and partition coverage beyond the import path, which DEV-2457 covers.
- Live-data invalidation of resolved unknown types. The per-engine, per-storage-fingerprint snapshot stays.
- Quoting of physical source columns, which stays database-native.

## Decisions

Each entry gives the decision, the options weighed, the reason, and who took it.

1. **The SQLite untyped-field gap is in scope.** Options: fix the probe here; narrow the SQLite scenarios to typed documents and file an issue; xfail (no issue existed). The issue's acceptance names SQLite, the corpus is mostly untyped, and the cause is one probe. The promise is the type ingestion would give, a data-dependent best guess; a user who wants certainty declares `datatype`. (user)

2. **`ARRAY_AGG(DISTINCT x)` keeps `collect`'s meaning for now; it is documented and pinned.**
   - Options: keep and document; load an `ANSI_SQL`-entry `ARRAY_AGG` as not evaluable.
   - Why keep: Ossie defines no `ARRAY_AGG` (extension functions need a dialect, `expression_language.md` "MAY Support (Extensions)"), and strict mode already refuses it.
   - Upstream search (apache/ossie issues, PRs, discussions, commits; the dev@ list; apache/ossie-converters) found only the non-normative FILTER note and the dev@ thread it came from. Nothing rules on it.
   - The clean-up to SQL's meaning is DEV-2499, filed with an empty worktree. (user)

3. **A declared name's normalized form follows upstream `declared_name` (`validation/validate.py`) exactly.** A name readable unquoted as the second part of a reference is upper-cased; a keyword that cannot open a reference unquoted is upper-cased too; any other name is kept as written. A parity test checks it against the vendored validator. (default, Ossie)

4. **One matcher change covers every namespace, native models and strict binding included.**
   - Natively the change only turns an unresolved reference into a match, so no input that resolves today changes meaning.
   - Scoping the rule to Ossie-origin documents would give one language two quoting rules, against system P8.
   - Any resolution path that compares quoted names outside the matcher is routed through it. (default, structural)

5. **Physical source columns keep database-native quoting**, per Ossie: "Comparing quoted and non-quoted identifiers is DB specific". (default, Ossie)

6. **`FIRST_VALUE` / `LAST_VALUE` stay as DEV-2457 left them** (its decision 17 and `queries/window-functions`). This change adds symmetric import-path scenarios only. (default)

7. **The `CASE` / scalar item is tests and docs only.** No defect was found; the new fixture test is the coverage the corpus cannot give. (default)

8. **The corpus guard is a committed manifest** of (document, entity, reason category) for every entity reported as not evaluable. Reason categories: unqualified field, unaggregated field, aggregate over no dataset, source measure, no readable SQL, unparseable. (default)

9. **Existing tests pinning the old quoted rule are aligned mechanically, since Ossie decides them.** Each respells its "wrong case" to one that still misses. Others found later get the same treatment.
   - `tests/test_dev2215_identifier_case.py`: `"ORDERS"` → `"Orders"`, `orders."STATUS"` → `orders."Status"`, `"ITEMS".value` → `"Items".value`, `items."VALUE"` → `items."Value"`.
   - `tests/test_dev2217_errors.py:128`: `"CUSTOMERS"` → `"Customers"`.
   - `tests/test_dev2021_stored_conversion.py` (two tests): `"REGIONS"` → `"regions"`.
   - `tests/test_stored_upgrade_filter_literals.py`: `"OPENED_ON"` → `"opened_on"`, `"REGIONS"` → `"regions"`. (default, Ossie)

10. **The quoted-name ruling is recorded on DEV-2452** as a comment, as its item 7 asks. (default)

11. **One SQLite classifier is shared by ingestion and unknown-type resolution.**
    - A field reading one source column gets the column's declared type refined by the bounded sample, with ingestion's exact fallbacks: declared type kept on an empty, all-NULL or saturated sample or a failed probe; text or blob makes it String; coercible text makes it Float.
    - An expression field gets the bounded census of its values, and with no non-NULL value stays String, as today.
    - Parity tests cover each branch for a single-column field.
    - The SQLite-specific part lives in the SQLite dialect / introspection modules (sql P2). The sample goes through the client (database P1). (codex)

12. **Normalized-name parity is table-driven** against the vendored validator: ASCII, `été`, `from`, `alter`, whitespace, punctuation, an embedded quote, a dotted name, and exact-versus-normalized collisions. (codex)

13. **Import-path window scenarios are symmetric**: `FIRST_VALUE` and `LAST_VALUE`, ascending and descending, NULL keys, an explicit `NULLS FIRST`; values come from a raw-SQL oracle. (codex)

14. **The quoted-name docs sentence says exact first, then the normalized form, then ambiguity.** (codex, folded part)

15. **Rejected (codex):** convert `ARRAY_AGG(DISTINCT x)` only where provably equal, else load as not evaluable. Decision 2 rules it; importers P1 allows documented NULL conventions; refusing would regress input that evaluates today.

16. **Rejected (codex):** scope the quoted-name fallback to Ossie-origin documents. See decision 4.

17. **Rejected (codex):** key the SQLite type cache on live data. The specified contract is once per engine and storage fingerprint on every dialect; a per-engine snapshot keeps types stable across queries, and ingestion's own types are a persisted snapshot.

18. **Rejected (codex):** per-construct assertions over the corpus. The corpus holds no scalar-around-aggregate metric, so that coverage lives in the fixture test.

19. **Rejected (codex):** treat the SQLite refusal of `ARRAY_AGG` as a fidelity failure. SQLite has no lists, so every list aggregate is a typed refusal there. This follows the DEV-2459 precedent that a dialect refuses a construct with a typed error where it has no translation.

## Risks / Trade-offs

- **[Risk]** A SQLite type taken from sampled values can differ from what later data would give. → This is the same policy as ingestion, stated in the spec; declaring `datatype` removes the guess.
- **[Risk]** Sampling rows at query time costs a bounded scan. → It runs once per engine and storage fingerprint, bounded by ingestion's sample cap.
- **[Risk]** A resolution path outside `slayer/core/names.py` keeps the old exact-only rule. → The matcher change is paired with a search for direct quoted comparisons, and the strict and per-segment scenarios exercise the main paths.
