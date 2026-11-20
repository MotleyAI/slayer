## Context

See proposal.md — Why. Measured on the base (DEV-2277 at `7a5fb8aa9` + origin/main):

- ~3,110 `DatasetField(sql=…)` in tests: ~2,490 bare identifiers (production `DatasetField._sql_shorthand` already turns
  an identifier into the `expression` shorthand), ~600 non-identifier native SQL, ~52 non-literal values; 319
  dict-literal fields with `"sql"`; ~1,775 `.sql` attribute reads, of which only those on `DatasetField`/`Column` are in
  scope.
- Production conversion seam: `slayer/sql/definitions.py::with_converted_sql` (called by `resolved_dataset` in
  `slayer/engine/definitions.py` and by `check_forms`) turns a non-identifier `sql` into
  `ExpressionObject(dialects=[DialectEntry(dialect=ossie_token(ds_type), expression=<siblings qualified>)])`; a portable
  form compiles through `portable_native`. Probe: `a * 2`, `UPPER(s)`, `COALESCE`, `CASE` render unchanged on SQLite,
  DuckDB and Postgres; `a / b` and Postgres `ROUND` render differently.
- `dr-refactor retarget DatasetField.sql → DatasetField.expression` dry run: accepted; 306 attr-blindspots, 8 unlinked
  keywords, ~33 stand-ins (other classes' `.sql`).
- Every YAML / embedded-YAML `sql:` key in `tests/` is Cube (`tests/fixtures/cube_project/**`, `test_cube_parser.py`),
  an Ossie expression object (`tests/fixtures/osi/**`, `ossie_vocabulary/*.osi.yaml`) or a frozen upgrade corpus
  (`tests/fixtures/upgrade/v0_10_2`, `v1_8_2`).
- Stacked on the unmerged DEV-2277 branch; it may still move.

## Goals / Non-Goals

**Goals:**
- Every test field outside the deprecated-spelling tests is spelled with `expression`, with every test's meaning kept.
- A static guard keeps it so.

**Non-Goals:**
- Production code, behaviour, specs, the LikeC4 model and arc42 (no principle binds test code), except the search-text fix (task 4.3).
- `sql` on other classes: `AggregationParam`, `SlayerDataset` (sql-mode), Cube models, `translate(...)`, responses.
- YAML fixtures (all foreign formats or frozen corpora).
- A runtime warning guard (rejected for the static guard, decision 2).

## Decisions

1. **Value-pass rule** (user): a native site becomes portable `expression="<qualified text>"` only where that keeps
   every field's resolved SQL unchanged on every dialect the site reaches; otherwise one dialect entry per reached
   dialect. Alternative weighed: always the dialect entry (what production stores) — rejected: verbose, dialect-pinned,
   and leaves the recommended portable spelling untested. A native site that never reaches a datasource becomes
   portable; its passing test confirms it.
2. **Regression guard** (user): a static AST guard test. Alternatives: a runtime `filterwarnings` error (exemptions leak
   onto importer tests that legitimately write `sql`; the warning text is shared) and no guard (decays until 3.0).
3. **Deprecated-spelling tests** (default): a test keeps `sql` iff its assertion depends on the input being the
   deprecated spelling — it pins the conversion, the `FutureWarning` / `deprecated_input` report, a stored document
   below the current SlayerModel version, or an importer writing `sql`. Judged per test, not per file.
4. **CI-only backends** (default): MySQL, ClickHouse, SQL Server, Trino, Snowflake cannot run locally; their dialects
   are compared offline (decision 15), and a site whose dialect is unknown gets dialect entries.
5. **YAML fixtures unchanged** (default): every hit is Cube, Ossie or a frozen upgrade corpus.
6. **Dict-literal fields** (default): respelled, key and value, by the value-pass script; the refactor tool cannot
   reach untyped keys.
7. **Test helpers** (default): a parameter named `sql` forwarded into a field definition is renamed `expression` in the
   keyword pass.
8. **Tooling** (default): the recording plugin and value-pass script are one-shot scratch tooling, never committed; the
   owner sees the substitution summary before anything is written.
9. **Base** (user brief): stacked on DEV-2277; every stage re-merges the DEV-2277 branch and origin/main (merge, never
   rebase).
10. **`skip_specs: true`** (default): test-only, no behaviour change.
11. **End-to-end equivalence** (codex): the comparison runs the dataset through `resolved_dataset` twice — as written
    (deprecated `sql`) and with the proposed replacement — and compares every field's resolved native SQL per dialect,
    both sides normalised by the same sqlglot parse-and-render; any binding error or mismatch gives dialect entries.
    All fields of a dataset are rewritten together and compared on every field, so sibling expansion and filters are
    covered.
12. **`.sql` reads by intent** (codex): never retargeted mechanically; each read on a `DatasetField`/`Column` is
    rewritten to what its assertion checks (`expression`, `portable_text(...)`, a dialect entry, or `definition_sql`
    after resolution) and listed in the substitution summary.
13. **Keyword-only retarget** (codex): the keyword pass's dry-run diff may hold only constructor-keyword and dict-key
    edits; an attribute read, write or `model_copy(update=…)` edit in it is a stop and goes back to the value pass.
    Blind spots are handled as `la:deterministic-refactor` prescribes.
14. **Recording coverage and reconciliation** (codex): the recording plugin wraps `with_converted_sql`; a static
    inventory proves every non-identifier site is observed, statically classified, or a deprecated-spelling exemption —
    an unreconciled site fails the pass.
15. **Replacement unit** (codex): the literal's AST node, recorded with dataset, sibling set and dialect per
    construction; one node with conflicting outputs (helpers, comprehensions, shared fixtures) is fixed at its call
    sites (the helper takes `expression`, callers pass their own values) and flagged in the summary. A site's dialect
    set is its observed dialects plus the backend of every CI-only test file importing its defining module; unobserved
    backends are compared offline through `resolved_dataset` with that `ds_type` and the recorded live types.
16. **Guard reach** (codex): the guard also catches import aliases of `DatasetField`/`Column`,
    `model_validate({... "sql" ...})` field payloads and `model_copy(update={"sql": …})` on fields, and has detector
    unit tests on synthetic snippets.
17. **Allowlist precision** (codex): each entry is `path::function` with the expected spelling count and a reason; a
    count mismatch (more, fewer, or none left) fails.
18. **Raw payload versions** (codex): a raw payload declaring a version below the current SlayerModel version keeps
    `"sql"`; unversioned and current-version payloads are respelled and still go through the real load pipeline.
19. **Identifier classification** (codex): by production `unquoted_identifier`, never a regex; quoted, bracket and
    backtick identifiers are native.
20. **CI evidence** (codex): done includes the path-gated per-backend integration workflows, triggered because their
    test files change.
21. **Rejected (codex): unobserved native sites must not default to portable** — reopens decision 1, which the user
    chose with that case included; deferral-pinning tests keep `sql` under decision 3, serialization expectation
    changes go through the consent gate, shared-fixture reach is covered by decision 15.
22. **Rejected (codex): a runtime construction hook in the guard** — the user chose the static guard (decision 2).

## Risks / Trade-offs

- [DEV-2277 still moves; its task 4.1 respells queries in the same test files] → re-merge it at each stage start;
  conflicts are mechanical respellings.
- [A field reached only by locally skipped parametrisations] → decision 15's static dialect set plus offline
  comparison; unknown dialect → dialect entries.
- [A rewrite changes an expected value] → listed separately, applied only with the owner's consent.
- [The recording run is slow (whole unit suite + local integration)] → one-shot; re-run only for sites whose dataset
  changed after a merge.
