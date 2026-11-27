## Context

See proposal.md (Why). Measured against apache/ossie `main` c2980498 (2026-11-26) with the vendored corpus at
d4dca4e9; a run of the in-process suites with `SLAYER_OSSIE_CORPUS` at a fresh clone showed exactly these failures:

1. **Case layout.** Upstream PR 803 turned `compliance/wide_table/cases/<id>/{query.json,expected.txt,query.ossie.sql}`
   into `compliance/wide_table/<id>.ost` (format: upstream `ost/docs/ost.md`, `compliance/wide_table/README.md`).
   Six consumers read the old layout: `tests/_ossie_gate_suite.py` (`cases`, `read_expected`, `case_query`), the
   oracle test, the compile test, the in-process and server gate modules, the corpus-root test and the provenance
   test.
2. **Harvested `e6812ba8ec56` and `adcaa27290a5`.** `orders.order_count` has only `DAX` and `MDX` entries. SLayer
   correctly raises `UnevaluableEntityError` ("has no readable SQL expression"); the compile check's
   `_expression_text` (`tests/test_dev2243_corpus.py`) falls back to `entries[0]` and parses the DAX as SQL, so it
   expects `compiled`. Structural cause: the oracle derives the expected outcome from an entry SLayer never reads.
3. **Spec prose.** § "Approximate Aggregations (RECOMMENDED)" gained "Engine support for approximate aggregates
   varies. For a non-normative mapping, see [extensions/function_mappings.md](…)" and lost its database-support table
   and BigQuery note.
4. **BigQuery.** The gate's `server` job resolves its BigQuery leg to `bigquery-shared-gcp-project` without
   `queue: max`, so GitHub keeps one pending job and cancels the older one.

Everything else upstream changed (portability `m5_agg_ref`, `m7` `NULLS LAST`, `examples/minimal.yaml`, grader)
already passes.

Architecture: no `slayer/` package changes, so the import law and model are untouched; both modified specs are
cross-cutting (`compatibility`, `models`). System principle §3.6 (Pydantic, never dataclasses) binds the case model.

## Goals / Non-Goals

**Goals:**
- One reader of upstream's wide-table case scripts, shared by every suite, failing closed.
- Vendored corpus equal to upstream `main` at implementation time.
- `workflow_dispatch` of `ossie-compliance.yml` green on every engine with `corpus=vendored` and `corpus=upstream`.

**Non-Goals:**
- Upstream's Layer 2 `compliance/query/*.ost` scripts (DEV-2400).
- Comparing SLayer's SQL with the script's lowering (non-normative upstream).
- Checking that Section 8 errors identify their dimension, filter or metric (the case requires only the code).
- Changing `ci.yml`'s own BigQuery serialisation.

## Decisions

1. **The gate's BigQuery job keeps sharing `bigquery-shared-gcp-project` and adds `queue: max`**, rather than taking
   an own per-run group. A BigQuery schema changing while a `ci.yml` run is in progress breaks that run's tests, so
   the gate must queue behind it, not run beside it. A unit test parses every workflow under `.github/workflows`
   (reusable ones included), finds every job whose concurrency group can resolve to that group — matched
   case-insensitively, inside expressions too — and requires `cancel-in-progress: false` and `queue: max` on each;
   it also asserts the gate's BigQuery leg is in the group. Runtime queuing of matrix instances is verified by the
   `workflow_dispatch` runs, not unit-tested. (user; test scope (codex))
2. **Read only `.ost`.** The old layout no longer exists after re-vendoring; no dual reader. (default)
3. **Upstream `skip(...)` is ignored.** It marks a gap of upstream's own runner; upstream defines a skipped case's
   expected output as "the output that it should produce". Honouring it would drop a12, a16, a19, a20 and a21, which
   pass today. Rejected (codex): an allowlist of known skips failing on new ones — every routine upstream skip would
   turn the weekly run red for bookkeeping alone, and a skipped case SLayer fails already fails the gate. (default)
4. **A case's data and model come from its script.** `connect(name)` names one of ost's built-in fixtures, so the
   reader maps those names to upstream SQL (`retail` → `retail/retail.sql`, `stores` → `stores/stores.sql`); an
   unknown name fails with a tested diagnostic. `modelFile(name, path)` gives the model; the model `executeLower`
   names must equal the one `modelFile` defines. Engines are keyed by the loaded model, so `stores_ambiguous` is no
   longer hard-coded. (default; unknown-name and model-name checks (codex))
5. **Only the query and the expected rows or error code are read.** The lowering raw string is skipped. An error
   line is `> error: ` then a code matching `E_[A-Z0-9_]+`, then `:` or end of line; an error case has no table.
   (default; code grammar (codex))
6. **The reader fails closed.** Commands are split by `ost.md` § Commands (parentheses, `'…'` and `"…"` with doubled
   quotes, `/*…*/`, `--` comments, raw strings `{|…|}` / `{_|…|_}` / tagged fences; a `;` ends a command only when
   followed on its line by spaces); `#` lines outside commands are comments. Accepted commands: `connect`, `set`,
   `modelFile`, `skip`, exactly one `executeLower`. Its output must be exactly one error line, or one raw string
   followed by one well-formed table (border lines, header, separator, rows of the header's width). Anything else
   raises naming the script and line. Synthetic unit fixtures cover each lexical construct and each failure.
   (default; structure and lexical tests (codex))
7. **Table cells are read in ost's format** with JSON-string-aware row splitting: empty is NULL, a quoted cell is
   JSON-decoded (so `"a|b"`, `""`, `" a"`, `\uXXXX` keep their text), a number is a `Decimal`, anything else is the
   text. Comparison is unchanged (multiset by logical type; `162.50` matches `162.5`). (default; quoted-pipe
   tokenisation (codex))
8. **Class-wide oracle fix for non-SQL metrics.** `_expression_text` reads, in order, the top-level `sql`,
   `OSSIE_SQL_2026`, `ANSI_SQL`, then any entry whose dialect has a SQL reader; a metric with none is expected
   `unevaluable`. Alternative rejected: declaring the two documents, which leaves the class open. (default)
9. **Spec prose:** the new line joins `KNOWN_PROSE`; `**Database Support:**` and the BigQuery `**Note**`, gone from
   the spec, leave it. (default)
10. **Re-vendor with `scripts/ossie_schema_drift.py --vendor`** at implementation time; its "listed before, gone
    upstream" rule removes `cases/`. A new test asserts every file under the vendored corpus except `provenance.yaml`
    and `harvested/` is listed in the provenance, so no stale file survives. Further upstream drift is triaged as in
    the issue: a declaration update, or a strict xfail against an existing construct-family issue; anything else
    stops for the user. Counts pinned to the corpus (e.g. the `examples/` count, 10 → 13) are updated mechanically.
    (default; stale-file test (codex))
11. **Layer 2 stays out of the gate.** The gate excludes `compliance/query` as a whole (upstream now keeps Layer 2
    scripts beside `scott/`); vendoring them is DEV-2400. (user, from issue)

## Risks / Trade-offs

- [Upstream keeps moving between planning and implementation] → re-vendor at implementation time and triage any new
  drift by decision 10's rule; the weekly run catches later drift.
- [The reader is stricter than ost on content upstream may legitimately add] → it fails loudly naming the line, and
  the weekly run surfaces it; extending the reader is then a deliberate change.
- [Sharing the BigQuery group delays PR BigQuery checks behind the nightly gate] → accepted (decision 1).
