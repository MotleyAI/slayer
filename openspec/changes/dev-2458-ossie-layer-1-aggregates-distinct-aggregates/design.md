## Context

See proposal.md › Why. Today Mode B maps `COUNT(DISTINCT x)` → `count_distinct` and `PERCENTILE_CONT(p) WITHIN GROUP
(ORDER BY x)` → `percentile(x, p=p)` in `core.syntax` (`_DISTINCT_CALLS`, `_ordered_set`); every other DISTINCT and
WITHIN GROUP is refused. Aggregations are keyed by name everywhere (`core.enums` tables, `sql/render/aggregates.py`
registry, `allowed_aggregations`, usage reporting, importers, exporter). Databases without an aggregate
`PERCENTILE_CONT` (MySQL, Trino, BigQuery) split the SELECT via `sql/dialects/_ordered_set_split.py`; SQL Server uses a
windowed percentile + MAX; SQLite registers `percentile_cont` / `percentile_disc` UDFs; ClickHouse emits the
reservoir-sampled `quantile(p)`. Strict (`ossie-l3`) judgment reads the one parse (`core/ossie_conformance.py`) plus the
parser's reports of written callees and non-Ossie spellings.

Principles that apply: system P4 (AST), P8 (one Mode B reader), P10 (one typing), P11 (fail at save time); semantics
P2.1 / P2.2 (one meaning on every dialect, a typed refusal otherwise), P4 (Ossie superset); engine P2 (every spelling
collapses to one node), P8 (models persist verbatim); core P4 (one key kind per meaning); sql P1 (AST end to end), P2
(dialect quirks only in `dialects/`), P9 (fail closed), P14 (emitted types are logical types); importers P1 (preserve
meaning). No arc42 or `.c4` edit; no new cross-node import. The new capability `aggregations/distinct-and-percentiles`
sits in the `aggregations` group, already cross-cutting in `architecture/index.yaml`.

## Goals / Non-Goals

**Goals:** the four built-ins on every Tier-1 dialect with one meaning; Ossie spellings parsed to them; strict admits
exactly Ossie's aggregate spellings that run; exact percentiles everywhere (ClickHouse fix); importers and exporter
carry the new names.

**Non-Goals:**
- DISTINCT in any other aggregate (`stddev`, `median`, …), multi-operand DISTINCT, `WITHIN GROUP (ORDER BY x DESC)`,
  other ordered-set functions (`MODE()`), DISTINCT / WITHIN GROUP aggregates inside SQL `OVER` windows (DEV-2457).
- `percentile_disc` on a sole primary key; any change to the `associated` warning (DEV-2272).
- A keyless home or duplicated join paths under association — generic association machinery this change does not touch.

## Decisions

### Representation
- **D1 (user)** — DISTINCT aggregates are new built-in names `sum_distinct` / `avg_distinct`, following
  `count_distinct`, not a `distinct` flag on the aggregation node. A flag would give `count_distinct` and `count`+flag
  one meaning (core P4) and reshape every name-keyed table; names keep keys, typing and registries unchanged.
- **D5 (default)** — `MIN/MAX(DISTINCT x)` parse to `min` / `max`: identical value on every input.
- **D10 (default)** — Names `percentile_disc` / `percentile_approx`, after Ossie's `PERCENTILE_DISC` and SLayer's
  `count_distinct_approx`.
- **D6 (default)** — `percentile_disc` admits every ordered type (numeric, text, date, timestamp, boolean) and preserves
  it: SQL's PERCENTILE_DISC is defined on any orderable type and Ossie does not narrow it, so numeric-only would refuse
  Ossie-valid input (semantics P4). Boolean reaches it through the numeric default set.
- **D8 (default)** — `WITHIN GROUP` keeps no `DESC`: Ossie's syntax has none.
- **D9 (default)** — DISTINCT / WITHIN GROUP aggregates inside an SQL `OVER` window are refused (DEV-2457).
- Rejected (codex #12, partial): adding `percentile_disc` to `PRIMARY_KEY_AGGREGATIONS` — that set deliberately holds
  only counts, extremes and `collect`; `median` / `percentile` are absent too.

### Approximate percentile
- **D2 (user)** — Add `APPROX_PERCENTILE` as `percentile_approx`: the database's approximate percentile where it takes
  an arbitrary `p` (DuckDB `approx_quantile`, Snowflake `APPROX_PERCENTILE`, Trino `approx_percentile`, ClickHouse
  `quantile(p)`, SQL Server 2022 `APPROX_PERCENTILE_CONT`), exact `percentile` elsewhere (Postgres, SQLite, MySQL,
  BigQuery — `APPROX_QUANTILES` takes only integer bucket counts). An exact value is within Ossie's tolerance.
  Alternative: keep it refused per the issue's "only if free".
- **(codex #5)** — `percentile_approx` casts to the float type `percentile` derives on every native path (sql P14);
  result-type tests for INTEGER and DECIMAL inputs on each native path.

### Emission
- **D3 (user)** — ClickHouse exact `percentile` / `median` move from reservoir `quantile(p)` to
  `quantileExactInclusive(p)` (R-7 = PERCENTILE_CONT), fixed here rather than on a separate issue (none existed);
  `quantile(p)` becomes `percentile_approx`'s native form.
- **(codex #4)** — ClickHouse percentile translations are guarded by a non-NULL count so an empty or all-NULL group is
  NULL, never NaN or a type default; tested for nullable / non-nullable numeric, date and timestamp inputs.
- `percentile_disc` per dialect: native `PERCENTILE_DISC … WITHIN GROUP` on Postgres, DuckDB, Snowflake; the SQLite UDF
  widened to any ordered type; the ordered-set split with a new discrete pick (rank ⌈p·n⌉ of the non-NULL values,
  rank 1 at p = 0) on MySQL and Trino; BigQuery's analytic `PERCENTILE_DISC(x, p) OVER (PARTITION BY keys)` collapsed
  by MAX via the split; ClickHouse via the split unless a native exact function's index rule matches ⌈p·n⌉ exactly.
- **(codex #1)** — SQL Server dispatches by input type: native windowed `PERCENTILE_DISC` only for numeric (not bit)
  input; text, date, timestamp and boolean always take the ordered-set split. Each fallback type is tested.
- **(codex #6)** — The outer MAX collapse stays (valid on every type `percentile_disc` admits); tests cover NULL
  grouping keys, no GROUP BY, several grouping keys and text / date results on BigQuery.
- `sum_distinct` / `avg_distinct` are `SUM/AVG(DISTINCT x)` on every dialect (ClickHouse accepts the DISTINCT form);
  `avg_distinct` stays fractional where `avg` is.

### Strict validator and export
- **D7 (default)** — Strict refuses `AVG/MIN/MAX(DISTINCT)`: Ossie §240 defines DISTINCT only for SUM and COUNT.
  Native mode accepts them as SLayer extensions.
- **(codex #3)** — The parser records the DISTINCT modifier of a call in its non-Ossie spelling report
  (`nonossie_spellings`), so strict refuses `AVG/MIN/MAX(DISTINCT …)` after `MIN`/`MAX` collapse to `min`/`max`. No text
  regex; tests cover case, whitespace, comments and nesting.
- **(codex #7)** — A cross-product test: written spelling × `slayer`/`ossie-l3` × FILTER, case variants and nested
  expressions.
- **D11 (default)** — Export: `sum_distinct` and `percentile_disc` portable; `avg_distinct` and `percentile_approx`
  datasource-dialect entries (as `count_distinct_approx`).

### Importers
- **D4 (user)** — The shared SQL aggregate importer and dbt's percentile flags convert the new constructs.
- **D12 (default), codex #8 rejected** — dbt approximate + discrete → `percentile_disc`: the exact discrete value is
  within any approximation, which importers P1 accepts (meaning, not algorithm); refusing would drop an answerable
  measure. Codex's coverage point is folded: tests cover the measure and the metric path.
- **(codex #9)** — Only `APPROX_PERCENTILE(x, p)` with a literal fraction and no accuracy / weight argument converts;
  BigQuery `APPROX_QUANTILES` (bucket / array) is dropped with a report.

### Library growth
- **D16 (user)** — The four names are reserved under `models/stored-upgrade` › Library growth never invalidates a
  stored artefact: SlayerModel / SlayerQuery / Memory step to 32 / 20 / 15 through per-document no-op migrations, the
  four names take the rename gate {32, 20, 15}, and a stored custom granularity of one of these names, written below
  the gate, is renamed `<name>_granularity` on load by the existing rename. Model-scoped metric names need nothing:
  their name check runs only when a metric is written. Rejected: a carve-out reserving the names only in call
  position (one word, two meanings across stores); pinning them (silences the guard).

### Tests
- **(codex #2)** — Execution (not only emission) of the ordered-set split on MySQL and Trino: grouped / ungrouped, NULL
  values, empty / all-NULL groups, p = 0 / 1, duplicates, two percentiles in one SELECT, FILTER.
- **(codex #10)** — Invalid `p` (negative, > 1, NULL, string, boolean, non-literal, duplicate, positional + keyword)
  refused at save time and with the same typed error at query time (system P11).
- **(codex #11, partial)** — One integration association case per dialect-specific `percentile_disc` strategy (MySQL,
  Trino, SQL Server, BigQuery, ClickHouse) and for `sum_distinct`. Rejected: keyless-home / duplicated-join-path
  variants (generic association machinery, covered by its own tests).
- **(codex #12, partial)** — Boolean binding and execution tests for `percentile_disc`.

### Specs
- **D13 (default)** — No special `associated`-warning treatment for the new aggregates; DEV-2272 owns dropping it for
  duplicate-insensitive aggregations.
- **D14 (default)** — One new capability `aggregations/distinct-and-percentiles` under the cross-cutting `aggregations`
  group.
- **D15 (default)** — Remove the stale `aggregations/formula-templates` › "Median and percentile on Trino are
  approximate": Trino already emits the exact split (pinned by `tests/integration/test_integration_trino.py` ›
  `TestTrinoMedianPercentile`), and the new capability states exactness on every Tier-1 database.

## Risks / Trade-offs

- [The ordered-set split's discrete pick mis-ranks NULLs or picks the wrong row at p = 0] → executed edge-case matrix on
  MySQL and Trino (codex #2), cross-dialect agreement scenario.
- [Native approximate functions return integer / temporal types] → explicit cast to the derived float type, type tests.
- [ClickHouse NaN / default on empty input] → non-NULL-count guard, empty / all-NULL tests per input type.
- [Strict misses a DISTINCT after MIN/MAX collapse] → parser-recorded DISTINCT spelling, cross-product test.
- [A stored model defines a custom aggregation named like a new built-in] → the existing exact-name override keeps it
  answering (scenario in the new capability).
