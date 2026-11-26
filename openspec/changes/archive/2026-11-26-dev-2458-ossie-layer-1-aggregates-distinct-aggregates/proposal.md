## Why

Ossie Layer 1 requires `SUM(DISTINCT x)` and `PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY x)` and recommends
`APPROX_PERCENTILE(x, p)`; SLayer refuses all three, so Ossie documents using them do not load and the compliance gate
carries them as gaps. Separately, SLayer's exact `percentile` / `median` on ClickHouse emit the reservoir-sampled
`quantile`, which is approximate on groups of more than 8192 rows.

## What Changes

- Four new built-in aggregations: `sum_distinct`, `avg_distinct`, `percentile_disc` (every ordered type, an observed
  value) and `percentile_approx` (native approximate where the database has one, exact elsewhere).
- Mode B reads `SUM/AVG/MIN/MAX(DISTINCT x)` (MIN/MAX as plain `min`/`max`), `PERCENTILE_DISC(p) WITHIN GROUP (ORDER
  BY x)` and `APPROX_PERCENTILE(x, p)`; every other DISTINCT and WITHIN GROUP stays refused.
- Strict (`ossie-l3`) admits `SUM(DISTINCT x)`, `PERCENTILE_DISC … WITHIN GROUP` and `APPROX_PERCENTILE`, and keeps
  refusing `AVG/MIN/MAX(DISTINCT …)` and the SLayer plain-call spellings.
- Exact `percentile` / `median` on ClickHouse become `quantileExactInclusive`; empty and all-NULL groups yield NULL.
  The stale requirement that Trino's percentile is approximate is removed: percentiles are exact on every Tier-1
  database.
- The shared SQL aggregate importer (Cube, dbt, Ossie vendor-dialect expressions) and dbt's percentile flags convert
  the new constructs instead of dropping them; the Ossie exporter writes `SUM(DISTINCT …)` and `PERCENTILE_DISC …`
  portably and `avg_distinct` / `percentile_approx` as datasource-dialect entries.

## Capabilities

### New Capabilities
- `aggregations/distinct-and-percentiles`: the four built-ins — spellings, input types, result types, NULL and empty
  values, and per-dialect exactness of `percentile`, `median` and `percentile_disc`.

### Modified Capabilities
- `aggregations/functional-form`: the allowed DISTINCT forms (Syntax boundaries); positional `p` for the new
  percentiles.
- `aggregations/formula-templates`: remove "Median and percentile on Trino are approximate".
- `queries/expression-grammar`: `p` is a keyword argument of `percentile_disc` and `percentile_approx`.
- `queries/ossie-l3-binding`: the Ossie aggregate shapes strict admits.
- `importers/meaning-preservation`: the newly translatable DISTINCT, discrete and approximate percentile forms.
- `importers/dbt-import`: dbt's discrete and approximate percentile flags.
- `importers/osi-export`: portability of the new built-ins.

## Impact

`slayer/core` (syntax, enums, ossie_conformance, keys typing), `slayer/engine` (binding of the new names),
`slayer/sql/render/aggregates.py` and every Tier-1 dialect plus `_ordered_set_split.py`, `slayer/importers/aggregates.py`,
`slayer/dbt/converter.py`, `slayer/osi/exporter.py`, usage reporting, MCP query-tool text, help memory, docs, and the
Ossie compliance manifest. Storage versions step (SlayerModel 32, SlayerQuery 20, Memory 15) with no per-document
change, so a stored custom granularity named like a new built-in is renamed on load; no API removal.
