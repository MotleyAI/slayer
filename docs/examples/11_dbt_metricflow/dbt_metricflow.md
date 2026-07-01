# From dbt MetricFlow to SLayer

SLayer can ingest a dbt [MetricFlow](https://docs.getdbt.com/docs/build/about-metricflow) semantic layer — its `semantic_models` and `metrics` — and turn it into queryable SLayer models. This worked example runs the real [dbt-labs ACME Insurance benchmark](https://github.com/dbt-labs/semantic-layer-llm-benchmarking) through the converter end-to-end and answers two of its questions, checking each answer against the benchmark's gold SQL.

The companion notebook ([`dbt_metricflow_nb.ipynb`](dbt_metricflow_nb.ipynb)) is self-contained — everything it generates lands in a gitignored `.cache/` directory next to it:

1. **Clone** the dbt project at a pinned commit (a shallow `git` fetch of a few MB; reused on later runs).
2. **Load** its CSV data into a local DuckDB file.
3. **Convert** the dbt MetricFlow definitions into SLayer models with [`DbtToSlayerConverter`](../../dbt/dbt_import.md).
4. **Query** the converted models with hand-written SLayer queries — and verify against gold SQL.

## What the conversion produces

Each dbt **semantic model** becomes a SLayer model; each dbt **metric** folds into a `ModelMeasure` formula on its source model. The second query showcases the metric types this conversion handles:

- `loss_payment_amount` and `loss_reserve_amount` are **simple metrics with a filter** (`has_loss_payment = 1` / `has_loss_reserve = 1`). The converter pushes the filter down so each becomes a filtered aggregate.
- `total_loss_amount` is a **derived metric** — `loss_payment_amount + loss_reserve_amount` — expressed as a formula over the two filtered metrics.

## The two queries

| Question | SLayer query | Verified against |
|----------|--------------|------------------|
| How many claims do we have? | `{"source_model": "claim", "measures": ["*:count"]}` | `SELECT COUNT(*) FROM claim` |
| Total loss by claim number | `total_loss_amount` grouped by `claim.company_claim_number` | the benchmark's multi-join gold SQL |

The claim-number grouping reaches across a join that the converter inferred from the dbt entities — no manual SQL join is written. Both answers match the gold SQL exactly.

## Gold checks run up front

SLayer opens the DuckDB file through a read-write engine, and DuckDB will not let a second raw connection share the file under a different configuration. The notebook therefore runs every gold SQL query **before** any SLayer query touches the file, caches the expected numbers, and compares afterwards.

## Further reading

- [Importing dbt Semantic Layer definitions](../../dbt/dbt_import.md) — the full conversion reference, including what is converted exactly and what fails cleanly.
- [SLayer vs dbt](../../dbt/slayer_vs_dbt.md) — how the two semantic layers compare.
