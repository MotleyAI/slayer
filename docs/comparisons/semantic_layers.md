# SLayer, Malloy, Cube and MetricFlow compared

The [feature comparison on motley.ai](https://motley.ai/semantic-layer-comparison) grades SLayer, Malloy, Cube Core
and MetricFlow on 48 capabilities, from re-aggregation and calendar-aware time shifts to gap filling, BI access and
row-level security. Every verdict marked *verified* is backed by a probe in the
[comparison suite](https://github.com/MotleyAI/slayer/tree/main/examples/comparisons), which runs one dataset with
deliberate edge cases through all four engines and checks each answer against hand-written SQL; the
[suite README](https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/README.md) explains how to rerun it
and lists the upstream bugs it found.

For SLayer against the dbt Semantic Layer specifically, including importing dbt definitions, see
[SLayer vs dbt](../dbt/slayer_vs_dbt.md).
