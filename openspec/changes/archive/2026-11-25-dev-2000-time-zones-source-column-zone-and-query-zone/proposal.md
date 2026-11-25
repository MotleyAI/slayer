## Why

SLayer has no notion of time zones: timestamps are bucketed, extracted, compared and returned in whatever zone the stored value happens to be in, so a UTC-stored order at 23:30 lands in the wrong local day and a local-day bucket needs dialect-specific SQL. It also reads two clocks — relative tokens the SLayer host clock, SQL `now()` / `current_date()` the database clock. Ossie distinguishes `DateTime` (civil, no zone) from `DateTimeTz` (an instant) but defines no conversion; SLayer 2.x adds zones as an opt-in extension that changes nothing for a query without one.

## What Changes

- New optional `time_zone` on `SlayerQuery` (and `QueryRefinement`): the zone results are bucketed, compared and returned in. The final stage's zone is the query's; a stage without its own inherits it, including the stored stages of query-backed datasets the query reads.
- New optional `time_zone` on `DatasourceConfig` (default source zone of its `DateTime` fields) and on `DatasetField` (an IANA zone or `civil`, on a `DateTime` field whose expression is a plain column).
- Under a query zone, every read of a dataset timestamp is converted once, at the read, to the query zone's wall clock: `DateTimeTz` from its instant, `DateTime` from its source zone; `civil` passes through; an undeclared `DateTime` is read as stored with a `time_zone_undeclared` warning. Derived fields and dataset metrics convert their reads; field filters, dataset filters and row-level security see stored values.
- Under a query zone one engine-clock reading, taken in the zone, drives relative tokens, `whole_periods_only`, the spine's implied bound and `now()` / `current_date()` (rendered as literals). Without a zone the clocks are unchanged.
- Stage outputs carry their zone state; re-bucketing a bucketed field across zones is a typed error.
- Converted time bounds keep a raw-column pre-filter so they stay index- and partition-prunable.
- Conversion renders on all nine Tier-1 dialects; Tier-2 dialects fail closed when a conversion is needed.
- Ingestion types BigQuery `TIMESTAMP`, ClickHouse zoned `DateTime` / `DateTime64` and MySQL `TIMESTAMP` as `DateTimeTz`; re-ingest re-types a stored `DateTime` field whose live column is an instant; stored BigQuery `TIMESTAMP` fields load as `DateTimeTz`. Results and SQL of queries without a zone are unchanged.
- `SlayerResponse.time_zone` reports the effective zone.
- New system principle 12 (zoned reads) and a live-schema principle 1 exception (the `DateTimeTz` re-type).

## Capabilities

### New Capabilities
- `queries/time-zones`: source zones, `civil`, the query zone and its stage inheritance, the read rule and its stored-predicate exception, stage zone state, cross-zone re-bucketing, the zoned clock, DST resolution, the pre-filter, per-dialect rendering, validation, warnings, surfaces and the response field.

### Modified Capabilities
- `queries/time-points`: relative tokens read the engine clock in the query zone when one is set.
- `queries/date-functions`: `now()` / `current_date()` render from the engine clock in the query zone when one is set; only `now()` then bypasses the cache.
- `queries/mixed-type-coercion`: the BigQuery zoned-column rule is keyed on `DateTimeTz` rather than `db_type`.
- `queries/saved-query-refinement`: `time_zone` is refinable and replaces the saved final-stage zone.
- `compatibility/ossie-vocabulary`: BigQuery `TIMESTAMP`, ClickHouse zoned `DateTime` and MySQL `TIMESTAMP` ingest as `DateTimeTz`; re-ingest re-types a `DateTime` field whose live column is an instant; `DateTimeTz` behaves as the timestamp type only without a query zone.
- `models/stored-upgrade`: stored BigQuery `TIMESTAMP` fields load as `DateTimeTz`.

## Impact

- `slayer/core`: `DatasetField`, `DatasourceConfig`, `SlayerQuery`, `QueryRefinement`, `StageField`, the `SLAYER` field payload, a new warning kind, typed errors.
- `slayer/engine`: clock contract and bundle, stage zone inheritance and propagation, re-bucketing check, cache bypass, response.
- `slayer/sql`: one typed read path with value / stored-predicate context, pre-filter lowering, AST-level read check; `dialects/*`: conversion primitives, converted-type literals, SQLite `slayer_convert_tz`, vendored CLDR IANA→Windows table, MySQL zone probe via the engine's client.
- `slayer/live_schema`: ingestion types, re-ingest re-type; `slayer/storage`: `SlayerModel` 30 → 31 load upgrade.
- Surfaces: REST `QueryRequest` and refine, MCP `query` / datasource / dataset tools, CLI `datasources create --time-zone`, inspect, help memory.
- Docs: `docs/concepts/time.md` and one sentence each in datasets, queries, datasource configuration, MCP and REST references, database support.
- `architecture/system.arc42.md` (principle 12), `architecture/python.live_schema.arc42.md` (principle 1).
