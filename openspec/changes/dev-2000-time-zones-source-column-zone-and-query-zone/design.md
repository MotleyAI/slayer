## Context

See proposal.md › Why. Current state, at `ff353e943`:

- `DataType` already has `DateTime` and `DateTimeTz` (`slayer/core/enums.py`), and they survive Ossie import, storage and export unchanged. Both map to `TypeKind.TIMESTAMP`, so the engine can't tell them apart.
- Ingestion narrows only types whose names carry a zone (`finer_datatype`). BigQuery `TIMESTAMP` stays `DateTime` and gets `db_type: TIMESTAMP` (DEV-2120's interim rule). ClickHouse `DateTime('tz')` and MySQL `TIMESTAMP` stay `DateTime`.
- Physical column reads reach SQL through about eight `dialect.read_column(…, zoned=…)` call sites (`slayer/sql/scope.py` `_anchor`, generator `_join_key`, `_joined_or_local_dim_expr`, `_dim_field_expr_from_planned`, `_raw_time_col_expr_for_planned`, `_ranked_value_expr`, zoned aggregate values). Several reads bypass the hook:
  - `_resolve_sql` for non-zoned aggregate inputs
  - `_derived_field_expr` and the inner reads of `field_expansion`
  - semi-join key lambdas
  - `_anchor` string anchors
  - session-policy predicates
- `_anchor(FieldSqlKey)` expands a field's value and its `filter` together.
- Clock: `QueryEngine(clock=…)` defaults to the naive host-local `host_clock()`, read once into `ResolvedSourceBundle.now`. Relative tokens, `_snap_whole_periods` and `plan_spine` consume it. `now()` / `current_date()` render as database-clock SQL, and `plans_read_clock` bypasses the cache for them.
- `QueryRefinement` merges into the final stage only (`refine_query`). `StageField` carries `granularity`, which is stamped onto query-backed fields.
- Ossie defines no zone semantics. Under `conformance="ossie-l3"`, `slayer/engine/ossie_l3.py` already refuses any key that isn't part of an Ossie query.

## Goals / Non-Goals

**Goals:**
- One read path, so no operation can see an unconverted value (system principle 12).
- Zero SQL or result change for a query without a zone, proven by goldens.
- Fail loudly (a warning or a typed error), never silently wrong.

**Non-Goals:**
- Facade `SET TimeZone` → query zone (DEV-2032).
- A default query-zone setting (the issue asks only for a query-level zone).
- A database-clock probe.
- Zone offsets in time-point literals. They stay rejected; the `format-datetimetz-offset-literal` and `format-datetimetz-truncation` gaps stay with DEV-2244.
- Changing BigQuery's `TRY_CAST(… AS TIMESTAMPTZ)` → `DATETIME` (the 2.x rule).

## Decisions

### D1. Under a zone, values are the zone's wall clock, converted once at the read (user)
Every dataset timestamp read under Q becomes Q wall-clock time (naive), and every existing temporal operation and dialect primitive then works unchanged on it.
- **Rejected:** keeping instants and using Q only as the calendar. That needs a conversion at every calendar operation (about 10 sites × 9 dialects), which is the "many sites agreeing by hand" bug class.
- **Cost:** a `DateTimeTz` comes out naive under Q, and elapsed-time arithmetic across a DST transition is off by the shift. Both are documented, and the response reports Q.

### D2. Instant vs naive comes from the Ossie datatype (default)
`DateTimeTz` is an instant and `DateTime` is naive. Ossie decides this. The October plan's `db_type` classification is dropped.

### D3. Ingestion fixes and the load upgrade (default)
- BigQuery `TIMESTAMP`, ClickHouse `DateTime(tz)` / `DateTime64(p, tz)` and MySQL `TIMESTAMP` ingest as `DateTimeTz`.
- Stored BigQuery fields with `DateTime` plus `db_type: TIMESTAMP` load as `DateTimeTz` (`SlayerModel` 30 → 31; the upgrade needs the datasource type, so it applies only when the dataset's datasource is BigQuery).
- The BigQuery zoned read and `bare_comparands` re-key on `DateTimeTz` with identical SQL.
- The rejected alternative is Codex's suggestion to put the re-typing behind an opt-in: it would leave wrong Ossie types in place. No-zone goldens prove that SQL and values stay identical instead. This matters because `DateTimeTz` reaches `template_type.py` (`TIMESTAMPTZ`), the facade's `canonical_datatype` and response metadata. (codex, partial)

### D4. Re-ingest re-types `DateTime` → `DateTimeTz` (user, approved wording)
live_schema P1 becomes: "user metadata is never overwritten by a re-ingest (documented exceptions: the `source_kind` refresh, a non-list column re-typed to the list its live column is, and a `DateTime` field re-typed `DateTimeTz` when its live column is an instant)".

### D5. Source zone order is field, then datasource, then undeclared (user, October plan)
- The datasource default applies only to `DateTime` fields.
- A field `time_zone` is allowed only on a `DateTime` field whose expression is a plain column. It is refused on `DateTimeTz`, `Date`, non-temporal and derived fields, at construction and on assignment (core P9).
- Since D8, a derived field's reads are already converted, so a zone on a derived field would mean nothing. (codex)

### D6. `time_zone: civil` (user)
It marks zone-free civil time, which is Ossie's own meaning of `DateTime`: never converted, skipped by the datasource default, no warning. It can't collide with an IANA name.

### D7. Undeclared is read as stored, with a warning (user, October plan)
- The warning is deduplicated by canonical dataset, field and join path, collected by the execution-level accumulator, and surfaced both as a Python warning and in `SlayerResponse.warnings` (core P6). (codex)
- Undeclared is not refused, because reading as stored is the Ossie civil reading.

### D8. Value context vs stored-predicate context (user; codex)
- The read path takes a context: `value` or `stored predicate`.
- Value context covers query expressions, derived field expressions, and dataset metric and aggregation expressions, including their `FILTER (WHERE …)`. These read converted values, so a derived field means the same as its inline expression (engine P10's derived ≡ structural spirit).
- Stored-predicate context covers field `filter`s, dataset `filters` and RLS. They read stored values, so a dataset's rows and access never depend on the zone (semantics Axiom 3.3, sql P13). This context passes down recursively into derived fields those predicates reference.
- `_anchor(FieldSqlKey)` therefore expands the value and the filter in their own contexts.
- **Rejected:** stored definitions all seeing stored values (the October D5), because a derived field would then disagree with its own expression written inline.
- A bare source column that no field maps is read as stored, with the warning. A dialect entry sees converted values; that is documented.

### D9. The query zone is the final stage's, inherited by unpinned stages (user)
- A stage's effective zone is its own `time_zone`, else the query's.
- The stored stages of every query-backed dataset the query reads inherit it too, so they behave like tables under Q, and refinement works.
- It is resolved once in `_normalize_input` / `_stages_of_dataset` and carried on each prebound stage → `PlannedQuery` → render context.
- **Rejected:** strictly per-stage zones, because refining `time_zone` on a multi-stage saved query would then mostly produce the cross-zone error.

### D10. Stage zone state (user, October plan; codex)
- `StageField` carries its zone state, computed by one generic walker over the dependency closure of the output's timestamp reads, which core P3 keeps total:
  - any undeclared read → undeclared
  - otherwise, any converted read → the stage zone
  - otherwise → civil
- A reader converts a zoned output Z → Q. A pinned stage's fields are stamped onto query-backed fields, as `granularity` is.
- Reconverting a repeated local hour follows D13 (the later occurrence). It is not preserved through a hidden instant, which would undo D1. This is documented and pinned by a test. (codex, partial)

### D11. Cross-zone re-bucketing is an error (user, October plan)
A bucket start in Z converted to Q is not a Q boundary. For example, a January UTC bucket starts at Dec 31 19:00 in New York and mixes rows from two New York years. The check is part of `check_time_dimension_field` and `check_axis_rebucket` and raises `TimeDimensionFieldError`.

### D12. The clock (default)
- No zone: unchanged.
- Under Q: one engine reading per execution, taken as an aware instant (a naive reading is host-local, through `astimezone()`), is held in `bundle.now`. Each stage derives its Q wall-clock "now" from it for relative tokens, the `whole_periods_only` clamp, the spine's implied bound, and `now()` / `current_date()` rendered as literals.
- `plans_read_clock` becomes: any `now()` rendered as a literal, or any `now()` / `current_date()` in a zone-less stage.
- This supersedes the October "BREAKING" change, which DEV-2280's 2.x-is-additive rule forbids. Engine P3 is kept: the clock is read once, into the bundle.

### D13. DST follows the Postgres rule (user, October plan)
- A skipped local time takes the pre-transition offset; a repeated one takes the later occurrence. DuckDB behaves the same.
- The SQLite `slayer_convert_tz` function implements it explicitly, because `zoneinfo`'s `fold=0` would pick the earlier occurrence.
- BigQuery, Snowflake and SQL Server apply their engine's rule; this is documented.

### D14. The pre-filter (user, October plan)
- A row-filter comparison between a converted bare column (or a `date_trunc` lowered to bounds) and a resolved time point also gets `raw >= L − 2d` / `raw < U + 2d`, at the same position. IANA offsets lie within [−12h, +14h], so the gap between any two zones is at most 26h.
- The literal is in the raw column's type: naive for a naive column, and a UTC instant through a dialect hook for an instant column. Where the dialect can't render it, the pre-filter is left out.
- Never on frame bounds. The exact filter implies it (semantics Law 6).

### D15. Dialect primitives (user, October plan; default for Trino)
- Postgres / DuckDB: `(c AT TIME ZONE S) AT TIME ZONE Q` for a naive column; `c AT TIME ZONE Q` for an instant.
- Snowflake: `CONVERT_TIMEZONE(S, Q, c)`; for an instant, `CONVERT_TIMEZONE(Q, c)::TIMESTAMP_NTZ`.
- BigQuery: `DATETIME(TIMESTAMP(c, S), Q)`; for an instant, `DATETIME(c, Q)`.
- Trino: `CAST(with_timezone(c, S) AT TIME ZONE Q AS TIMESTAMP)`; for an instant, `CAST(c AT TIME ZONE Q AS TIMESTAMP)`.
- SQL Server: `AT TIME ZONE` twice plus `CAST(… AS datetime2)`, through a vendored CLDR `windowsZones` table (territory `001`, with its CLDR version recorded). An unmapped zone is a typed error.
- MySQL: `CONVERT_TZ(c, S, Q)`; for an instant, `CONVERT_TZ(DATE_ADD('1970-01-01', INTERVAL UNIX_TIMESTAMP(c) SECOND), '+00:00', Q)`.
- ClickHouse: `toTimeZone(c, Q)`.
- SQLite: `slayer_convert_tz(c, S, Q)`, which parses naive and offset text and returns fixed `YYYY-MM-DD HH:MM:SS.ffffff`. Malformed text gives NULL. Its comparisons go through normalisation, not the midnight raw-text fast path.
- The converted value's SQL type (BigQuery `DATETIME`, ClickHouse `DateTime64(6, Q)`) travels with it into literals, origins, spine bounds and window bounds.
- Tier-2 dialects raise a typed error only when a conversion is needed.
- All quirks live in `dialects/` (sql P2), and the hooks take typed operands (sql P1).

### D16. The MySQL zone probe goes through the database door (user, October plan; codex)
- Each distinct zone is checked with `CONVERT_TZ('2000-01-01 00:00:00', '+00:00', Q)` through the engine's own client (database P1), before the statement that needs it. The result is cached on the client.
- The first statement for a zone costs one extra request; warm statements cost one (database P5, as one-time state).
- It never runs from rendering code. NULL raises the typed error naming the zone and the remedy.

### D17. Zoned casts are converted at the cast (default)
A cast to a zoned timestamp under Q yields Q wall-clock time, which keeps D1's invariant. BigQuery's `TRY_CAST(… AS TIMESTAMPTZ)` keeps its specified `DATETIME` result.

### D18. An AST-level read check (codex)
- Every physical dataset read is built by the one read path and tagged with its context and zone outcome.
- A harness validator, next to `SLAYER_VALIDATE_SCOPES` and enabled in tests, walks the emitted SQL and fails on any temporal dataset column that has no tag.
- Each known bypass gets its own focused test.

### D19. The response and the surfaces (user, October plan)
- `SlayerResponse.time_zone` is the query's zone, or null.
- Surfaces: REST `QueryRequest` and `refine`, MCP `query` (schema from `SlayerQuery`), `create_datasource` / `edit_datasource`, dataset tools through the field schema, CLI `datasources create --time-zone`, inspect, and the help memory.
- `refine_query` merges `time_zone` like the other replaceable scalars: supplied replaces, explicit `null` clears, omitted keeps. (codex)

### D20. Storage (default)
- `DatasetField.time_zone` goes in the `SLAYER` field payload (`_FIELD_ONLY`) and is exported.
- `DatasourceConfig.time_zone` goes in the datasource YAML.
- All new keys are optional and additive. The only version bump is `SlayerModel` 31, for D3's load upgrade.

### D21. Architecture
- New system §3 principle 12 (user, approved wording): "**Zoned reads**: under a query zone, every read of a timestamp from a dataset — a derived field's reads included — yields the query zone's wall clock, converted once at the read; the predicates defining the dataset (its filters, field filters, row-level security) see stored values, and the query's "now" is one clock reading taken in its zone. [enforced: test:tests/test_law_zoned_reads.py]"
- live_schema P1 per D4 (user, approved wording).
- `queries/time-zones` goes under the cross-cutting `queries` group, so `index.yaml` doesn't change, and no new import edges are added. (default)
- Rejected (codex): filing new principles in core, database and engine. The arc42 files stay minimal, and the existing core P6/P8/P9, database P1/P5 and engine P3/P5/P10 already bind this work.

### D22. Ossie conformance (codex)
`ossie_l3` already refuses `time_zone` as not part of an Ossie query. Tests cover a fresh query, a refinement and a query-backed dataset's stored stage. Without a zone, SLayer source zones can't change Ossie SQL.

## Risks / Trade-offs

- [Converted filters lose sargability] → D14 pre-filter. Frame bounds and expressions over several columns get none.
- [DST rule differs on BigQuery / Snowflake / SQL Server] → documented. It affects at most one hour a year of naive data.
- [A repeated hour through a pinned stage reconverts as the later occurrence] → documented, and pinned by a test (D10).
- [A `DateTimeTz` comes out naive under a zone] → the response reports the zone; documented (D1).
- [MySQL server without time-zone tables] → D16 fails closed before execution.
- [Undeclared derived reads warn until annotated] → the warning names both remedies; the datasource default covers ingested fields.
- [Re-typing alters something without a zone] → the goldens in tasks 1.2 and 1.8 fail.

## Migration Plan

Stored documents gain nothing until users set zones. Stored BigQuery `TIMESTAMP` fields upgrade on load (`SlayerModel` 31); a load never writes. Existing ClickHouse and MySQL instant fields are re-typed by their next re-ingest. Rollback is a plain revert. The `SlayerModel` 31 upgrade only re-types fields, and older code treats `DateTimeTz` like `DateTime`.
