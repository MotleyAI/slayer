## Why

A time bucket `date_trunc(unit, x)` is typed by `x`, but each dialect's bucket SQL returns whatever its engine returns. Postgres gives `timestamptz` for a DATE, DuckDB a TIMESTAMP, MySQL a DATE for a DATETIME month, and SQLite date-only text for a TIMESTAMP day bucket. So `date_trunc('day', ts) = ts` returns no rows on SQLite. DATE and TIMESTAMP values are also never aligned when they meet (SQLite `d = ts` returns no rows; BigQuery joins a DATETIME spine bucket to a DATE axis bucket).

## What Changes

- A bucket has one logical type: DATE when its source is DATE and its unit (a custom granularity's base) is day or coarser, TIMESTAMP otherwise. `date_trunc('hour', <DATE>)` becomes TIMESTAMP.
- Every Tier-1 dialect emits exactly that type for every bucket: time dimensions, row expressions, the time spine, `time_shift`, consecutive periods, trailing windows and custom granularities.
- A DATE meeting a TIMESTAMP reads as its midnight instant in comparisons, `in`, null-safe equality, value-returning conditionals, `nullif`'s comparison and grain join-backs (the spine join included). A literal is typed to the column instead, so bounds stay sargable.
- On SQLite, comparisons between two temporal expressions are independent of how a TIMESTAMP is spelled in storage.
- The pg facade advertises a bucket column with the bucket's type.
- `queries/time-points` drops the restriction that a bucket is temporal only as a comparison's direct operand.
- **BREAKING** (output types): DuckDB and Postgres DATE buckets at day or coarser return dates, not datetimes. SQLite TIMESTAMP buckets at day or coarser return `YYYY-MM-DD HH:MM:SS`. Sub-day buckets of a DATE are TIMESTAMPs. The emitted SQL for buckets changes on every Tier-1 dialect.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/date-functions`: a bucket's logical type, and that type on every dialect; `date_trunc` in the temporal-operand list and in "Date results carry their types".
- `queries/time-points`: the temporal-operand definition loses the direct-operand restriction on buckets.
- `queries/mixed-type-coercion`: a DATE meeting a TIMESTAMP reads as its midnight instant, independent of SQLite storage spelling.

## Impact

- `slayer/core/keys.py` (bucket type rule, operand-coercion positions), `slayer/core/granularity.py`.
- `slayer/sql/dialects/*` (`build_bucket` takes the operand type; new `demote_to_date`; `bucket_comparand` removed; SQLite temporal comparison form), `slayer/sql/render/{value_expr,row_expr,joins}.py`, `slayer/sql/generator.py` (bucket call sites, typed join-back pairs).
- `slayer/facade/translator.py` (projection types).
- `architecture/python.sql.arc42.md`: new principle 14.
- Goldens under `tests/golden/` and `tests/dialects/`; docs `docs/concepts/time.md` and the date-function / mixed-type references; notebooks with SQLite outputs.
