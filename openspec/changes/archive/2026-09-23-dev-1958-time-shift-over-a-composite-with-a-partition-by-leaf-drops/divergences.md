# Golden divergences — DEV-1958

## Blessing procedure

`tests/golden/dev1958_sql_baseline.json` was recorded before the fix: it pins the
pre-fix SQL (and the pre-fix raises of `leaf/ranked_composite` and
`leaf/carried_degenerate`). The fix moves every `time_shift` key; re-bless per the
harness protocol (`tests/_golden_harness.py`):

1. Run the golden modules; collect every moved key.
2. Classify each movement below (class a: values change — only the fixed cases;
   class b: SQL shape only, values unchanged), confirmed by the executed suites
   (`tests/test_dev1958_*`, the existing time_shift / change executed tests).
3. List each moved key in its module's `ALLOWED_DELTAS` with its class, run
   `SLAYER_UPDATE_GOLDEN=1 poetry run pytest <module>`, then empty `ALLOWED_DELTAS`.
4. Every re-blessed shape still passes `assert_scope_closed` and
   `assert_dependency_ordered_ctes`.

Known gaps kept out of this change (strict xfails in `test_dev1958_golden_sql.py`):
`stage/share` on postgres / sqlite / duckdb (nested `WITH`, DEV-1878) and on
bigquery (scope leak, DEV-1961). Their golden keys still move with the fix.

## Movements

- **Class a — fixed values** (`dev1958`, 5 dialects each): `leaf/share`, `leaf/change`,
  `leaf/change_pct`, `leaf/trivial_composite`, `leaf/reaggregation`,
  `leaf/windowed_composite`, `frame/bare_partitioned`, `frame/bare_ranked`,
  `frame/bare_windowed`, `population/association_filter`, `population/tier_partition`,
  `stage/share`; `leaf/ranked_composite` and `leaf/carried_degenerate` flip from a recorded
  raise to SQL. Values pinned by `tests/test_dev1958_*`.
- **Class b — shifted relation is a producer read by a lookup join-back** (SQL shape only;
  values unchanged, verified by the executed time_shift / change suites): `dev1958`
  `leaf/non_time_partition`, `frame/mixed_conjunction`, `frame/non_time_partition`,
  `join/two_offsets`, `join/unaligned_day`, `population/associate_dimension`; every moved
  key of `dev1747` (5), `dev1750` (45), `dev1800` (80), `dev1837` (15), `dev1846` (30),
  `dev1868` (5).
- **Class b — producer rendered as AST instead of re-parsed text** (cosmetic: T-SQL
  `DATETRUNC(month, …)` casing, BigQuery qualifier quoting): `dev1739`
  `local/time_dim::tsql`, `dev1745` `windowed/src_scope::tsql`, `dev1859`
  `mixed/cross_model_constituent`, `mixed/outer_partition`, `param/broadcast`,
  `param/ranked_transform` (bigquery).
