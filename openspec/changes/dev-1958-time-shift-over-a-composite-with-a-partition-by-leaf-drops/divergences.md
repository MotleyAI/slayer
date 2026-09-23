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

(filled in at spec-implement: one line per movement class, with the modules and keys.)
