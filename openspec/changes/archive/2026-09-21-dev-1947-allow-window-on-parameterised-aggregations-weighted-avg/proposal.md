## Why

DEV-1947 was filed when the checker's `sum`/`avg` allowlist refused `window=` on every
parameterised aggregation, so a windowed aggregation could not carry an attached
(aggregate-valued) parameter. DEV-1915 has since deleted that allowlist structurally and
lifts every reference-bearing parameter per interval row, and the strict xfail pinning
the windowed-parameter scenario was dropped on main when it XPASSed. What remains is a
close-out: the corpus still describes the scenario as deferred target behaviour, the
custom-aggregation form the issue asks for is unpinned, and the issue's golden is missing.

## What Changes

- `queries/partitioned-aggregates` › "Attached parameters on row-level sources": the
  scenario "Windowed aggregation with an attached parameter" loses its stale "deferred to
  DEV-1915, pinned by a strict xfail" note and states that the same value prunes or sorts
  in filter and ORDER BY positions; the requirement gains one normative sentence — an
  attached parameter composes with `window=` on the enclosing non-ranked aggregation,
  built-in or custom, read per interval row — and one scenario for the custom form
  (`customers.spend:wsum(window='1y', weight=sum(amount, partition_by=customers.regions.name))`).
- Executed pins on SQLite and DuckDB in every `to_many_handling` mode for the custom form,
  a filter / ORDER BY pin for the built-in form, and a plan-structure pin that the
  trailing-window kernel's picked parameter is the nested producer's row-attach column,
  never the raw aggregate.
- Goldens `param/windowed` and `param/windowed_custom` in the DEV-1859 attached-parameter
  golden module, under its existing "every `param/` case generates SQL" guard.
- No change under `slayer/`: the behaviour is already on main.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/partitioned-aggregates`: "Attached parameters on row-level sources" — the
  windowed-parameter scenario is current behaviour, composes in filter / ORDER BY
  positions, and covers custom aggregations.

## Impact

- Tests only: `tests/_dev1919_fixtures.py` (constant + raw-row oracle),
  `tests/test_dev1919_home_rooted_attached_inputs.py` (three pins in
  `TestWindowedOuter`, a `wsum_engine` fixture), `tests/test_dev1859_golden_sql.py` +
  `tests/golden/dev1859_sql_baseline.json` (two cases, fourteen new baseline rows).
- No source, docs, arc42, LikeC4 or `index.yaml` change; `guards.baseline` and
  `legacy_arrows` unchanged.
