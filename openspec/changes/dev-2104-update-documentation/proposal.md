## Why

The README's example picture still shows the retired colon syntax (`order_total:sum`, `time_dimensions`), and the comparison matrix behind the motley.ai post has three stale spots: C13's title suggests SLayer isn't embeddable, C16 says SLayer refuses cross-database queries although one Trino datasource federates several databases, and the just-merged unrelated-dataset broadcast has no row.

## What Changes

- README example picture re-rendered from a committed HTML source with current syntax.
- C13 renamed "Embeddable UI components", with intent, SLayer, Cube and Malloy texts and the comment reworded around UI components; verdicts unchanged.
- C16 renamed "Cross-database queries"; all four engines `yes` via Trino, with evidence-qualified caveats and a new comment.
- New row Q26 "Metrics without a join path", backed by probes on all four engines; an `events` source is added to the Malloy, Cube and MetricFlow models.
- `matrix.json` regenerated.

## Capabilities

### New Capabilities

None: docs and examples only.

### Modified Capabilities

None.

## Impact

- `README.md`, `docs/images/sql-vs-slayer.png`, `examples/readme/sql_vs_slayer.html`.
- `examples/comparisons/`: `matrix.yaml`, `matrix.json`, `probes.yaml`, `malloy/model.malloy`, `cube/model/`, the MetricFlow dbt project, and `README.md` (only if a probe records a known bug).
- No production code, API or behaviour change.
- Outside the repo: the motley.ai post says "54 capabilities"; with Q26 the matrix has 55 rows, so the post's title and count need an edit in motley-website (the slug stays).
