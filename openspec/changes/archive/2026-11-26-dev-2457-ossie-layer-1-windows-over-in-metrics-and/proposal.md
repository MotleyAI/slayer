## Why

Ossie's expression language requires window functions: the `OVER` syntax, six ranking functions, five offset functions, window aggregations and a NULL placement rule for a window's `ORDER BY` (`tests/fixtures/ossie_corpus/core-spec/expression_language.md`, "Window Functions" and "Window ORDER BY NULL Placement"). SLayer refuses `OVER` in every Mode B position, so a portable windowed field is refused at save, a windowed metric loads as "cannot evaluate yet", and the three TPC-DS window metrics import as not evaluable. DEV-2432 ruled that a windowed field is a legal column of its dataset and built it for datasource-dialect entries; this change gives the same construct to Mode B (DEV-2457).

## What Changes

- `function(args) OVER ([PARTITION BY …] [ORDER BY … [ASC|DESC] [NULLS FIRST|LAST]] [ROWS|RANGE frame])` is read in every Mode B position: a field's portable `sql` / `OSSIE_SQL_2026` entry, a metric's `expression` (dataset-scoped and model-scoped), and every query position.
- A window with no aggregate-valued argument or key is row-level: the windowed column of one dataset, computed over that dataset's filtered rows before any join, query filter or aggregation. In a definition the dataset is the declaring one; in a query it is the one the window's references are anchored at.
- A window with an aggregate-valued argument or key is a transform over its operand's cells, with SQL's meaning. Its keys are aggregate-valued or query-grain members.
- An order item without `NULLS FIRST|LAST` places NULL as the largest value on every database.
- A raw window becomes legal in a field's `filter`. A raw window in a dataset's `filters` is refused as a filter cycle.
- Portable windowed fields are saved and evaluated. Windowed metrics are saved and evaluated; their "cannot evaluate yet" load warnings and save refusals go.
- The three TPC-DS window metrics import as evaluable and return hand-oracle values.
- `conformance="ossie-l3"` keeps `E_WINDOW_IN_WHERE` and `E_DEFERRED_WINDOWED_METRIC`; windowed fields and dataset-scoped windowed metrics evaluate under it.
- `first(x, t)` / `last(x, t)` keep skipping NULL ranking keys, unlike `FIRST_VALUE` / `LAST_VALUE`; neither is rewritten into the other on import or export.
- The 21 window rows of the Layer 1 compliance gate turn from gap to pass.

Nothing accepted today is refused: the change is additive (2.x).

## Capabilities

### New Capabilities
- `queries/window-functions`: the window grammar in Mode B, row-level and measure-level window semantics, NULL placement, frames, the functions allowed under `OVER`, per-database coverage and typed errors.

### Modified Capabilities
- `models/field-expressions`: a windowed field is one whose runnable definition holds a window, portable or dialect entry; the portable refusal requirement is removed.
- `models/model-filters`: a raw window written in a dataset's `filters` is a filter cycle.
- `models/semantic-model-documents`: a window function is no longer an entity SLayer cannot evaluate.
- `models/save-validation`: authoring a windowed metric is accepted.
- `queries/expression-grammar`: raw `OVER (…)` leaves the forms rejected with a hint.
- `queries/ossie-l3-binding`: the native conformance evaluates a window in a filter; strict is unchanged.
- `importers/osi-import`: the TPC-DS window metrics are stored and evaluable.
- `importers/osi-export`: windowed fields and metrics export as Ossie expressions.

## Impact

- `slayer/core`: `syntax.py` (one grammar, resolved NULL placement), `keys.py` (a window key kind), `models.py` / `query.py` (regex window gates removed), `ossie_conformance.py`.
- `slayer/engine`: binding, the checker (classification, anchor, key and literal typing), compile and plan (row-level windows as dataset windowed columns; measure-level windows as transform layers), `definitions.py`, `field_dependency.py`, `ossie_l3.py`.
- `slayer/sql`: `generator.py` (relation-door windowed columns, window rendering), `render/`, `dialects/` (NULL placement, per-database rewrites and refusals), `definitions.py`, `sql_predicate.py`, `window_detect.py`.
- `slayer/storage`: `semantic_models.py`, `model_metric_rules.py` (window warnings and refusals removed).
- `slayer/osi`: `exporter.py`.
- `architecture/semantics.arc42.md`: one sentence in Axiom 1 and clause 11.7, both with approved wording.
- Tests: a new suite, the Layer 1 gate manifest and suite, and the existing tests that pin the lifted refusals.
- Docs: `docs/concepts/`, `docs/osi/`, `docs/interfaces/`, `docs/database-support.md`, the agent help memories.
