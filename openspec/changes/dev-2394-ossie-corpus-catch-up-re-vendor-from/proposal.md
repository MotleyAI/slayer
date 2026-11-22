## Why

The weekly upstream corpus job went red: apache/ossie `main` now lets a dataset-scoped metric evaluate a measure of its dataset's
`source` with `AGG(source.measure)` (core-spec/spec.md, Metric Scoping rules 7–9, commit `1b1ebb6`), and SLayer's native validity
check still refuses it. Four more upstream commits landed since the vendored snapshot (`94cf86bd`); the corpus must catch up and
SLayer must keep agreeing with upstream's validator.

## What Changes

- `scripts/ossie_schema_drift.py --vendor <checkout>` re-vendors the corpus mechanically (byte-identical copies, computed blob shas,
  staged swap, then harvest); the vendored set gains the core-spec text and `compliance/expressions/{valid,invalid}.txt`. The corpus
  is re-vendored from current upstream `main` with it.
- The native validity check mirrors upstream `validate.py`: `AGG(source.measure)` is accepted in dataset-scoped `OSSIE_SQL_2026`
  metrics and every other `AGG` form there is refused with upstream's message; `INTERVAL`, an empty `IN ()` list, an alias and
  argument-less `DISTINCT` are refused in `OSSIE_SQL_2026`. **BREAKING** (2.0 line): a document using those constructs in portable
  `sql` no longer loads.
- A document repeating a YAML mapping key, or holding a malformed scalar, is invalid with a typed reason. **BREAKING** (2.0 line).
- `AGG` is recognised in both sqlglot AST shapes (`exp.Anonymous` before 30.19, `exp.Agg` from 30.19) by one shared iterator, so
  every check works on every allowed sqlglot.
- A dataset-scoped metric calling `AGG(source.…)` loads with a load warning and every query reading it, directly or through model-scoped
  metrics, fails with a typed error: SLayer's sources expose no measure columns (Ossie rule 9). A SLayer-authored save adding one is
  refused. Evaluation on measure-relation sources is DEV-2399 (post-2.0).
- Portable SQL SLayer writes (dataset and model-metric saves, the import upgrade, export) spells `INTERVAL` arithmetic as
  `DATEADD(unit, n, x)`, with the same meaning.
- Upstream's Layer 2 `.ost` query scripts are not vendored here (DEV-2400, post-2.0).

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `models/semantic-model-documents`: duplicate keys and malformed scalars refuse a document; a native-validity requirement mirroring
  upstream's validator; `AGG(source.measure)` metrics and their dependents load unevaluable; SLayer-written portable SQL is valid
  `OSSIE_SQL_2026`; the corpus requirement covers the new vendored set and `--vendor`.
- `queries/model-scoped-metrics`: in a stored dataset metric, `AGG(source.x)` is Ossie's source-measure form, never a relationship
  path.
- `importers/osi-import`: duplicate keys and malformed scalars are among the reasons a document is refused.
- `importers/osi-export`: exported portable `sql` is valid `OSSIE_SQL_2026` (`INTERVAL` exported as `DATEADD`).

## Impact

- Code: `slayer/core/ossie_validity.py`, `slayer/storage/semantic_models.py`, `slayer/storage/model_metric_rules.py`,
  `slayer/storage/base.py`, `slayer/storage/document_mapping.py`, `slayer/osi/importer.py`, `slayer/osi/exporter.py`,
  `slayer/engine/binding.py`, `scripts/ossie_schema_drift.py`.
- Fixtures: `tests/fixtures/ossie_corpus/` re-vendored (provenance, harvested documents, spec text, expression fixtures).
- Architecture: `architecture/python.engine.arc42.md` principle 8 amended (approved wording in design.md); no model change.
- Docs: Ossie pages, the model-scoped-metrics page, `docs/concepts/references.md`, agent help text on `AGG`.
- CI: the weekly `ossie-corpus-weekly.yml` job goes green again.
