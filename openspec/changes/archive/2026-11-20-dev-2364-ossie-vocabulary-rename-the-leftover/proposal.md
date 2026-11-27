## Why

DEV-2318 renamed SLayer's identifiers to the Ossie vocabulary (dataset, field, metric, relationship, expression) and DEV-2347 respelled the prose, but some identifiers still use the old words: the 37 `stragglers` of the identifier guard in `slayer/`, and helper, fixture and constant names, parameters and test-data names in `tests/`. DEV-2347 routed both here.

## What Changes

- Rename every straggler's SLayer-concept uses in `slayer/`; a name that keeps carve-out uses in its file moves to `carve_outs` with a reason. The guard's `stragglers` list, its `Straggler` schema, its count ceiling and `x-vocabulary-stragglers` in `architecture/index.yaml` are removed.
- **BREAKING** (unspecified output): the Cube conversion report category `segment_as_column` becomes `segment_as_field` (`CubeIssueCategory.SEGMENT_AS_FIELD`), noted in `docs/upgrading/2.0.md`.
- One-time sweep of `tests/` (except `tests/perf/compare/`): helper, helper-class and fixture names, non-test methods, module constants, class fields, helper and fixture parameters (`joins=` → `relationships=`) and import aliases that name a SLayer concept with an old word are renamed. `tests/_model_access_fixtures.py` → `tests/_dataset_access_fixtures.py` and `tests/_cross_model_chain.py` → `tests/_cross_dataset_chain.py`, without stubs, with their vocabulary-corpus files and index entries following.
- Test-data dataset and field name literals with an old word (`no_such_model` → `no_such_dataset`) are respelled.
- Test function and class names, all locals, `test_*.py` file names and fixture data paths keep their words. No guard is added for `tests/`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. No specified behaviour changes; the Cube report category is not specified (`skip_specs: true`).

## Impact

- `slayer/`: private names and locals across `facade/`, `live_schema/`, `sql/`, `engine/`, `flight/`, `osi/`, `dbt/`; `cube/report.py` and `cube/converter.py` (report category).
- `tests/`: helpers and fixtures across the suite, two helper modules renamed, the guard and its allowlist, `tests/fixtures/vocabulary_corpus/`, goldens whose output moves only with a respelled test-data name.
- `architecture/index.yaml`: `x-vocabulary-stragglers` removed. No arc42 or LikeC4 change.
- Users of `slayer import-cube` reading the report JSON see `segment_as_field`.
