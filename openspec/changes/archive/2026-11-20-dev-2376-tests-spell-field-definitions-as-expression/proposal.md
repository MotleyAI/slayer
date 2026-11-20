## Why

`expression` is a field's only definition key; the deprecated field `sql` converts on load with a warning until 3.0.
The test suite still builds ~3,100 fields (plus 319 dict-literal fields) with `sql`, so it emits thousands of
deprecation warnings and pins the deprecated spelling instead of the current one. This carries the suite-respelling
tasks deferred from the field-expression change (`2026-11-19-dev-2175-…`, tasks 6.1 and 6.2).

## What Changes

- A static guard test fails on any field-level `sql` spelling in `tests/` outside an allowlist of deprecated-spelling
  tests (each entry: test function, expected count, reason).
- Value pass: every non-identifier field `sql` in tests becomes its `expression` form, decided by comparing what
  production resolves before and after: the portable string where every field's resolved SQL is unchanged on every
  dialect the site reaches, else one dialect entry per dialect; sibling reads become `<dataset>.<field>` paths. Test
  reads of a field's `.sql` are rewritten by intent. Tests whose expected value would change wait for the owner.
- Keyword pass: `sql=` and field dict keys respelled to `expression` with the deterministic refactor tooling.
- Deprecated-spelling tests (those whose assertions depend on the deprecated input) keep `sql`.
- No production change except one: search text renders a field's `expression` (it rendered only the deprecated `sql`).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None — test-only change (`skip_specs: true`).

## Impact

- `tests/**/*.py`: field definitions, field dict literals, `.sql` reads on fields, test helpers forwarding `sql` into a
  field.
- New `tests/test_field_expression_spelling_guard.py`.
- `.basedpyright/baseline.json` kept aligned by the refactor tooling.
- YAML fixtures unchanged (Cube, Ossie and frozen upgrade corpora).
