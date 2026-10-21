## Why

A saved query-backed model's declared joins disappear when its final stage becomes a consuming relation. A customer-segment filter consequently returns all-customer revenue (150 instead of 120), because the missing relationship is treated as a product hop. The older outbound traversal gap is tracked as DEV-1918; this change fixes its wrong-result consequence reported in GitHub issue #507.

## What Changes

- Retain the saved relation's own joins through query-backed placeholder and relation materialization.
- Bind saved relations as model scopes, keeping ordinary query-stage outputs flat.
- Add executable SQLite/DuckDB tests for filtered totals, traversal and aggregate attribution.
- Preserve existing demand splicing, private stage scopes, cycle errors and extension behavior.

## Capabilities

### Modified Capabilities

- `queries/query-backed-inline`: consuming a saved relation retains its own declared relationships.

## Impact

A focused engine/IR fix with regression tests and a clarification in the models guide. No new dependency, persisted schema, public request field or normative architecture change.
