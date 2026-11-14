## MODIFIED Requirements

### Requirement: Explain plan rows capped without a limit

When `query` runs with `explain=True` and no `limit`, the returned plan rows SHALL be subject to the same 20-row cap and notice. `dry_run` output SHALL never carry a truncation notice.

#### Scenario: Large explain plan capped

- WHEN `explain=True` without `limit` yields a plan of more than 20 rows
- THEN 20 plan rows are returned with a truncation notice

#### Scenario: Dry run unaffected

- WHEN `dry_run=True`
- THEN the response contains the SQL and never a truncation notice
