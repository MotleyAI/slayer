## MODIFIED Requirements

### Requirement: Demo setup heals stored models without discarding edits
Every demo setup SHALL ingest the demo models through idempotent ingestion — creating missing models
and additively merging existing ones — and then apply the curated demo enrichment, so stored demo
models gain any joins, keys and curated metadata they lack while every user edit is kept.

#### Scenario: Join-less demo models regain their joins
- **WHEN** stored demo models have no joins (ingested by an affected release) and the demo is set up
  again
- **THEN** the models carry their FK joins and the README query
  `{"source_model": "orders", "measures": ["sum(order_total)", "change_pct(sum(order_total))"], "dimensions": ["stores.name", "date_trunc('month', ordered_at)"]}`
  returns rows

#### Scenario: A user edit survives setup
- **WHEN** a user changed a demo column's description and the demo is set up again, with or without
  a rebuild
- **THEN** the changed description is kept
