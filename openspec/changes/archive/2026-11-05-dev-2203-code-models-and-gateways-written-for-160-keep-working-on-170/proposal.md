## Why

1.7.0 must not silently break what users wrote for 1.6.0 (DEV-2203). On main, verified by reproduction: a 1.6.0 `SqlClient` loses every type probe; re-ingest re-types a stored TEXT array column to a list and its stored measures and saved queries stop binding unreported; a model aggregation `Sum` now hijacks the built-in `sum`; type verdicts refined since 1.6.0 (interval, YEAR, OID, BIT, `Bool`, MONEY, JSON, XML, GEOMETRY) make `validate-models --force-clean` delete 1.6.0-typed columns that still exist; the deprecated category probes no longer answer what 1.6.0 answered; and MONEY / SMALLMONEY are treated as floats although they are exact.

## What Changes

- A client that serves only the 1.6.0 `get_column_types` is adapted wherever SLayer takes a client (engine client factory, OSI `collect_metadata`, `validate_datasource(client=...)`): its categories read as DOUBLE / TEXT / TIMESTAMP / BOOLEAN and each probe warns until 2.0. `DelegatingSqlClient.get_column_types` returns, deprecated. The conformance suite keeps checking the current contract.
- Re-ingest reports every stored definition a list re-type breaks: the column's dependants (the drift cascade's reference walk) are planned before and after the re-type, before it is saved; each that planned before and fails after is listed in `IdempotentIngestResult.broken_dependents` and warned. The re-type still happens.
- A call naming a built-in aggregation resolves only to a model definition of exactly that built-in's name.
- **BREAKING (1.7.0-only surface)** A type mismatch on a live column that still exists is never a delete: explicit validation and query-time attribution report one `retype` finding kind (lists and scalars), which replaces the unreleased `list_retype` finding. Missing columns, tables and join keys still delete and cascade.
- The deprecated category probes answer exactly 1.6.0's category for every database type; each dialect declares its 1.6.0 classification.
- MONEY and SMALLMONEY are exact numerics.

## Capabilities

### New Capabilities

### Modified Capabilities
- `sql/client-contract`: a 1.6.0 client's category probe keeps working and warns; the deprecated category probes answer 1.6.0's categories.
- `compatibility/removal-tripwire`: the covered set gains the 1.6.0 client probe.
- `models/list-columns`: a list re-type reports the dependants it breaks.
- `models/aggregation-names`: built-in aggregation names resolve exactly.
- `models/schema-drift`: a type mismatch on an existing column is a re-type finding.
- `aggregations/native-type-preservation`: MONEY and SMALLMONEY are exact numerics.

## Impact

- Code: `slayer/sql/client.py` (`adapt_client`, `type_categories`), `slayer/sql/dialects/` (1.6.0 category declarations, MONEY exactness in `reflection.py`), `slayer/engine/query_engine.py` (`client_for`), `slayer/osi/converter.py`, `slayer/engine/schema_drift.py` (`RetypeFinding`, live type names, no type-mismatch deletes, `IdempotentIngestResult.broken_dependents`), `slayer/engine/ingestion.py` (dependant check), `slayer/storage/` (a model-overlay view), `slayer/core/models.py` (`aggregation_definition`), `slayer/core/warnings.py`, `slayer/cli.py`, `slayer/mcp/server.py`.
- Public API: `ListRetypeFinding` / `list_retype` (new in 1.7.0, unreleased) become `RetypeFinding` / `retype`.
- Docs: `docs/configuration/sql-clients.md`, `docs/concepts/schema-drift.md`, `docs/concepts/models.md` (aggregation names, if it states the case rule), release notes.
