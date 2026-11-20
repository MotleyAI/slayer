## ADDED Requirements

### Requirement: create_datasource auto-ingest runs the idempotent ingest
When the `create_datasource` tool auto-ingests, it SHALL ingest the datasource exactly as `ingest_datasource_models` would with the same scope: it SHALL create missing models and additively merge existing ones, never overwriting a stored model's user metadata. Its result SHALL be the datasource line ("Datasource '<name>' created" or "replaced", with the connection test message) followed by the same rendering `ingest_datasource_models` returns, plus the explore hint when models were created. A database failure during the ingest SHALL be reported as "Auto-ingestion failed: <friendly database error>" after the datasource line.

#### Scenario: Fresh create
- **WHEN** `create_datasource` creates a SQLite datasource over a database with tables `a` and `b`
- **THEN** models `a` and `b` are stored, and the result names both under the new-models section

#### Scenario: Re-create keeps model edits
- **WHEN** model `a` of datasource `app` is stored with a user-written description, and `create_datasource` is called again for `app`
- **THEN** the result says the datasource was replaced, and model `a` keeps its user-written description

#### Scenario: Database failure during ingest
- **WHEN** the ingest fails with a database error after the datasource is saved
- **THEN** the result contains "Auto-ingestion failed:" with the friendly database error, and the datasource is stored
