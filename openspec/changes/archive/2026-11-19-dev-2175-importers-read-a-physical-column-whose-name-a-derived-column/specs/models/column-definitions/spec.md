## REMOVED Requirements

### Requirement: Column.sql is one expression
**Reason**: A field no longer has its own `sql`; its native SQL is the `dialects` entry for its datasource's dialect, and the one-expression rule now governs that entry (`models/field-expressions` › Portable sql has Ossie's meaning; a dialect entry is the database's own SQL).
**Migration**: A field's `sql` converts to its `expression` (`models/field-expressions` › A field's sql converts to its expression); a statement in the converted entry is refused exactly as before.
