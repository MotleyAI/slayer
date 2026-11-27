## Why

DEV-2461 closes the Ossie importer's Layer 1 fidelity gaps. Two are real defects:

- A double-quoted name in Ossie's upper-case normalized form (`"AMOUNT"` for a field `amount`) fails to resolve, although Ossie's identifier table says it matches.
- On SQLite, a document field without a `datatype` is typed String even over an INTEGER or REAL column, so `SUM` of it is refused.

The other items need tests and docs only. `CASE` and scalar functions around aggregates already evaluate as written. `FIRST_VALUE` / `LAST_VALUE` keep their window form (DEV-2457). `ARRAY_AGG(DISTINCT x)` keeps `collect`'s meaning for now, and DEV-2499 replaces it with SQL's meaning.

## What Changes

- A quoted name segment matches the declared name spelled exactly. Otherwise it matches the declared name whose Ossie normalized form equals it: a name readable unquoted is upper-cased, any other is kept as written. Several such names raise the typed ambiguity error. This holds in every Mode-B namespace, natively and under strict `ossie-l3` binding. Unquoted names, physical source-column reads and stored text are unchanged.
- On SQLite, an untyped field gets the type ingestion would give it, from one classifier shared with ingestion:
  - a plain column read gets its declared type, refined by the bounded value sample;
  - an expression field gets the census over a bounded sample of its values.
- Tests import, through `slayer import-osi`, `CASE` and scalar functions around aggregates, `FIRST_VALUE` / `LAST_VALUE` windows, `ARRAY_AGG(DISTINCT x)` and quoted names. Each value is checked against a raw-SQL oracle on DuckDB and SQLite.
- The Ossie corpus test pins the set of entities the import reports as not evaluable.
- Docs: the quoted-name rule (`docs/concepts/references.md`), and in `docs/osi/osi_import.md` that `CASE` and scalars around aggregates evaluate as written and that `ARRAY_AGG(DISTINCT x)` reads as `collect(x)`.
- The quoted-name ruling is recorded on DEV-2452 (the stored-format issue).

No input that resolves today changes meaning: a quoted name that resolved before still resolves to the same declaration.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/identifier-case`: a quoted name falls back to Ossie's normalized form when nothing matches exactly.
- `models/semantic-model-documents`:
  - SQLite resolves an untyped field's type as ingestion does;
  - `ARRAY_AGG(DISTINCT x)`'s NULL handling is pinned;
  - `CASE` and scalar functions around aggregates evaluate as written;
  - the corpus import reports only known unevaluable entities.

## Impact

- Code:
  - `slayer/core/names.py` (the one name matcher), plus any resolution path that compares quoted names outside it (strict `ossie-l3` included);
  - `slayer/engine/unknown_types.py` and the SQLite typing in `slayer/database/sqlite_introspect.py` / `slayer/sql/dialects/sqlite.py`.
- Tests:
  - new `tests/test_dev2461_importer_fidelity.py`;
  - `tests/test_dev2243_corpus.py` gains the not-evaluable manifest;
  - aligned: `tests/test_dev2215_identifier_case.py`, `tests/test_dev2217_errors.py`, `tests/test_dev2021_stored_conversion.py`, `tests/test_stored_upgrade_filter_literals.py`.
- Docs: `docs/concepts/references.md`, `docs/osi/osi_import.md`.
- No storage format, API or dependency change.
