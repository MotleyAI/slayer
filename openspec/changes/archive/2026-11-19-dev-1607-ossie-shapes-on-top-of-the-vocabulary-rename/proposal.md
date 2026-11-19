## Why

DEV-2318 gave every SLayer name its Ossie word but deliberately kept the old shapes. The result is a query format that uses Ossie's words without Ossie's forms: metric entries `{expression, name}`, field references `{name, dataset}`, relationships `join_pairs`, a list-only `filter`, uppercase SLayer type names. Unknown keys are silently ignored too, so an Ossie dataset can load as an empty one. 2.0's goal is that a valid Ossie query, and the Ossie dataset shapes covered here, are valid SLayer input as written, so these shapes change before 2.0 ships.

## What Changes

- **BREAKING** Query metric entries are a string or `{metric, alias}` (`QueryMetric`). Dimension entries are a string or `{field, alias}` (`QueryDimension`), which absorbs `FieldRef` and `ComputedDimension` as query entries.
- **BREAKING** Time dimensions name their result `alias`, order items take `field` as text, and query extensions name their target `dataset`.
- **BREAKING** Relationships carry Ossie's parallel `from_columns` / `to_columns`, of equal, non-zero length, with pair order kept. The same holds for relationship references on MCP and drift surfaces.
- A query's `filter` is a string or a list of conjuncts. It is dumped as written, and a refinement appends.
- **BREAKING** A fresh dataset with an unknown key is refused, nested parts included. A stored dataset drops the key with one structured warning.
- **BREAKING** Datatype values are Ossie's names (`String`, `Integer`, `Decimal`, `Float`, `Boolean`, `Date`, `Time`, `DateTime`, `DateTimeTz`, `Opaque`) plus SLayer's `Array`, stored verbatim. The engine's type kinds and behaviour are unchanged.
- Every 1.x spelling these shapes replace converts, with one deprecation per distinct form per document in fresh input, and silently in stored documents. Spellings that existed only on `main` before 2.0 are replaced outright.
- No new stored version numbers: the 2.0 version steps write the final shapes. The Ossie `SLAYER` payload's version 2 carries the final shapes, and version 1 is still read.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `compatibility/ossie-vocabulary` (added by DEV-2318, archived before this change): datasets and queries take Ossie's shapes, so the kept `join_pairs`, the `{expression, name}` metric entry and the `dataset`-keyed field reference go. New requirements cover query entries, relationship columns, `filter` as a string, unknown keys and datatype values, and the fresh-conversion, Python-name, stored-migration and export-payload requirements are updated.

## Impact

- **Core models:** `slayer/core/query.py` (entry classes, `TimeDimension`, `OrderItem`, `DatasetExtension`, `filter`), `slayer/core/models.py` (`DatasetRelationship`, unknown keys, the `DatasetMetric` base), `slayer/core/enums.py` (datatype values and engine kinds), `slayer/core/vocabulary.py` (conversions), `slayer/core/relationship_edges.py`.
- **Storage:** the 2.0 version steps in `slayer/storage/ossie_vocabulary_migration.py` and the stored-type helpers.
- **Surfaces:** the REST API, the MCP tools and their schemas, the CLI, the Python client, `inspect` and search rendering, drift and ingestion.
- **Importers:** Ossie import and export (`slayer/osi/`), dbt and Cube converters.
- **Docs:** `docs/concepts/`, `docs/reference/mcp.md`, `docs/upgrading/2.0.md`, examples.
- **Harness:** `architecture/semantics.arc42.md` §4 P3 (`join_pairs` → `from_columns` / `to_columns`, approved). Spec-corpus literals are respelled.
- **Stores written by `main` between DEV-2318 and this change** must be recreated.
