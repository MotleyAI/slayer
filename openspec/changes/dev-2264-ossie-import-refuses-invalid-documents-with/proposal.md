## Why

The Ossie importer imports a document that breaks the Ossie core spec anyway, skipping, ignoring or reinterpreting the offending parts with a warning. The owner's ruling (DEV-2215 review, 2026-11-11) is that an invalid Ossie document is refused, with every reason, instead of being partly imported; DEV-2215 covered only repeated names within a dataset.

## What Changes

- **BREAKING** A document that breaks a document-checkable Ossie rule (schema, duplicate keys, extension data, name length and uniqueness, key and relationship references, relationship arity, the pinned Ossie SQL language, metric and field scoping) is refused with one `OsiConversionError` listing every violation in every file, each with its location and rule. Nothing is saved.
- **BREAKING** Earlier document shapes (`semantic_model` wrappers, flat documents declaring an earlier known version, `custom_extensions`) are upgraded as Ossie's migration section prescribes and then validated like any other document; lenient resolution of legacy documents is removed. An unknown version, or a flat document without one, is refused.
- **BREAKING** A field expression column qualified by another dataset of the document is refused instead of resolving through relationships.
- **BREAKING** References resolve as Ossie identifiers: key and relationship names match declared names case-insensitively and store the declared spelling; a quoted reference is compared with the declared name's normalized form (`"AMOUNT"` reaches `Amount`, `"Amount"` does not).
- What stays a per-entity report is unchanged: SLayer limits (names SLayer cannot store, vendor dialects it cannot read, computed join keys, no join path, unsupported aggregates, `SLAYER` payload problems) and environment limits (an unreachable source, a column the live source lacks, an ambiguous source column).
- The pinned Ossie schema ships as package data and `jsonschema` becomes a core dependency.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `importers/osi-import`: invalid documents are refused with every violation; earlier shapes are upgraded then validated; lenient resolution removed; Ossie identifier matching; the skip-and-warn requirements for document violations replaced.
- `importers/meaning-preservation`: an OSI field expression no longer resolves a reference to another dataset's field; the import is refused.

## Impact

- Code: `slayer/osi/parser.py`, `slayer/osi/models.py`, `slayer/osi/converter.py`, a new `slayer/osi/validation.py`, `slayer/cli.py` (`import-osi`), the packaged schema `slayer/osi/ossie-schema.json`.
- Dependencies: `jsonschema` moves from the dev group to the core dependencies.
- Tests and fixtures: `tests/test_osi_*.py`, `tests/test_dev2171_osi_import.py`, `tests/test_dev2175_osi*.py`, `tests/test_cli_import_osi.py`, `tests/fixtures/osi/`, `scripts/ossie_schema_drift.py`.
- Docs: `docs/osi/osi_import.md`, `docs/upgrading/2.0.md`, `docs/reference/cli.md`.
