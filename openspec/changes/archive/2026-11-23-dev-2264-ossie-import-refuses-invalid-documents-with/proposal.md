## Why

The owner's ruling (DEV-2215 review, 2026-11-11) is that an invalid Ossie document is refused with every reason instead of being partly imported. DEV-2243 delivered native validity that agrees with upstream `validate.py` on everything except duplicate mapping keys, which SLayer silently overwrites (last value wins). And an expression SLayer cannot parse is stored silently, then fails at query time with a raw `ValueError`.

## What Changes

- **BREAKING** A document whose YAML or JSON repeats a mapping key, at any depth, is refused at import, unloadable in storage, and refused on save or edit, naming every repeated key and its line, as upstream's loader refuses it. Repeated YAML merge directives stay allowed and merge overrides keep working. A scalar the YAML loader cannot build (`2024-02-30`, `!!int abc`) is a reason with its line, never a crash.
- Every place that parses document text (import, storage load, save, edit) reads it through one document reader.
- A field or metric whose runnable form SLayer's reader cannot parse loads not evaluable, with a typed reason shown at import and on `inspect`; a query reading it fails with the typed error instead of a raw `ValueError`.
- Validity stays exactly upstream `validate.py`'s errors plus duplicate keys. Anything else Ossie rejects is stored and evaluated where SLayer can evaluate it (DEV-2243 decision 35's superset principle), otherwise loaded not evaluable.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `importers/osi-import`: invalid documents are also refused for repeated mapping keys and unbuildable scalars, every repeated key listed.
- `models/semantic-model-documents`: a stored document whose text repeats a mapping key is unloadable; an expression SLayer cannot parse loads not evaluable.

## Impact

- Code: `slayer/storage/semantic_models.py` (`parse_document_text`, decomposition load warnings), `slayer/osi/importer.py` (reads through the shared reader), `slayer/storage/base.py` (`_document_name`), `slayer/core/document_text.py` (edit path).
- Tests: the old-plan `tests/test_ossie_validity_*.py` and `tests/_ossie_validity_fixtures.py` are replaced by gap tests; existing tests pinning last-wins or the raw `ValueError` are aligned with approval.
- Docs: `docs/upgrading/2.0.md`, the Ossie import page.
