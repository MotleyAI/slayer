## Context

See proposal.md › Why. DEV-2243 put Ossie validity in `ossie_validity_errors` (`slayer/core/semantic_model.py`, rules in `slayer/core/ossie_validity.py`), mirroring upstream `validate.py`; the vendored corpus (`tests/fixtures/ossie_corpus/`, apache/ossie `94cf86b`) is the oracle. Probes on this tree: SLayer agrees with upstream on every rule except duplicate mapping keys. Document text is parsed in several places — `parse_document_text` (storage), `yaml.safe_load` in `slayer/osi/importer.py` (`_file`, `_flat`), `_document_name` in `slayer/storage/base.py`, and the edit path in `slayer/core/document_text.py` (`json.loads`, ruamel) — so a text-level rule has no single home, and the importer raises an unhandled `ValueError` on an unbuildable scalar. A field with a subquery stores silently and fails at query time with `ValueError: Invalid Mode-B expression`.

**Symptom → structural cause → fix.** Symptoms: duplicate keys silently overwritten; unbuildable scalars crash the import; unparseable expressions fail late and untyped. Cause: no single document reader, and load decides evaluability without asking SLayer's own expression reader. Fix: one shared reader for every document-text parse site; at load, an expression whose runnable form SLayer's reader rejects gets a load warning.

**Principles that apply.** System P11 (fail at save time): text-level faults refuse at import/save, and evaluability is decided at load, not at query time. System P6 (typed errors): reasons are carried by `OssieDocumentInvalidError` / import refusals and `UnevaluableEntityError`. `python.importers` P1 (importers preserve meaning): nothing is skipped or reinterpreted. No arc42 or LikeC4 edit is needed; the work stays inside existing nodes and the existing specs.

## Goals / Non-Goals

**Goals:**
- One document reader, used wherever document text becomes data, that rejects repeated keys and unbuildable scalars as upstream's loader does.
- Load-time detection of expressions SLayer cannot parse, through the same reader the query path uses.

**Non-Goals:**
- Refusing what upstream `validate.py` accepts: names over 128 characters, constructs outside the pinned Ossie SQL subset, field expressions qualified by another dataset, a `FILTER` on another dataset, model-scoped reference violations upstream only warns on. They are stored, and evaluate where SLayer can evaluate them, else load not evaluable (decisions 2, 3).
- ANSI identifier syntax for names.
- The dbt and Cube importers.

## Decisions

1. **Re-plan as the gap DEV-2243 leaves**, built on its validator and native importer; the earlier plan (a separate `slayer/osi/validation.py` over the retired converter) is abandoned. (user, 2026-11-21)
2. **Model-scoped reference violations upstream only warns on stay stored and reported as not evaluable.** Options: A, refuse in Ossie forms (spec MUST, but refuses 2 corpus documents upstream accepts); B, keep upstream's tolerance. Took B. (user) Its leak-closing half (core metric→metric references stop evaluating) is withdrawn by decision 3.
3. **Superset everywhere: SLayer refuses exactly upstream `validate.py`'s errors plus repeated keys; whatever else Ossie rejects is stored, evaluated where SLayer can evaluate it, else loaded not evaluable with a typed reason.** Extends DEV-2243 decision 35 (user) beyond model-scoped metrics. Options: A, superset everywhere; B, superset for model-scoped metrics only, refusing names over 128, unsupported constructs and cross-dataset field qualifiers; C, override decision 35. Took A: one validity rule and one fallback; SLayer never refuses a document upstream passes. Standing instruction: ask the owner whenever applying it would break existing behaviour. (user)
4. **Repeated keys refuse documents that load today**, at import, in storage and on save/edit, every repeated key listed with its line. Options: approve; keep last-wins. Approved: upstream refuses the same documents, and last-wins silently loses a value. (user)
5. **The shared reader follows upstream's `UniqueKeyLoader` (`ossie/_loading.py`):** explicit duplicates refused at any depth, repeated merge directives allowed, merge overrides kept, JSON read by the same loader, keys compared by their constructed value; a scalar the loader cannot build is a reason naming value and line. Implemented natively, no runtime dependency on upstream's SDK (DEV-2243 decision 20). (default)
6. **Duplicate keys are found on the composed node tree, so every repeated key is reported in one refusal; a reader failure is terminal** — no semantic reasons are added for that file, since no document exists to check. (codex)
7. **The edit path reads through the shared reader**, so ruamel and `json.loads` in `slayer/core/document_text.py` never see, normalise or silently drop a repeated key; save and edit of hand-placed duplicate-key YAML and JSON are tested. (codex)
8. **"Not evaluable" comes from SLayer's own expression reader failing on the form it would run** (the existing first-readable-candidate rule), never from keyword matching; tests cover `sql` alone, a portable entry alone, both, vendor-only and mixed entries, and keywords inside literals. (codex)
9. **Parity cases with upstream's loader:** duplicates at depth, repeated merge directives, merge overrides, JSON duplicates, keys equal by value (`1`/`true`), unbuildable scalars with their line — each checked against upstream `validate.py`. (codex)
10. **Per-file refusal and the legacy-wrapper upgrade stay as DEV-2243 merged them**; an invalid document is never partly stored. (default)
11. Rejected (codex): validating ANSI identifier syntax for names — upstream does not, and a YAML name is the identifier's value (decision 3).
12. Rejected (codex): field-expression qualifier rules, qualified-star dataset ownership in a `FILTER` rule, and amending `queries/model-scoped-metrics` — superseded by decision 3 (no such refusals).

## Risks / Trade-offs

- [A document SLayer accepts may not run in other Ossie tools] → upstream's validator accepts it too; SLayer reports what it cannot evaluate.
- [Stored documents with repeated keys stop loading after upgrade] → the error names every key and line; `docs/upgrading/2.0.md` shows the fix.
- [Load-time parsing of every expression costs time] → decomposition is already cached per document text.

## Migration Plan

Fix repeated keys named by the error and reload; no data migration.
