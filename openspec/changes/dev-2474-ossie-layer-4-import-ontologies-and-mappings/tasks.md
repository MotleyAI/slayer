## 1. Corpus

- [ ] 1.1 Vendor `ontology/ontology.json`, `ontology/mapping.json`, `ontology/ontology.md` from apache/ossie at the pinned `d4dca4e9` into `tests/fixtures/ossie_corpus/ontology/`, add them to `scripts/ossie_schema_drift.py`'s tracked paths and `provenance.yaml`; verify `tests/test_ossie_schema_drift.py` passes. (A local checkout is at `/home/james/GitHub/SLayer/ossie`; the files are unchanged on upstream `main`, so the pin is not bumped.)
- [ ] 1.2 Harvest upstream's ontology and mapping validation cases (`validation/tests/test_ontology.py`, `python/tests/test_ontology_models.py` at the pin) into `harvested/` as the semantic-model cases were; verify each harvested case's expected verdict against the vendored `validation/validate.py`.

## 2. Tests (pr-tests; every spec scenario becomes a test, failing until implemented)

- [ ] 2.1 `tests/test_dev2474_upstream_oracle.py` (named by the arc42 principle tag): every vendored ontology and mapping example is accepted and passes the vendored validator; SLayer refuses every harvested case the validator reports an error for ("Ossie's validator is the floor").
- [ ] 2.2 `tests/test_dev2474_round_trip.py` (named by the arc42 principle tag): every example round-trips byte-identical through save and export, including FOAF's comments, prefixes, iris and annotations.
- [ ] 2.3 Ontology checks: one test per scenario of "Ontologies are checked when saved", "Rule expressions are kept, not interpreted" and "recognised by Ossie's keys".
- [ ] 2.4 Mapping checks: one test per scenario of "Mapping references resolve", "Mapping structure follows the ontology", "Mapping expressions are row-level Layer 1…" and "Mapping expression types fit the concept"; include a typed-mismatch case and the untyped flights case separately.
- [ ] 2.5 `core.syntax` mapping-expression production: value with and without `WHERE ( … )`, nested parentheses and string literals containing `WHERE`, rejection of aggregates and windows.
- [ ] 2.6 Storage and integrity on both YAML and SQLite backends: verbatim storage, replace, datasource delete, every "Explicit changes never break a stored mapping" scenario, and the database-driven warning path.
- [ ] 2.7 Access tags: redaction, hidden-content conflict on edit, hidden entries kept on save.
- [ ] 2.8 Import: osi-import delta scenarios (file order, refused ontology refuses its mapping, `flights.yaml` upgrade, "Ontology examples", "Ontology document").
- [ ] 2.9 Export: osi-export delta scenarios.
- [ ] 2.10 Custom backends: a backend without the ontology hooks keeps working and fails typed.
- [ ] 2.11 Surfaces: Python storage API, client (+ sync), REST, CLI (`ontologies` / `ontology-mappings` list, show, create, replace, delete), MCP tools and `inspect` kinds, MCP structured edits ("An agent edits one concept"), and the tool-surface budget rule.
- [ ] 2.12 Codex review of the tests against this change's specs; fold findings.

## 3. Architecture (approved wording in design.md decisions 25–28; apply verbatim)

- [ ] 3.1 Add node `ontology` and its four arrows to `architecture/model.c4`, the `ontologies` entry to `architecture/index.yaml`, and `architecture/python.ontology.arc42.md`; run `la-arch-diagrams`; verify `la-arch-check` and `npx -y likec4@1.47.0 validate architecture` pass. If measured edges differ from the approved arrows, stop and ask before editing the model. Until this task lands, `la-arch-check` reports "spec group ontologies is mapped by no element and not cross-cutting" (since the plan commit); the `index.yaml` line needs the node, and the node needs `slayer/ontology/` on disk, so all three land together with the package.

## 4. Parsing and checks

- [ ] 4.1 `core.syntax`: the mapping-expression production `<value> [WHERE ( <predicate> )]`, exposed for the ontology package; verify 2.5.
- [ ] 4.2 `slayer/ontology/`: Pydantic document models, offline schema validation through a local registry for the absolute `$ref` URIs (no network), ontology checks; verify 2.3 and the ontology half of 2.1.
- [ ] 4.3 Mapping resolution against a given set of datasets and a given ontology: references, structure through `extends`, expressions, types, the per-mapping unknown-type entry; verify 2.4 and the mapping half of 2.1.

## 5. Storage

- [ ] 5.1 Backend hooks defaulting to unsupported; YAML (`ontologies/<name>.yaml`, `ontology_mappings/<datasource>/<name>.yaml`) and SQLite (`ontologies`, `ontology_mappings` tables) implementations; `graph_fingerprint` changes; verify 2.6 (storage part) and 2.10.
- [ ] 5.2 Integrity: re-check affected mappings on every explicit ontology, semantic-model, dataset and field change and refuse with every broken mapping and path; database-driven changes warn and leave mappings unresolved; unresolved mappings reported in list/inspect and as unloadable on load; datasource delete deletes its mappings; verify 2.6.
- [ ] 5.3 Access-filtered storage: redaction from parsed expressions' dataset references, hidden-content conflicts, hidden entries re-inserted on save; verify 2.7.

## 6. Import and export

- [ ] 6.1 `slayer/osi/importer.py`: recognition, ontology → semantic model → mapping ordering, refused-dependency refusal, embedded `ontology_mappings` upgrade with its report entry; verify 2.8 and 2.2.
- [ ] 6.2 Export API and `export-osi --ontology` / `--mapping`; verify 2.9.

## 7. Surfaces

- [ ] 7.1 Python storage API and client (+ sync); REST routes; CLI command groups; verify 2.11.
- [ ] 7.2 MCP tools, structured edits and `inspect` kinds within the description budget; verify 2.11.

## 8. Docs

- [ ] 8.1 New `docs/concepts/ontologies.md` (added to `zensical.toml` nav); update `docs/osi/osi_import.md`, `docs/osi/osi_export.md`, `docs/reference/cli.md`, `mcp.md`, `rest-api.md`, `python-client.md`. State exactly: imported, validated, stored and exported; not yet used; `requires` / `derived_by` kept but not interpreted; `X WHERE ( P )` is SLayer's reading pending upstream; no Layer 4 support claim. Verify every new page is in the nav.

## 9. Gates

- [ ] 9.1 Local test scope per the `/la:pr` flow, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), and the enforcement bundle (`la-arch-check`, `npx -y likec4@1.47.0 validate architecture`) all pass.
