## 1. Tests (pr-tests; every spec scenario becomes a test, failing until implemented)

- [x] 1.1 Fixture: the vendored flights ontology, semantic model and mapping stored in a datasource backed by an in-process DuckDB or SQLite database holding small FLIGHT, AIRCRAFT, AIRPORT, ROUTE, RUNWAY and CARRIER tables, including a latitude of 95, an `AIRPORT` row with NULL latitude, a `ROUTE.dest_airport_code` absent from `AIRPORT`, NULL `cancel_code` on non-canceled flights and a `dist_grp` outside 1–10; variants declaring keys and a `FLIGHT → AIRCRAFT` relationship per scenario. These are unit tests (in-process engines), never marked `integration`.
- [x] 1.2 Annotations: one test per scenario of "Mapped elements are annotated with their concepts", "Verbalizations are filled with mapped names" and "Value-type constraints are shown as domains"; add inherited constraints (a subtype of `DegreesLatitude`), a qualified `Base.r` relationship, a filtered `X WHERE ( P )` position, a computed single-dataset expression, a multi-dataset expression, two mappings annotating one field, a mapping against a semantic-model document vs one against the datasource, an unresolved mapping, and swapped composite columns that do not line up.
- [x] 1.3 Inspect: the "Inspect shows annotations and the concept graph" scenarios and "Compact ontology inspect is the concept graph", in markdown and json, on the MCP tool, REST, CLI and Python client.
- [x] 1.4 Search: the "Search reads concepts" scenarios; two ontologies declaring `Delay` (unqualified and qualified `concept:`), subtype matching, no visible match, embeddings refreshed on mapping save/edit/delete (with a fake embedding client), and no concept text derived from a dataset hidden from the caller in any result.
- [x] 1.5 Model checks: the identifier and multiplicity scenarios; composite primary key, enforced composite key, a unique singleton inside a compound identifier, `OneToOne` in both stored orientations, a ternary and a unary relationship not compared, and parallel relationships.
- [x] 1.6 Save warnings: "A dataset save warns", plus mapping, ontology and semantic-model saves and edits returning their mismatches on MCP, REST, CLI and the Python client; `tests/test_ontology_checks_report_only.py` (named by the arc42 principle) asserting no check refuses a save or changes a stored model.
- [x] 1.7 Data checks: the value-constraint and required-link scenarios; reversed comparisons, unary minus, `NOT`, `IN`, a quoted literal containing the concept name, a filtered position, a compound identifier with a NULL part, `Airport.latitude` and `Airport.longitude` reported separately, and the cross-dataset anti-join with no stored relationship (the feasibility test — run it first; if the engine cannot express it, stop and raise it as a plan question).
- [x] 1.8 Not checked: "The flights rules not checked", a multi-dataset position, a check query the database rejects, an unresolved mapping; `tests/test_ontology_not_checked_listed.py` (named by the arc42 principle).
- [x] 1.9 Report entries: "Force-clean ignores ontology findings"; JSON serialisation on MCP and REST, CLI formatting, and access redaction of findings (a caller who cannot see `AIRCRAFT` gets no finding naming it).
- [x] 1.10 Tool surface: "The descriptions fit and name the ontology use".
- [x] 1.11 Codex review of the tests against this change's specs; fold findings.

## 2. Architecture (approved wording in design.md decisions 26–29; apply verbatim)

- [x] 2.1 Add the three arrows to `architecture/model.c4`, the `ontologies` touches to `architecture/index.yaml`, the §1 sentence and §3 principles 3–4 to `architecture/python.ontology.arc42.md`; run `la-arch-diagrams`; verify `la-arch-check` and `npx -y likec4@1.47.0 validate architecture` pass once the code lands. If measured edges differ from the approved arrows, stop and ask before editing the model.

## 3. Ontology package (pure)

- [x] 3.1 Extend the concept graph with effective descriptions, `requires` (accumulated over ancestors), verbalizations and multiplicities with provenance; verify 1.2.
- [x] 3.2 `core.syntax`: read a `requires` entry, classify it (plain value-type subset, single-path required link, other) and build a check predicate by respelling the concept variable to a mapping value and conjoining the mapping predicate; verify the 1.7 syntax cases.
- [x] 3.3 Annotations per resolved mapping: field roles, dataset own / also-populated concepts, relationship line-up, filled verbalizations, domains; verify 1.2.
- [x] 3.4 Model checks (identifier, multiplicity) and the check plans for data checks (value constraints, required-link anti-joins as SLayer queries), plus not-checked entries; the `OntologyFinding` model; verify 1.5 and the pure half of 1.7–1.8.

## 4. Storage, engine and validation

- [x] 4.1 `warnings_out` on semantic-model, ontology and mapping saves and edits through storage, the access-filtered view and the engine; mismatches of the change's mappings and datasets added on every save; verify 1.6.
- [x] 4.2 `validate_datasets` runs the model and data checks per datasource under the caller's storage and policies; `OntologyFinding` in `DriftReportEntry` and `REPORT_ONLY_FINDINGS`; verify 1.7–1.9.

## 5. Surfaces

- [x] 5.1 Inspect: dataset, field and relationship annotations; ontology concept graph (compact) plus text (full); verify 1.3.
- [x] 5.2 Search: concept text in the rendered entity text, `concept:` entity references, embedding refresh on mapping and ontology save/edit/delete; verify 1.4.
- [x] 5.3 MCP, REST, CLI and Python client: save warnings, `validate_datasets` entries, tool descriptions within budget; verify 1.6, 1.9, 1.10.

## 6. Docs

- [x] 6.1 `docs/concepts/ontologies.md`: drop "not yet used"; state what is annotated, what is checked (and how), what is not checked; still no Layer 4 support claim. Update the `validate_datasets`, `search` and `inspect` docs (`docs/reference/mcp.md`, `docs/reference/cli.md`, `docs/reference/rest-api.md`, `docs/reference/python-client.md` as they cover them); verify every touched page is in the `zensical.toml` nav.

## 7. Gates

- [x] 7.1 Local test scope per the `/la:pr` flow, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), and the enforcement bundle (`la-arch-check`, `npx -y likec4@1.47.0 validate architecture`) all pass.
