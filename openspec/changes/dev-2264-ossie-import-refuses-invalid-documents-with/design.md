## Context

See proposal.md for the motivation. Today validity is decided in about twenty converter branches that each skip and warn (`slayer/osi/converter.py`), plus parser paths that log and drop (`slayer/osi/parser.py`). Two resolution laws run side by side: the current spec's, and the legacy one that exposes every source column. The reference revision is apache/ossie at the vendored pin `900d637` (`tests/fixtures/osi/ossie-schema.source.yaml`): `core-spec/ossie-schema.json`, `core-spec/spec.md`, `core-spec/expression_language.md`. Upstream's `validation/validate.py` at that pin is the model for the rule set; it is not on PyPI.

**Symptom → structural cause → fix.** The symptom is partial imports of invalid documents. The structural cause is that no single place decides validity: the converter both validates and converts, so each rule is re-checked, or forgotten, where it is used. The fix is one validator that owns every document rule and returns every violation; the converter assumes validity and loses its per-rule branches.

**Principles that apply.**
- `python.importers` P1, importers preserve meaning: it still holds, since a refusal emits nothing.
- System P6: violations are Pydantic models.
- System P11, fail at save time: an invalid definition is refused before anything is stored.

No arc42 or LikeC4 edit is needed: the new module sits in `slayer.osi`, inside the `importers` bucket, and the behaviour stays in the existing `importers/osi-import` spec.

## Goals / Non-Goals

**Goals:**
- One module, `slayer/osi/validation.py`, holds every document rule and one Ossie-identifier helper shared with the converter.
- The converter's branches for rules the validator owns are deleted, along with its legacy path.

**Non-Goals:**
- Upstream changes after the pin: `AGG(dataset.metric)`, `AGG(source.measure)`, and the rule that a key names each field once. A pin catch-up adopts them.
- Changing what counts as a SLayer or environment limit. Those reports stay as they are.
- The dbt and Cube importers.

## Decisions

### Rule set (ids used in the error)

| id | rule |
|---|---|
| `unreadable-file` | a file cannot be read, does not parse as YAML/JSON, or its root is not a mapping |
| `duplicate-key` | a YAML or JSON mapping repeats a key, at any depth (file and line) |
| `legacy-shape` | a wrapper that cannot be upgraded (empty, malformed, mixed with root `datasets`, declaring `0.3.0.dev0` or an unknown version, a root key the model also sets), or a `custom_extensions` string that does not decode to a JSON object |
| `schema` | the upgraded document against the pinned schema, every `iter_errors` entry |
| `extension-data` | a non-JSON value under `data` (date, timestamp, non-finite number, non-string key) |
| `name-length` | a declared name longer than 128 characters |
| `duplicate-name` | repeated dataset, relationship or model-scoped metric names in a document; repeated field + dataset-metric names in a dataset; dataset names repeated across the documents of one import |
| `unknown-dataset` | a relationship `from` / `to` that is not a dataset of its document |
| `undeclared-key-field` | a `primary_key`, `unique_keys`, `from_columns`, `to_columns` name that is not a declared field |
| `relationship-arity` | `from_columns` and `to_columns` of different lengths |
| `ossie-sql` | an Ossie-dialect form that does not parse as one expression, calls `AGG`, holds a construct the pinned language does not support, or has a `FILTER` predicate reading another dataset |
| `field-scope` | a field-expression column qualified by another dataset of the document, unless the qualifier is also the last part of the dataset's own table `source` |
| `metric-scope` | a dataset- or model-scoped metric reference outside its scope (spec "Metric name resolution", "Source columns in dataset-scoped metrics") |

The `metric-scope` rules are checked on every form SLayer can parse: Ossie forms, and vendor dialects SLayer's reader parses. A form that does not parse is left to `ossie-sql` if it is an Ossie form, and skipped if it is a vendor form.

### Decisions log

1. Older shapes are upgraded as Ossie's migration section prescribes and then validated under the current rules. Lenient resolution is removed, and unknown or missing versions are refused. Options: A, upgrade then validate; B, keep lenient resolution for wrappers. Took A: one validity law and one converter path, and Ossie's version history says converters "must now resolve them as fields". (user)
2. Mismatches with the live source stay per-entity drops with a report: a column the source lacks, a non-own qualifier other than `field-scope`, an unknown or ambiguous `source.column`. Only document-checkable rules refuse. Options: A, refuse; B, drop with a report. Took B: validity depends on the document, not on the database it is pointed at, matching the entity-level rejection in Ossie's Metric Scoping rule 9. (user)
3. One validator module runs in the parser and when the converter is constructed. The converter's per-rule skip branches and its legacy path (`_overlay_fields`, `_apply_legacy_primary_key`, the `_shadowed` revert, unqualified-unique metric resolution, the relationship-walking resolution of field-expression references, `OSIDocument.legacy`) are deleted, and one identifier helper is shared. A structural fix rather than instance fixes. (default)
4. Validate against the vendored pin `900d637`, not upstream `main`. Under the pin, any `AGG` call in an Ossie form is an `ossie-sql` violation (see 16). (default)
5. Model-scoped metric reference rules refuse, following the spec's MUST, even though upstream's validator only warns "for now". (default)
6. A "valid identifier" is any non-empty name of at most 128 characters, regular or delimited. Ossie decides this: ANSI naming includes delimited identifiers, and upstream's `declared_name` treats them as legal. (default)
7. Uniqueness and reference matching use Ossie's normalized identifier form. SLayer-only case collisions (non-regular names that differ only in case) keep today's refusal, since SLayer's save refuses case collisions. (default)
8. Only `OSSIE_SQL_2026` forms refuse the document when they break the language; vendor dialects, `ANSI_SQL` included, are opaque to Ossie. (default)
9. A field-expression column qualified by another dataset of the document is refused (`field-scope`), which removes SLayer's resolution of such references. A column qualified by the dataset's own name reads the source column. Any other non-own qualifier is a live mismatch (see 2). (default)
10. Dataset names that collide across the documents of one import stay refused (a SLayer limit: one namespace per datasource), now compared case-insensitively, inside the same aggregated error. (default)
11. Ontology and mapping documents (root `ontology` or `concept_mappings`, no `datasets`) stay skipped with a logged warning. (default)
12. `jsonschema` becomes a core dependency, and the pinned schema ships as package data at `slayer/osi/ossie-schema.json`. The provenance file and the drift script point at it. A Pydantic mirror of the schema would be a second copy to keep in sync by hand. (default)
13. `OsiConversionError` moves to `slayer/osi/validation.py` and stays importable from `slayer.osi.converter`. It carries `violations: list[OssieViolation]`, where `OssieViolation` is a Pydantic model with `file`, `document`, `location` (a path by entity names), `rule` (an id from the table) and `message`. `str(error)` gives one line per violation. (default)
14. References match as Ossie identifiers: key and relationship names case-insensitively, storing the declared spelling; quoted references by the normalized form. This was an approved extension, so that no Ossie-valid document is refused. (user)
15. No arc42 or LikeC4 edit is needed. `python.importers` P1 and system P6 and P11 apply, and the behaviour stays in `importers/osi-import`. (default)
16. Ossie forms are held to the pinned language, not only parsed: no `AGG`, none of the "Not Supported in Expressions" constructs, and a `FILTER` predicate reads only its aggregate's dataset. The "Predicate on another dataset" scenario flips from unconverted to refused. (codex)
17. Duplicate YAML and JSON mapping keys are refused (`duplicate-key`), as upstream's `UniqueKeyLoader` does. (codex)
18. An `OSSIE_SQL_2026` entry beside a top-level `sql`: if it is unparseable or differs from the `sql`, an informational entry; if it is identical, nothing. (codex)
19. A wrapper root key that the model also sets is a `legacy-shape` violation; it is never overwritten. (codex)
20. Validation on the converter side runs when the converter is constructed, before `collect_metadata`. It re-runs the semantic rules and the schema on the typed documents' dump (`by_alias`, `exclude_unset`, private attributes excluded). Its reduced guarantee is stated: unknown keys and other raw-shape faults are caught only by the parser, because a typed document cannot carry them. (codex)
21. The parser attaches private provenance to each document (file, file ordinal, wrapper-entry ordinal). It is never dumped, and it orders the violations stably. (codex)
22. Export round-trip tests assert that every exporter output passes the validator and re-imports. (codex)
23. Rejected (codex): upgrading `custom_extensions` only for pre-0.3 documents. SLayer 1.8 exports declare `0.3.0.dev0` and carry `custom_extensions`, and decision 1 keeps them importing.
24. Rejected (codex): validating ANSI delimited-identifier syntax for names. A YAML name is the identifier's value, and ANSI writes any value as a delimited identifier by doubling quotes. Tests for names with a quote, a dot, whitespace and Unicode are added instead.
25. Rejected (codex): refusing own-dataset-qualified columns in field expressions. Ossie reads identifiers in field expressions as source columns, SLayer reads the own-dataset qualifier as the source (today's behaviour), and upstream's validator does not flag it. This is stated in the spec and tested.

## Risks / Trade-offs

- [Real-world documents that imported partly now fail entirely.] → The error lists every violation with its location in one run, and `docs/upgrading/2.0.md` gives a before and after for each breaking change.
- [SLayer's Ossie reader might fail on valid Ossie SQL, which would wrongly refuse a document.] → `ossie-sql` covers parsing only; SLayer's inability to convert a parsed expression stays a per-entity report. The vendored upstream examples and the exporter round trips are guards.
- [Validation can drift from the converter's resolution.] → Both use one shared identifier helper. The converter asserts validity rather than re-checking, so any disagreement surfaces as a crash in tests, not as a silent skip.
- [Adding `jsonschema` as a runtime dependency.] → It is pure Python and widely used, and it is the schema's own validator.
