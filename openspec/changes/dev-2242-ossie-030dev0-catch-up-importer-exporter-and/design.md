## Context

See proposal.md (Why). Upstream pin: `apache/ossie@c1a33181c815852f059ce7986729612332240ff8` (2026-11-09); the
normative text is `core-spec/spec.md` §Name Resolution, §Metric Scoping and §Version History, and
`validation/validate.py` (`SOURCE_QUALIFIER`).

Field-based resolution already exists for flat documents in one reader: metric references are canonicalised to
declared fields, keys and relationship columns must be declared fields, and wrapped (legacy) documents keep
resolving against source columns too. The exporter already writes flat documents whose metrics name fields only.

SLayer's `Column.sql` has one mixed namespace: a bare `x` reads the logical column `x` when one exists, else the
physical `x`. So a physical column whose name a field has taken cannot be read (DEV-2175).

Principles: `python.importers` §3.1 "Importers preserve meaning" (every `source.column` case either reads the
physical column with the source's meaning or is reported); `system` §3.6 (Pydantic models). No `.c4` or arc42
change: the change stays inside `slayer.osi`, `slayer.importers`, `scripts/` and tests, with no new import edges
between nodes. The spec deltas attach to the existing `importers/osi-import` and `importers/osi-export`
specs (owned by the `python.importers` bucket).

## Goals / Non-Goals

**Goals:** one current-spec reader for every flat document; `source.column` with the source's meaning or a report;
exports that validate against 0.3; every upstream example vendored, imported and drift-checked.

**Non-Goals:**
- Reading a source column behind a field that took its name (DEV-2175); reported as unconverted meanwhile.
- Writing `source.column` on export; helpers export as fields, their hidden flag in the SLAYER extension.
- Lossless re-import of `--no-extensions` exports.
- Native Ossie documents as SLayer configuration and Ossie dataset-surface completeness (DEV-2244).

## Decisions

- **`source.column` in every flat document.** Alternatives: only documents declaring `0.3.0.dev0`. Taken: every
  flat document — a pure superset (`source.x` is unconverted in every earlier flat document today), one reader, and
  upstream supersedes `0.2.0.dev0`. (user)
- **Renamed collision reported, not read.** Alternatives: (i) unconverted + strict xfail pinned to DEV-2175;
  (ii) pull DEV-2175 in; (iii) leave unspecified. Taken: (i) — preserves meaning; DEV-2175 lifts every importer's
  collision together. (user)
- **Drift covers the schema and every vendored example.** Alternatives: schema only. Taken: schema + examples, so a
  stale copy cannot hide a real gap. (user)
- **Existing tests and fixtures aligned with the plan:** `tests/test_osi_parser.py`
  (`test_known_versions_constant` gains `0.3.0.dev0`; `test_known_version_no_warning` uses the flat
  `current_shop.yaml` for `0.3.0.dev0`), `tests/test_osi_export_document.py` (version), `tests/_osi_helpers.py`
  `flat()` and `tests/test_osi_models.py` (`0.3.0.dev0`), `tests/fixtures/osi/current_shop.yaml` and
  `docs/examples/13_osi_import/shop.osi.yaml` (`0.3.0.dev0`), `tests/test_ossie_schema_drift.py` (new URLs, example
  fakes). (user)
- **Exporter writes no `source.column`.** Alternatives: rewrite pass-through helpers as `source.column`; only report
  hidden columns as lossy. Taken: only the version changes — re-import fidelity without extensions is not a goal,
  and SLayer becomes an Ossie superset. (user)
- **Duplicate output labels of a query source are reported.** Alternatives: out of scope (tracked on DEV-1358, or
  untracked). Taken: in scope — every field and `source.column` reading such a label is reported, so the new path
  never ships with a silent pick. (user)
- **`source` is not reserved.** A field named `source` takes precedence (upstream rule 3); this corrects the
  tracker's "reserved". (default)
- **Qualifier matching.** `source` casefolds quoted or not, as upstream's validator does; field precedence uses the
  importer's existing unquoted-name matching. (default)
- **Version handling.** `0.3.0.dev0` joins the known versions; only a wrapped document declaring it is skipped;
  unknown versions keep warn-and-parse; `0.2.0.dev0` stays known and silent. (default)
- **Version recorded as declared.** `OSIDocument.version` is optional and records only the declared version, so a
  versionless legacy wrapper is never labelled 0.3. Nothing downstream reads it. (codex)
- **Helper column.** A model column reading the physical column unchanged is reused; otherwise a hidden
  `_<metric>_<n>` column reads it, created only when the metric converts (so a metric may be named like the column).
  The rewrite runs over every root-scope column reference of the metric before owner resolution, so `FILTER`
  predicates, percentile orderings and compound operands all see the helper (a compound operand is then materialised
  as today). (default; operand coverage codex)
- **Identifier folding.** The leaf matches the live source columns with the existing physical-name matcher
  (quoted exact, unquoted folded); several matches make the metric unconverted as ambiguous. (codex)
- **Duplicate labels detected where query outputs are recorded.** The live query source keeps which labels occur
  more than once; the OSI converter consults it. Other importers' behaviour is unchanged. (default)
- **Upstream examples vendored byte-identical**, all six, under `tests/fixtures/osi/upstream/`, with one provenance
  file (repository, commit, date, upstream path → vendored path) shared by the schema and the examples; the drift
  script reads its file list from it with PyYAML (installed by the workflow), and compares the schema canonically
  and the examples as text. Exit codes unchanged in meaning: 0 all match, 1 any drift, 2 any fetch failed.
  (default; all six codex; PyYAML over stdlib-only: user)
- **No OpenSpec requirement for the drift tooling.** It is CI tooling, not product behaviour. (default)
- **Docs follow the spec's wording** for `--no-extensions` ("every entity that exists only in an extension"), not
  the broader "everything". (default)
- rejected (codex): "implement a physical-namespace escape hatch for the renamed collision instead of an xfail" —
  that is DEV-2175's design choice, and the user decided (i).

## Risks / Trade-offs

- [Upstream edits examples often] → the weekly run goes red more often; re-vendoring is one commit with the
  provenance file updated.
- [A vendored example uses a construct SLayer cannot express] → the example test pins the exact expected report
  (tpcds: its three window metrics) rather than "no entries".
- [Helper columns surface as fields to other Ossie tools after export] → accepted (user decision on the exporter).
