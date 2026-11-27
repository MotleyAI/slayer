## Context

See proposal.md for the motivation. Constraints:

- docs.motley.ai grafts SLayer's `nav` but not its redirect map, so a renamed file would 404 there. Only the
  `queries.md` split adds files; no existing path changes.
- Notebooks are not edited (an edit forces a re-execution). Seven notebooks link to anchors the split moves
  (`#cross-dataset-metrics`, `#query-extensions`, `#list-fields`, `#query-lists`, `#variables`).
- Every page under `docs/` stays in the nav; `NAV_EXEMPT` (`CLAUDE.md`, `api_gaps.md`,
  `getting-started/flight-sql.md`) is unchanged.
- Architecture: docs and tests only; no model element, import arrow or arc42 file is touched, and no arc42
  principle binds docs layout. The binding rule is the repo `CLAUDE.md` nav-coverage rule.
- `tests/test_docs_guards.py::test_every_relative_docs_link_resolves` drops the `#fragment`, so nothing checks
  anchors today. A probe on main: 266 anchored relative links (pages + notebook Markdown cells), none broken under
  Python-Markdown's default slugify.

## Goals / Non-Goals

**Goals:**

- The nav below, each topic in exactly one place.
- The four-way `queries.md` split with no dangling anchor anywhere, guarded in CI.

**Non-Goals:**

- New content: the follow-up add-ons (a "how your model is stored" intro, a Motley Cloud link, published
  `openapi.json` / JSON Schemas, `llms.txt`, folding `docs/CLAUDE.md` into the root `CLAUDE.md`, moving the Mode-B
  maintainer note to `CONTRIBUTING.md`) are a separate issue.
- Moving the RLS (10) and Query Cache (12) tutorials next to their pages.
- Redirect-map changes; editing notebooks.

## Decisions

### Nav (top to bottom)

1. **Home**: `index.md` (keeps its "Upgrading from 1.x?" link to `upgrading/2.0.md`).
2. **Get started**: `getting-started/index.md` (section index), MCP (AI Agents), CLI (Terminal), REST API (Any
   Language), Python SDK.
3. **Connect your data**: Datasources (`configuration/datasources.md`), Database Support (`database-support.md`),
   Storage Backends (`configuration/storage.md`), SQL Clients (`configuration/sql-clients.md`), Flight SQL
   (`interfaces/flight-sql.md`), SQL API (`interfaces/pg-facade.md`).
4. **Model your data**: Datasets, Semantic Models, Ontologies and Mappings, Auto-Ingestion; **Schema Drift**
   (`concepts/schema-drift.md` as section index, then "Worked example" = `examples/schema-drift.md`); **dbt** (SLayer
   vs dbt, Importing dbt definitions, Tutorial = `examples/11_dbt_metricflow/dbt_metricflow.md` + its notebook);
   **Cube** (Importing Cube definitions, Tutorial = `examples/17_cube_import/cube_import.md` + its notebook);
   **OSI** (Importing Ossie documents, Exporting to OSI, Tutorial = `examples/13_osi_import/osi_import.md` + both
   notebooks).
5. **Query your data**: Queries, Expressions, Time, Filters, Cross-Dataset Queries, Saved & Multistage Queries, SQL
   vs DSL (`concepts/references.md`).
6. **Agent context**: Memories, Search.
7. **Operate**: Row-Level Security, Query Cache, OpenTelemetry (`reference/opentelemetry.md`), Usage Reporting
   (`reference/usage-reporting.md`), Upgrading to 2.0 (`upgrading/2.0.md`).
8. **Tutorials**: examples 01–10, 12, 14, 15, 16, entries as today.
9. **Reference**: MCP Server, REST API, Python Client, CLI, Glossary (`concepts/terminology.md`), Ossie Layer 3,
   Ossie compliance.
10. **Contributing**: `development.md`.

### The `queries.md` split

All four files live in `docs/concepts/`, so relative links to sibling and parent pages stay valid. Moved headings go
up one level with unchanged text, so each slug is identical in its new file.

| File | H1 | Sections |
|---|---|---|
| `queries.md` | Queries | Query Parameters; Dimensions (+ subsections); TimeDimension; OrderItem; Response; Population; Examples (Count by status … Share of parent); Query extensions |
| `filters.md` | Filters | the whole Filters section: Comparison Operators, Boolean Logic, Field vs metric filters, Scalar Functions in Filters, Filtering on Computed Metrics, Filters and Auto-Joins, List fields, Window functions in filters, Variables (+ Variables passed as a runtime kwarg) |
| `cross-dataset-queries.md` | Cross-Dataset Queries | Cross-dataset metrics; Multi-hop dimensions; Choosing a root dataset (+ Forcing a root with `root_hint`) |
| `saved-and-multistage-queries.md` | Saved and Multistage Queries | Query lists; Run a saved query by name (+ Refining a saved query) |

Section bodies move verbatim **except link targets**: every same-page `#…` link that points across a file boundary
after the split (in moved or remaining text, e.g. `#expression-dimensions`, `#ordering-by-something-you-dont-project`,
`#filters-and-auto-joins`, `#population`) becomes `<file>.md#…`.

### Hidden anchors for moved sections

`queries.md` ends with a short "Moved sections" list: each item carries `<a id="<old-slug>"></a>` for every moved
heading slug (subsections included) and links to the section's new home. Notebook, docs.motley.ai and external deep
links keep landing on the right pointer; Markdown pages link to the new homes directly.

### Anchor guard

In `tests/test_docs_guards.py`:

- Every relative `page.md#anchor` and same-page `#anchor` link outside code, in `docs/**/*.md` and in notebook
  Markdown cells, resolves to a heading slug (Python-Markdown default slugify: NFKD → ASCII, drop `[^\w\s-]`,
  lowercase, `[-\s]+` → `-`; inline code and link markup stripped from the heading; duplicates get `_1`, `_2`, …) or
  an explicit `id=` in the target page.
- Meaningful-count sanity checks: ≥ 250 anchored links overall and a separate minimum for notebook cells (set from
  the measured count, leaving headroom).
- Unit tests: slug extraction (backticks, punctuation, duplicates, explicit ids) and notebook-cell extraction on an
  inline notebook JSON with one valid and one broken link.
- `MOVED_ANCHORS` (like `MOVED_PAGES`): moved `queries.md` slug → new `page.md#slug`. Tests: each moved slug is a
  hidden id in `queries.md` and its new target resolves; no `docs/**/*.md` link targets `queries.md#<moved slug>`
  (notebooks exempt).

### Decisions log

- Moved anchors: hidden `<a id>` list at the end of `queries.md`, notebooks untouched, plus the anchor guard. Weighed:
  (A) hidden ids — no break anywhere, notebooks untouched; (B) edit and re-run 7 notebooks, no stubs — breaks the
  no-notebook-edit constraint, external deep links fall to page top; (C) visible stub headings — clutter TOC and
  search with empty sections. Took A. (user)
- Flight SQL and SQL API pages under Connect your data, after SQL Clients — the candidate the issue named. (default)
- `api_gaps.md` and `getting-started/flight-sql.md` stay out of the nav: `CLAUDE.md` exempts the first, a future issue
  adds the second. (default)
- `concepts/ontologies.md` (added after the issue was written) under Model your data, after Semantic Models. (default)
- `interfaces/ossie-compliance.md` (added after the issue was written) under Reference, after Ossie Layer 3. (default)
- Examples 10 (RLS) and 12 (Query Cache) stay in Tutorials: the issue pairs only dbt, Cube and OSI with their
  tutorials. (default)
- Section assignment as in the table: Population stays in `queries.md` (every query has one); Query extensions stay
  in `queries.md` (they apply to any dataset or stage). (default)
- H1 of `concepts/terminology.md` → "Glossary" and of `development.md` → "Contributing", matching their nav labels.
  (default)
- Anchor guard as the structural fix: the split's failure mode (dangling anchors) has no check today because the link
  guard drops fragments. (default)
- A nav-shape test pins the set of ten top-level sections, so the reorganisation has a failing-first test; nav order
  is not asserted. (user)
- The anchor guard alone cannot catch unmigrated links (the hidden ids keep them resolving): add `MOVED_ANCHORS` with
  a no-Markdown-link-to-moved-anchor test, notebooks exempt. (codex)
- "Bodies verbatim" conflicts with same-page links that cross files after the split: bodies move verbatim except link
  targets. (codex)
- The ≥ 250 total does not prove notebook cells are read: separate notebook minimum plus a notebook-extraction unit
  test. (codex)

## Risks / Trade-offs

- [Zensical's slugify differs from Python-Markdown's default] → the probe's 266/0 on main matches the published
  site's anchors; the tests stage confirms against Zensical's documented default before fixing the algorithm.
- [docs.motley.ai rewrites nav entries during sync; a nested section-index entry (Schema Drift, dbt/Cube/OSI
  Tutorial) may graft differently] → reuse the shape the Tutorials tree already uses (bare page first, then named
  children), which the sync already handles.
- [`queries.md` keeps a small legacy list permanently] → accepted for link safety (user decision).
