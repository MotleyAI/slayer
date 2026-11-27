## Why

The docs nav mixes concepts, operations and integrations, and some topics live in two places. A developer should
find each topic once, in the order they work: set up, connect, model, query, give agents context, operate.

## What Changes

- Reorganise the `nav` block in `zensical.toml` into ten sections: Home, Get started, Connect your data, Model your
  data, Query your data, Agent context, Operate, Tutorials, Reference, Contributing. The dbt, Cube and OSI tutorials
  and the schema-drift worked example sit next to their pages.
- "Upgrading to 2.0" moves from the second nav slot to Operate; the home page keeps linking to it.
- Split `docs/concepts/queries.md` into `queries.md`, `filters.md`, `cross-dataset-queries.md` and
  `saved-and-multistage-queries.md`. Every docs link into a moved section is retargeted; `queries.md` keeps hidden
  ids for the moved anchors so notebook and external deep links still resolve.
- Terminology moves to Reference as the Glossary; `development.md` is the Contributing entry. Their H1s follow.
- A new docs guard checks that every relative `#anchor` link (Markdown pages and notebook Markdown cells) resolves,
  and that no Markdown page links to a moved `queries.md` anchor.
- No new content, no file moves (except the split's new files), no redirect-map change, no notebook edits.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. Documentation and test guards only (`skip_specs: true`).

## Impact

- `zensical.toml` (nav), `docs/concepts/queries.md` plus three new pages beside it, `docs/concepts/terminology.md`,
  `docs/development.md`, and every `docs/**/*.md` page linking to a moved `queries.md` anchor.
- `tests/test_docs_guards.py` (anchor guard).
- docs.motley.ai picks up the new nav on the next sync; no URL of an existing page changes.
