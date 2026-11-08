# Proposal: Auto-generated mermaid diagrams in arc42 docs from the LikeC4 model

## Why

The arc42 docs point at LikeC4 views (`landscape`, `query_pipeline`) that nobody sees
when browsing on GitHub — the model renders only through LikeC4 tooling. Embedding the
views as mermaid diagrams makes the structure visible where people actually read the
docs, and generating + freshness-checking them means no one ever updates a diagram by
hand.

## What Changes

- New `tools/arch_diagrams.py`: pure-Python (stdlib + PyYAML) parser of the §5
  constrained `.c4` authoring convention plus deterministic mermaid emission and
  in-place marker-block rewriting. Becomes the ONE parser of the convention;
  `tools/arch_check.py` imports it (importlib) and drops its duplicate regex parsing.
- Marker pairs `<!-- likec4:<view_id> -->` … `<!-- /likec4:<view_id> -->` placed in
  the mapped arc42 docs; the generator rewrites only between markers.
- New `core_focus` view in `architecture/views.c4` (`include core, core -> *,
  * -> core`, title 'Core in context').
- New top-level `diagrams:` block in `architecture/index.yaml` mapping docs to view
  ids: system → landscape, core → core_focus, engine/sql → query_pipeline;
  semantics maps to nothing.
- New `arch_check` check id `diagrams-fresh`: mapped docs carry exactly their marker
  blocks, content equals regeneration byte-for-byte, no orphan markers, mapped views
  exist, `diagrams:` schema valid; failure messages name the single fix
  `poetry run python tools/arch_diagrams.py`.
- Doc updates: `system.arc42.md` §5 (view-authoring + marker convention), §4
  (`diagrams-fresh` note), §2 texts adjusted where views embed inline.
- Tests: `tests/test_arch_diagrams.py` fixture suite + a real-repo freshness test so
  plain `pytest` goes red on stale diagrams; `tests/test_arch_check.py` rewired for
  the parser consolidation.

## Capabilities

### New Capabilities

None — developer tooling and architecture docs only (`skip_specs: true`).

### Modified Capabilities

None.

## Impact

- `tools/arch_diagrams.py` (new), `tools/arch_check.py` (new check + parser
  consolidation), `architecture/` (views.c4, index.yaml, four arc42 docs),
  `tests/test_arch_diagrams.py` (new), `tests/test_arch_check.py`.
- No production `slayer/` code, no user-facing docs, no new dependencies; Node stays
  out of the check path (rejected `npx likec4 codegen mmd`: Node dependency, loses
  `#legacy`, needs mermaid ≥ 11.3).
