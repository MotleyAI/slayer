# Proposal: Adopt the query-semantics axioms as the normative in-repo spec

## Why

The query-semantics axioms and laws live only in external scratchpad notes, while
`docs/architecture/` carries ~2,700 lines of contributor prose that mixes normative
constraints with stale implementation walkthroughs. With the living-architecture layer
scaffolded (DEV-1857), the repo can become the normative home: behaviour in OpenSpec,
principles/laws in arc42 with status tags, history in git + the openspec archive.

## What Changes

- New cross-cutting spec `queries/semantics` stating the currently-true axioms
  (determination-based attribution, home-dataset/no-double-counting, grain guarantee,
  grain-union broadcasting, association-restricting filters, compositionality, loud
  degradation) with executed-value scenarios; not-yet-true semantics (mode axis,
  positions, population, second-order aggregation) stay out and arrive via their own
  issues' deltas.
- `models/join-cardinality` gains a Determination requirement (to-one chains as the
  evidence attribution consumes).
- New `architecture/semantics.arc42.md`: the complete end-target algebra — all axioms
  and all six laws — every clause tagged `[enforced: <id>]` / `[review]` /
  `[target: DEV-XXXX]`; laws live only here.
- `docs/architecture/` (14 files) deleted; normative residue distilled into new
  `architecture/engine.arc42.md` and `architecture/core.arc42.md` plus three added
  `sql.arc42.md` principles; `index.yaml` cross-walk extended (engine/core `arc42:`
  entries, `cross_cutting_arc42:` list).
- `system.arc42.md`: principle 8 becomes the algebra pointer + cardinality invariant;
  §4 documents the precise three-tag vocabulary; stale `docs/architecture` refs updated.
- `tools/arch_check.py`: tag vocabulary validation (all three kinds, principle items
  must carry a status tag) and orphan-arc42 detection.
- Root `specs/` folder (4 pre-OpenSpec planning docs) deleted — deliberately reversing
  the DEV-1857 retention note; archive references are historical and non-resolving by
  policy.
- Nav (`zensical.toml`), root `CLAUDE.md` documentation-requirements, and a test
  comment updated; a pointer comment posted on DEV-1841 for the deferred dice–slice law.

## Capabilities

### New Capabilities
- `queries/semantics`: the query-wide semantic axioms as observable behaviour —
  attribution by determination, no double counting, the grain guarantee, grain-union
  broadcasting, filter association semantics, compositionality, loud degradation.

### Modified Capabilities
- `models/join-cardinality`: ADDED requirement Determination — chains of provably
  to-one hops as the evidence attribution and filter-inlining consume.

## Impact

- `openspec/specs/queries/semantics/` (new), `openspec/specs/models/join-cardinality/`
- `architecture/` (two new node files, one new cross-cutting file, index.yaml,
  system/sql arc42 edits), `tools/arch_check.py` + `tests/test_arch_check.py`
- Deletions: `docs/architecture/` (14 files), root `specs/` (4 files)
- `zensical.toml` nav, root `CLAUDE.md`, `tests/test_projection_trim.py` comment
- No production `slayer/` code changes; no behaviour changes.
