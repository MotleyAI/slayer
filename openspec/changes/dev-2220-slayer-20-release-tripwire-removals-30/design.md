## Context

See proposal.md (Why). Already on `main`: the 2.0-tagged removals and `2.0.0.dev0` (DEV-2231); `strict` as a
deprecated alias, the shared deprecated-input mechanism and `docs/upgrading/2.0.md` (DEV-2250 and later slices); the
strict binding and `docs/interfaces/ossie-layer-3.md` (DEV-2217); native Ossie documents and `slayer ossie-roundtrip`
(DEV-2243). `tests/test_law_removal_tripwire.py` already requires every `removal=` tagged site to be pinned in
`PINNED`, but it is static: many vocabulary spellings share one tagged site in `slayer/core/vocabulary.py`, so a
deleted rename mapping or a silent conversion goes unnoticed. The vendored Ossie corpus is at
`tests/fixtures/ossie_corpus/` (revision in `provenance.yaml`: `d4dca4e9`, 2026-11-22). A throwaway probe of all 290
REQUIRED Layer 1 cases on DuckDB and SQLite passed 218; its wrong values are DEV-2446's scope, its refusals DEV-2282's.

This slice merges last, after DEV-2441, DEV-2434, DEV-2444, DEV-1760, DEV-2446, DEV-2437, DEV-2428, DEV-2430,
DEV-2443, DEV-2439, DEV-2455 (a fresh install resolves sqlglot ≥ 30.18, under which the demo build fails) and
DEV-2246.
Its audits run against `main` as it stands once those have merged.

## Goals / Non-Goals

**Goals:**
- A compliance page whose every claim is backed by CI evidence it names, and whose gap lists cannot go stale silently.
- Executable proof that every 2.0-deprecated form still converts, with the right warning, until 3.0.

**Non-Goals:**
- Closing any Ossie gap: Layer 1 additive coverage is DEV-2282 (2.x), the dataset surface DEV-2244 (2.x). Full
  Layer 1 compliance is not claimed in 2.0.
- Certifying databases where CI does not execute the evidence.
- Committing release notes; tagging or publishing (after merge, owner go-ahead); opening the upstream grader PR
  (after 2.0.0 is on PyPI, owner go-ahead); filing the Storyline 2.0 migration issue (ask first).
- Architecture edits: no arc42 or LikeC4 change. system.arc42 §3.8 (one reader of Mode B) holds: the pins reach
  expressions only through the engine and `parse_expr`.

## Decisions

1. **DEV-1760 (mcp 2.x `MCPServer`) ships in 2.0** as its own slice merged before this one; its break is
   `create_mcp_server()`'s return type, with the dependency floors it raises. Alternative, defer to 3.0: keeps the
   `<2` cap and its resolver conflicts through 2.x. (user)
2. **DEV-2437 gates 2.0**, so the Layer 3 claim carries no value caveat. Alternative, list it as a deviation: a wrong
   value in an answered query, against the "follow Ossie strictly" ruling. (user)
3. **Layer 1 claims soundness, not full compliance.** DEV-2446 fixes the expressions SLayer accepts with a different
   value before 2.0; the refused constructs stay in 2.x under DEV-2282 (about 7–9 PRs, all additive) and are listed.
   Alternatives: pull DEV-2282 into 2.0 (no SemVer reason, large delay); list the wrong values as deviations
   (publishes known wrong answers). Changes the issue's original "compliant, with gaps listed". (user)
4. **The statement adds a semantic-model format section, worded conservatively.** Extends the issue's item 5. (user)
5. **Gap lists are pinned by strict xfails plus a page-sync test**, rather than tests pinning today's refusals: an
   xfail asserts Ossie's result, so each case is already the acceptance test of the issue that fixes it, and its strict
   marker forces its own removal. (user)
6. **The statement is its own page, `docs/interfaces/ossie-compliance.md`**, beside `ossie-layer-3.md`, since it spans
   layers. (default)
7. **Issue items 1–4 are audits**; their substance landed in DEV-2231, DEV-2250 and the slices. `x:sum` is removed,
   not deprecated (DEV-1920). (default)
8. **DEV-2244 gaps are listed and pinned the same way as Layer 1's.** DEV-2439 is a prerequisite: it fixes every
   strict-window case this slice pinned, so Layer 3 lists no gap. (user)
9. **The grader registration is drafted as a DEV-2220 comment**; the upstream PR waits for 2.0.0 on PyPI and the
   owner's go-ahead. (default)
10. **`skip_specs: true`.** No product behaviour changes. (default)
11. **Claims are scoped to the databases where CI executes the evidence** (the pins on DuckDB and SQLite; the Tier-1
    library matrix `tests/test_dev2120_library_matrix.py`); other databases are stated as not certified. (codex)
12. **The format claim states what passes** against the pinned corpus revision and the grader; wider support is
    described as intended, not proven. (codex)
13. **Each xfail declares `raises=`** the specific error its gap produces today, so an unrelated crash or a different
    wrong behaviour is not mistaken for the gap. (codex)
14. **One parameter per (case, database)** with stable ids, so a database-specific pass or regression cannot hide.
    (codex)
15. **One case id and one page row per independently fixable behaviour** (each function spelling, window form,
    format behaviour), never one per issue. (codex)
16. **Page rows carry the case id and a `planned` status; a test compares them with the xfail table, both
    directions. The fixing issue stays in each xfail's `reason`: docs name no tracker issues.** (user)
    Alternative, generate the page from a manifest: rejected, the docs build has no generation step and row checks
    suffice. (codex)
17. **A runtime 3.0 catalogue covers every Part 2 form and surface**: dataset, query, refinement and extension keys,
    datatype values, root-relative references, REST routes, CLI commands and flags, MCP arguments, search vocabulary,
    Python names, attributes, methods and errors, storage hooks, Mode-B spellings, `strict`, an extension object in
    `source_model`, a field's `sql`. This is the issue's item 2. (codex)
18. **Warning contracts per kind**: `deprecated_input` for renamed input, `deprecated_spelling` for Mode-B spellings,
    each with `removal` "3.0" and one `FutureWarning`; stored documents converting silently is tested as the
    deliberate exception. (codex)
19. **Every `PINNED` key is exercised by at least one catalogue case**, except the moved-module stubs (their own test).
    Alternative, a full guide-versus-code inventory: rejected for Part 1, whose breaking changes are not mechanically
    enumerable; Part 1 stays a manual audit. (codex)
20. **The page and the gap table cite the vendored Ossie revision**, so re-vendoring forces a review of the claims.
    (codex)
21. rejected (codex): a compatibility spec instead of `skip_specs` — no product behaviour changes; the invariants are
    test rules; a forms gap is fixed under the existing "Deprecated input forms convert with a structured warning".
22. rejected (codex): testing tripwire timing at `2.0.0.dev0` versus `2.0.0` — nothing relies on the bump; the
    tripwire already fires at major 2 and `main` has no 2.0-tagged site.
23. rejected (codex): asserting the wheel reports 2.0.0 — `scripts/release_gate.py` already checks `slayer --version`
    against the wheel's version.
24. rejected (codex): committing release notes — owner rule: `RELEASE_NOTES_*.md` stay untracked.
25. rejected (codex): running every suite against the installed wheel — CI runs the full suite on the same commit; the
    gate stays a smoke test of the packaged install.
26. **The Layer 1 xfails are built from DEV-2446's REQUIRED-construct manifest** (`tests/_dev2446_required_manifest.py`):
    each gap row gains `value=` (Ossie's result on the manifest's fixture row) and `fails_with=` (the error it raises
    today), and becomes one strict xfail per database; there is no separate Layer 1 gap table. The DEV-2244 cases
    stay in a DEV-2220 table. Alternative, a DEV-2220 Layer 1 table cross-checked against the manifest:
    two hand-kept lists of the same gaps. (user)

## Risks / Trade-offs

- [A prerequisite merges with a guide section or pin missing] → the guide audit and the catalogue run against the
  final `main`; gaps are written here.
- [The gap list is measured before DEV-2446 and the other prerequisites land] → re-measure at implementation, after
  they merge; DEV-2446's cases start as strict xfails pointing at it and must be gone (fixed) before this slice merges.
- [Re-vendoring the corpus changes REQUIRED constructs] → the cited revision plus the page-sync test force a review.
- [The catalogue duplicates per-slice deprecation tests] → accepted: it is the single place proving the whole Part 2
  surface until 3.0, and the 3.0 removal deletes it with the forms.
