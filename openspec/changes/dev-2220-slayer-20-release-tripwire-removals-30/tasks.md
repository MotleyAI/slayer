## 0. Preconditions

- [x] 0.1 Merge `origin/main` once DEV-2441, DEV-2434, DEV-2444, DEV-1760, DEV-2446, DEV-2437, DEV-2428, DEV-2430,
      DEV-2443, DEV-2439, DEV-2455 and DEV-2246 have all merged; verify each is Done in Linear and its PR is merged. Until then, tests that depend
      on them (DEV-2446's cases) start as strict xfails pointing at the open issue; none may remain at merge.
- [x] 0.2 Before writing tests, merge `origin/main`, then read the plan of every prerequisite not yet merged (DEV-2443
      included): its OpenSpec change (proposal, design, tasks, delta specs) in its worktree under
      `slayer.worktrees/egor__dev-<n>-…` or on its PR branch. Fold what each plans into this slice's tests: new
      deprecated forms (catalogue cases and pins), new refusals or Layer 1 changes (gap pins), and the
      migration-guide sections it promises (for the 4.1 audit). Verify: a DEV-2220 comment lists each open
      prerequisite with what it contributes, or "nothing".

## 1. Compliance pins (tests first)

- [x] 1.1 Re-measure the Layer 1 gaps against the code (every REQUIRED construct of the vendored
      `core-spec/expression_language.md`, revision `d4dca4e9`), on DuckDB and SQLite; the earlier throwaway probe
      (218/290 passing, 23 Nov) is a starting point, not the answer. Verify: the gap list in the test is exactly the
      constructs that fail today.
- [x] 1.2 Write the gap-case table: one case id per independently fixable behaviour, each asserting Ossie's result,
      one pytest parameter per (case, database), each `xfail(strict=True, raises=<its current error>,
      reason=<DEV-2282 | DEV-2244>)`. Verify: the file runs green with every case xfailing for the
      declared error.
      - Layer 1 cases are the gap rows of DEV-2446's `tests/_dev2446_required_manifest.py` (decision 26); the test
        cannot collect until DEV-2446 is on `main`. After task 0.1's merge, add `value=` and `fails_with=` to every
        gap row. Verify: every Layer 1 case xfails for its declared error on DuckDB and SQLite.
- [x] 1.3 Add the DEV-2244 format-gap cases. Verify: they xfail with the declared error.
- [x] 1.4 Write the page-sync test: the page's gap-table rows (case ids) equal the xfail table, both
      directions. Verify: it fails when a row or a case is removed alone (check once by hand, then restore).

## 2. Ossie compliance page

- [x] 2.1 Write `docs/interfaces/ossie-compliance.md`, conservatively worded, citing the vendored revision and naming
      the databases CI executes each claim on (others "not certified"): Layer 1 soundness plus the refused-constructs
      table; Layer 3 under `conformance="ossie-l3"`, superset native language, output names only under the switch,
      bare model-scoped metrics, no known deviation; semantic-model format (what passes on the corpus and grader,
      `slayer ossie-roundtrip`, DEV-2244 rows, wider support "intended"); "Layer 2 not claimed", naming no issue.
      Verify: 1.4 passes; `tests/test_doc_examples.py` passes.
- [x] 2.2 Link it from the `nav` in `zensical.toml` and from `docs/interfaces/ossie-layer-3.md`. Verify: the docs nav
      check passes.

## 3. 3.0 deprecation catalogue

- [x] 3.1 Inventory every Part 2 form of `docs/upgrading/2.0.md` and every `PINNED` key; list each form's surface
      (dataset/query/refinement/extension keys, datatype values, root-relative references, REST routes, CLI commands
      and flags, MCP arguments, search vocabulary, Python names/attributes/methods/errors, storage hooks, Mode-B
      spellings, `strict`, extension object in `source_model`, field `sql`). Verify: the inventory is the catalogue's
      case table.
- [x] 3.2 Write the catalogue test: each case converts one form on its surface and asserts the canonical output, one
      `FutureWarning`, and the structured warning by kind (`deprecated_input` / `deprecated_spelling`, `removal`
      "3.0"); stored documents convert silently (the deliberate exception, its own cases). Verify: green.
- [x] 3.3 Each case names the `PINNED` key it exercises; add a test that every `PINNED` key except the moved-module
      stubs has a case. Verify: green, and fails if a case is dropped.
- [x] 3.4 Any form found converting without its warning, unpinned, or to the wrong canonical form: add the warning and
      pin through the existing deprecation helpers, or fix the conversion (today a reference object
      `{"name", "dataset"}` under the deprecated time-dimension `dimension` / order-item `column` keys drops its
      dataset). Verify: 3.2 and `tests/test_law_removal_tripwire.py` pass.

## 4. Migration-guide audit

- [x] 4.1 Check `docs/upgrading/2.0.md` against every merged 2.0 slice and prerequisite (DEV-1695, DEV-2398, DEV-2264,
      DEV-2136, DEV-1914, DEV-2385, DEV-2018, DEV-2388, DEV-2217, DEV-2243, DEV-2394, DEV-2266, DEV-2255, DEV-2249,
      DEV-2441, DEV-2434, DEV-2444, DEV-1760, DEV-2446, DEV-2437, DEV-2428, DEV-2430, DEV-2443, DEV-2439, DEV-2246): each breaking change has a
      Part 1 before/after, each deprecation a Part 2 entry. Write what is missing. Verify: a checklist of the slices
      against guide sections, all ticked; `tests/test_doc_examples.py` passes.
- [x] 4.2 List the moved modules `slayer.engine.syntax`, `slayer.core.formula`, `slayer.osi.expression` with their
      replacements. Verify: present in the guide.

## 5. Version and sweep

- [x] 5.1 Set `pyproject.toml` to `2.0.0`. Verify: `poetry version -s` prints `2.0.0`; the removal tripwire passes.
- [x] 5.2 Sweep README and `docs/` for removed or renamed terms (identifier guard plus a read). Verify: the guard and
      `tests/test_doc_examples.py` pass.

## 6. Release

- [x] 6.1 Draft `RELEASE_NOTES_2.0.0.md` with the `release-notes` skill from the slice comments and PRs; keep it
      untracked. Verify: `git status` shows it untracked and unstaged.
- [x] 6.2 After task 0.1, run the DEV-2205 release gate on a wheel built from this branch, installed with every
      dependency at its newest version. Verify: the gate passes.
- [x] 6.3 Check upstream apache/ossie `compliance/portability`'s current runner interface and post SLayer's drafted
      registration (`slayer ossie-roundtrip <in> <out>`, every element PORTABLE) as a DEV-2220 comment. Verify: the
      comment exists. Opening the upstream PR waits for 2.0.0 on PyPI and the owner's go-ahead.
- [x] 6.4 Once the 2.0 surface is final, propose the Storyline 2.0 migration issue to the owner (search Linear first);
      file it only on an explicit yes.
- [ ] 6.5 Tag and publish 2.0.0 only after merge and with the owner's explicit go-ahead.

## 8. Review fixes

- [x] 8.1 Refuse `save_datasource` while the stored datasource has a pending reserved-name granularity rename
      (decision 27), tests first on YAML and SQLite. Note the refusal in `docs/upgrading/2.0.md` beside the rename
      entry. Verify: the new scenarios pass and fail without the fix.
- [x] 8.2 Thread 13 / nitpick test alignments (approved): the NULL-placement probe ranks every row; the MCP edit
      cases assert the canonical edit took effect. Verify: both files green.

## 7. Gates

- [x] 7.1 Run the local test scope (`la-affected-tests` plus the new tests and `tests/test_doc_examples.py`), ruff,
      basedpyright against the baseline, and `la-arch-check`. Verify: all green.
