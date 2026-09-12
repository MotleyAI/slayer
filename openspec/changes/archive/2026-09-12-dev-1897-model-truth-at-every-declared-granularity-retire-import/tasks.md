# Tasks

## 1. Failing test suite (spec-tests stage)

- [x] 1.1 Rewrite `tests/test_arch_check.py` fixture repo for the new index schema
      (`root_package`, `legacy_arrows`, `children:`, no `[tool.importlinter]`) with declared
      children and child arrows; verify the healthy fixture passes `run_checks` once
      implemented (fails now).
- [x] 1.2 Law tests — attribution: longest-prefix child attribution (incl. a grandchild
      fixture), undeclared-module→node, relative imports, `from pkg import submodule`,
      re-exported attribute vs child-name collision, TYPE_CHECKING exclusion at child
      level, root `__init__` exemption; verify each via targeted `model-truth` findings.
- [x] 1.3 Law tests — coverage: sibling child→child ban (missing-arrow with module→module
      witness text), ancestor↔descendant internality, parent arrow covering child cross-node
      edges, most-specific selection, one-sided-specific incomparable covers both live,
      fully-shadowed arrow dead, plain dead arrow, node-level reduction (no-children fixture
      behaves exactly as today); verify via findings lists.
- [x] 1.4 Ratchet + schema tests: legacy count == baseline both directions; malformed
      `legacy_arrows` (missing/non-integer/negative), unknown/duplicate child paths, child
      on virtual node, child missing on disk, malformed `view_depth` (non-integer, unknown
      view id) — all findings, never exceptions.
- [x] 1.5 Parser tests (`test_arch_diagrams.py`): dotted FQN relation endpoints, FQN element
      identity with identical leaf names under different parents, unknown-FQN endpoint
      findings; fixture pyproject loses `[tool.importlinter]`.
- [x] 1.6 View/render tests: depth collapse relative to view tops (view_depth override and
      default 3), edge roll-up with dedupe + self-drop + dashed-iff-all-legacy, focus
      predicates matching by top-level ancestor and pulling ancestor chains, subgraph
      mermaid golden (nested subgraphs, `.`→`__` id mangling, leaf labels, classDef,
      deterministic order), `diagrams-fresh` byte-for-byte on the new output.
- [x] 1.7 `tests/test_import_law_parity.py`: frozen old law (layer order, forbidden, 7 door
      literals matched as prefix pairs at declared-child granularity, per design D7),
      cross-node domain only; assertions (1) banned-old ⇒ banned-new and (2)
      allowed-old ⇒ allowed-new for doors still declared; runs against the real repo via
      `arch_check.license`.
- [x] 1.8 Real-repo gate tests: `run_checks(REPO_ROOT) == []` plus no-importlinter-in-
      pyproject, CI-runs-arch_check, and no-lint-imports-in-arc42 pins (green now while
      checker and repo are both on the old law where applicable; red once the new checker
      lands; all green again at cutover).

## 2. arch_check implementation

- [x] 2.1 Read `root_package` and `legacy_arrows` from index.yaml; delete
      `_pyproject_importlinter`, `contracts-known`, old `_check_baselines`; retarget
      `baseline-ratchet` to the legacy-arrow count; verify 1.4 passes.
- [x] 2.2 Children validation (on-disk dotted paths, non-virtual single-package nodes,
      model↔index FQN agreement) extending claims/model-identity; verify 1.4/1.5 pass.
- [x] 2.3 Extend measurement to child attribution and implement coverage/most-specific/
      dead-arrow with witness-bearing findings; expose `license(src, dst)`; verify 1.2/1.3
      and 1.7 (harness half) pass.
- [x] 2.4 `enforced-tags`: drop contract ids, keep `arch_check:`/`test:`; verify tag tests
      pass.

## 3. arch_diagrams implementation

- [x] 3.1 FQN element identity + dotted relation endpoints in the parser; verify 1.5 passes.
- [x] 3.2 Per-view depth expansion/collapse, edge roll-up, predicate matching by ancestor;
      verify 1.6 view tests pass.
- [x] 3.3 Subgraph mermaid rendering with mangled ids, classDef styling, deterministic
      ordering; verify 1.6 goldens and `diagrams-fresh` pass.

## 4. Cutover (model, index, pyproject, CI)

- [x] 4.1 Edit `architecture/model/slayer.c4` per design D6 (children + arrows, node legacy
      arrows removed) — **user approval for the exact diff first**; verify
      `npx -y likec4@1.47.0 validate architecture` green.
- [x] 4.2 Edit `architecture/index.yaml` (children, `legacy_arrows: {baseline: 8}`,
      `root_package`, header comment) — **user approval first**; verify arch_check schema
      checks green.
- [x] 4.3 Delete `[tool.importlinter]` and the `import-linter` dev dependency; run
      `poetry lock` and `poetry install`; verify `poetry run pytest` collects and
      `grep -r importlinter pyproject.toml` is empty.
- [x] 4.4 Add the arch_check step to `.github/workflows/ci.yml` lint-and-test; verify by
      running the step's command locally.
- [x] 4.5 Regenerate diagrams (`poetry run python tools/arch_diagrams.py`); verify
      `run_checks(REPO_ROOT) == []` (task 1.8 green) and the parity harness (1.7) green.

## 5. Normative docs (each diff individually user-approved)

- [x] 5.1 `system.arc42.md`: §3 P1/P2 retag + reword, P5 legacy-arrow ratchet wording
      (review owns monotonicity), §4 bundle minus lint-imports + CI note, §5 dotted
      endpoints/children/`from pkg import name` note/depth knob/subgraphs; verify
      enforced-tags + diagrams-fresh green.
- [x] 5.2 `ir.arc42.md` P2 retag; `core.arc42.md` §1 door wording; verify enforced-tags
      green.
- [x] 5.3 Full gate: `poetry run ruff check slayer/ tests/ tools/`, full non-integration
      suite, enforcement bundle (arch_check, likec4 validate, basedpyright no-new-errors);
      verify all green.

## 6. Out-of-repo sweep

- [ ] 6.1 (at merge — spec-review) Update `~/.claude/skills/{living-architecture,arch-slice,
      spec-review,spec-plan}` and deterministic-refactor docs to the one-law regime (diffs
      shown to user); verify `grep -rn "lint-imports" ~/.claude/skills` shows only intended
      references. Out-of-repo, so not part of the PR diff; lands alongside the merge.
