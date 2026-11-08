# Tasks: dev-1857-living-architecture-scaffold-likec4-arc42-import-linter

Flow note: spec-tests stage is skipped for this change (tooling/docs only, user decision);
`tests/test_arch_check.py` is written here, in spec-implement.

## 1. Verifier tooling

- [x] 1.1 Add basedpyright + import-linter to `[tool.poetry.group.dev.dependencies]`
      (`poetry add --group dev`); verify `poetry run basedpyright --version` and
      `poetry run lint-imports --help` succeed and `poetry.lock` is updated.
- [x] 1.2 Add `[tool.basedpyright]` per design D7 (basic mode, include slayer/tests/tools,
      exclude generated pb2); write and commit `.basedpyright/baseline.json` via
      `--writebaseline`; verify `poetry run basedpyright` exits 0 at baseline.
- [x] 1.3 Smoke-verify the basedpyright gate: inject a deliberate type error → non-zero exit;
      remove it → 0. Nothing committed from this step.
- [x] 1.4 Re-measure violations by running `lint-imports` with the D3 contracts and empty
      ignores; confirm the failing edge set equals design D3 (15 layers + 2 forbidden), then
      add the exact `ignore_imports` lists; verify `poetry run lint-imports` exits 0 and that
      removing any one ignore entry makes it fail (unmatched alerting smoke: add a bogus
      entry → fails).

## 2. Architecture scaffold

- [x] 2.1 Write `architecture/model/slayer.c4` per design D1/D4/D5 (9 nodes, sql children,
      #virtual tags, 37 flat relations, 4 #legacy tags); verify with the pinned LikeC4 CLI
      (D8) and pin the exact working invocation.
- [x] 2.2 Write `architecture/views.c4` (landscape + query-pipeline views); verify the pinned
      CLI still validates the project.
- [x] 2.3 Write `architecture/index.yaml` per design D2 (nodes, claims, children, contracts
      baselines layers=15/forbidden=2, cross_cutting_specs for queries/models/aggregations);
      verified by 3.1 (arch_check green).
- [x] 2.4 Write `architecture/system.arc42.md`: root narrative, global principles promoted
      from CLAUDE.md (sqlglot-AST-only, async-first, cardinality invariant, dotted-canonical
      paths, …) tagged `[enforced: …]`/`[review]`, the D5 authoring convention, the pinned
      LikeC4 invocation, and the enforcement-bundle commands; verify every `[enforced:]` id
      resolves (checked by arch_check in 3.1).
- [x] 2.5 Write `architecture/sql.arc42.md` (purpose, view pointer, principles incl.
      dialect-quirks-only-in-dialects, rationale linking archived openspec change ids);
      verified by 3.1.
- [x] 2.6 Add the CLAUDE.md pointer line (architecture/ + enforcement bundle); verify the
      conventions one-liners are untouched.

## 3. Cross-walk checker

- [x] 3.1 Write `tools/arch_check.py` implementing design D6 checks 1–9; verify
      `poetry run python tools/arch_check.py` exits 0 on the scaffold.
- [x] 3.2 Write `tests/test_arch_check.py` (tmp-dir fixtures; negative cases per D6: unclaimed
      module, duplicate claim, missing arc42, unknown touches node, baseline exceeded,
      missing/extra element, relation drift); verify
      `poetry run pytest tests/test_arch_check.py` passes.

## 4. DECISIONS.md fold

- [x] 4.1 Build the categorized inventory of all DECISIONS.md entries per design D9
      (arc42-principle / in-openspec-corpus / history-only); deliver it in the session for
      user review.
- [x] 4.2 Fold the arc42-principle entries into system/sql arc42 files; grep the repo for
      `DECISIONS.md` references and update them; verify grep returns no stale references.
- [x] 4.3 With explicit user confirmation in-session, delete `DECISIONS.md`; verify arch_check
      and the full gate bundle stay green.

## 5. Convergence

- [x] 5.1 Run the full enforcement bundle: `poetry run lint-imports`,
      `poetry run python tools/arch_check.py`, pinned LikeC4 validation,
      `poetry run basedpyright`; all green.
- [x] 5.2 Run `poetry run pytest -m "not integration"` and
      `poetry run ruff check slayer/ tests/`; all green; verify `git diff --stat` shows no
      changes under `slayer/` or `.github/workflows`.
- [x] 5.3 With user go-ahead: commit (adding each new file by name), push, open the PR.
