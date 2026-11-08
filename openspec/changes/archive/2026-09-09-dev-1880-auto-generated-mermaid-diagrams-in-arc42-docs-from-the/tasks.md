# Tasks: Auto-generated mermaid diagrams from the LikeC4 model

## 1. Generator module

- [x] 1.1 `tools/arch_diagrams.py::parse_model(root)`: quote/comment-aware scanner for
  `model/*.c4` — elements (id, kind, title, parent, virtual), relations (src, dst,
  legacy), fail-closed findings (unrecognized line, relation in element body,
  duplicate element/relation, unknown endpoint, undeclared kind); verified by
  parse_model tests in `tests/test_arch_diagrams.py`.
- [x] 1.2 `parse_views(root)`: `views { view <id> { title/include } }` only; include
  forms `*`, bare top-level id, `x -> *`, `* -> x`; multi-line union, stable dedup;
  loud failures (unsupported line, unknown/child id, `* -> *`, duplicate view id);
  verified by parse_views tests.
- [x] 1.3 Mermaid emission: `flowchart TD`, `  %% <view_id>: <title>` comment line,
  declaration-order nodes/edges, `id["T"]`/`id("T")` shapes, `-->`/`-.->` edges,
  `"`→`#quot;` escaping, legend line iff legacy edge present; verified by exact-text
  emission tests.
- [x] 1.4 Marker rewriting + CLI (`python tools/arch_diagrams.py` rewrites mapped
  docs in place, prints changed files): rewrite strictly between
  `<!-- likec4:<id> -->` pairs, preserve all other bytes, `newline=""`; errors for
  missing/duplicate/unmatched markers; verified by rewrite + idempotence tests.

## 2. arch_check integration

- [x] 2.1 Consolidate: `arch_check` imports `arch_diagrams` via importlib; delete
  `_ELEMENT_RE`/`_RELATION_RE`/`_parse_elements`/`_parse_relations`; `model-identity`
  / `model-truth` consume the new parser with unchanged finding texts; verified by
  the existing `tests/test_arch_check.py` suite staying green.
- [x] 2.2 New check `diagrams-fresh` (in `CHECK_IDS`): mapped doc exists, mapped view
  ids exist, exactly one marker pair per mapped view id, byte-for-byte fresh content,
  orphan open/close markers across all arc42 docs, `diagrams:` schema; parse errors
  are findings, other checks still run; failure messages name
  `poetry run python tools/arch_diagrams.py`; verified by per-finding fixture tests.

## 3. Architecture content

- [x] 3.1 Add `core_focus` view (`title 'Core in context'`; `include core,
  core -> *, * -> core`) to `views.c4`; verify `npx -y likec4@1.47.0 validate
  architecture` passes.
- [x] 3.2 Add `diagrams:` block to `index.yaml` (system→landscape, core→core_focus,
  engine/sql→query_pipeline) with schema comment; verify `arch_check` reports only
  missing-marker findings until 3.3.
- [x] 3.3 Place marker pairs in §2 of the four mapped docs, adjust each "See the
  … view" sentence (one concise sentence), run the generator; verify `poetry run
  python tools/arch_check.py` exits 0 and diagrams render on GitHub markdown preview.
- [x] 3.4 `system.arc42.md`: §5 view-authoring + marker convention, §4
  `diagrams-fresh` note; verify `arch_check` enforced-tags still green.

## 4. Tests (written first, red, in the spec-tests stage)

- [x] 4.1 `tests/test_arch_diagrams.py`: parse_model, parse_views, emission,
  rewriting, per-finding `diagrams-fresh` coverage through `run_checks`, mutation
  tests (model edit, views edit incl. title-only → red naming the fix command), and
  the real-repo freshness test; verified red before implementation, green after.
- [x] 4.2 Update `tests/test_arch_check.py` fixtures/wiring for consolidation with
  assertions unchanged; verified by the full suite.

## 5. Final gate

- [x] 5.1 `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/
  tests/` (+ tools/), `poetry run lint-imports`, `poetry run python
  tools/arch_check.py`, `npx -y likec4@1.47.0 validate architecture`, `poetry run
  basedpyright` (no new errors) — all green.
