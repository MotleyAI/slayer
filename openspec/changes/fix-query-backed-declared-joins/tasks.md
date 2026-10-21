## 1. Plan and reproduce

- [x] 1.1 Refresh current upstream and public issue/PR history; report the wrong-result behavior in #507, distinguishing the known DEV-1918 traversal gap.
- [x] 1.2 Review the relation-boundary design independently, preserving flat ordinary stages and demand splicing without a normative architecture edit.
- [x] 1.3 Record failing SQLite/DuckDB segmented-total regressions before production changes.

## 2. Implement and document

- [x] 2.1 Preserve declared joins through saved-relation placeholder, root/extension and sibling/target materialization.
- [x] 2.2 Verify logical-key traversal, aggregate attribution, extension compatibility, ordinary-stage isolation and cycle behavior.
- [x] 2.3 Clarify saved-relation joins in the user models guide.

## 3. Validate and review

- [x] 3.1 Pass the focused regressions and related query-backed scope/cycle tests.
- [ ] 3.2 Pass the full documented unit, lint, type and architecture pre-push gates.
- [x] 3.3 Independently review the implementation and verify the final tree.
