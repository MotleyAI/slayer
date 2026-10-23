## 1. Plan and reproduce

- [x] 1.1 Refresh current upstream and public issue/PR history; report the wrong-result behavior in #507, distinguishing the known DEV-1918 traversal gap.
- [x] 1.2 Review the relation-boundary design independently, preserving flat ordinary stages and demand splicing without a normative architecture edit.
- [x] 1.3 Record failing SQLite/DuckDB segmented-total regressions before production changes.

## 2. Implement and document

- [x] 2.1 Preserve declared joins through saved-relation placeholder, root/extension and sibling/target materialization.
- [x] 2.2 Verify logical-key traversal, aggregate attribution, extension compatibility, ordinary-stage isolation and cycle behavior.
- [x] 2.3 Clarify saved-relation joins in the user models guide.
- [x] 2.4 Treat an extension join repeating a stored join as that edge for every model kind, in the runtime overlay and the schema-drift stage graph.

## 3. Validate and review

- [x] 3.1 Pass the focused regressions and related query-backed scope/cycle tests.
- [x] 3.2 Run the full documented unit selection, retest environment/timing failures, and pass lint, type and architecture gates.
- [x] 3.3 Independently review the implementation and verify the final tree.

Validation note: the local Python 3.14 unit run reported 31,377 passed, 99 skipped, 7 xfailed and three failures. All three passed isolated retries; the Poetry PATH was corrected for the affected subprocess test, and the timing-sensitive failure also reproduced on unchanged upstream. The documented integration selection passed with 656 passed, 36 skipped and 1 xfailed. CI validates the full suites on Python 3.11 and 3.14.
