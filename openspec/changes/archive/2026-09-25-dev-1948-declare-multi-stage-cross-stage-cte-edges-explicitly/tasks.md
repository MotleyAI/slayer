## 1. Failing tests (spec-tests stage)

- [x] 1.1 `queries/multi-stage` execution tests (SQLite + DuckDB, hand-computed values): the `[x, c(src x), b(joins c), root]` list in every non-root permutation; a stage reaching a sibling through an inline model's joins (joins only; nested `source_queries` covered in 1.2 at the ordering level); one stage reading three siblings; strict-xfail execution tests naming DEV-1966 for a stored join to model `customers` beside a sibling named `customers` resolving to the model, and a depth-2 nested `source_queries` sibling read. Verify: red today (the chain shape fails with `target 'c' not in source bundle`).
- [x] 1.2 `plan_stages` tests: returned order equals `topologically_order_stages`; `stage_reads` correct (root included, plan-ordered) for every shape in 1.1 plus a producer rooted at a sibling and a semi-join (`EXISTS`) hop over a sibling; `[named, unnamed_root]` accepted; unnamed non-root, self-reference, root-referenced and cycle rejected. Verify: red today (no `stage_reads`, source-only sort).
- [x] 1.3 Declared-edge tests on captured `assemble_with_chain` entries: each stage relation declares its hoisted CTEs, its reuse deps and its `stage_reads`; every hoisted CTE and every root entry declares its statement's `stage_reads`; a later stage body reusing an earlier stage's shared producer declares that producer. Verify: red today.
- [x] 1.4 Precondition test: `generate_planned_stages` fed a planned list with a stage before a sibling it reads (root last) raises `ValueError` naming both. Verify: red today (currently emits SQL).

## 2. Implementation (spec-implement stage)

- [x] 2.1 Promote `stage_ordering._extract_sibling_refs` to public `stage_sibling_reads`; update its callers. Verify: `tests/test_topologically_order_stages.py` green.
- [x] 2.2 `plan_stages` orders via `topologically_order_stages` and sets `PlannedQuery.stage_reads` from one immutable all-sibling-names set; add the field to `slayer/ir/planned.py`. Verify: 1.1 and 1.2 green.
- [x] 2.3 Delete `compile/stages._topo_sort` and its two rows in `tests/_dev1871_raise_ledger.py`; switch `query_engine`'s warning alignment to `topologically_order_stages`. Verify: `tests/test_dev1871_raise_parity.py` and `TestTopoSort` green.
- [x] 2.4 `generate_planned_stages`: dependency-order precondition check; push/pop each stage `relation_name` on `_gen_split_consumers`; declare `stage_reads` on stage relations, their hoisted CTEs and root entries; reword `CteEntry.depends_on` to "CTEs that must precede this one". Verify: 1.3 and 1.4 green.

## 3. Verification

- [x] 3.1 Full unit suite (`poetry run pytest -m "not integration"`) and CI-mode integration suite green with every DAG / notebook golden byte-identical — any golden change is a stop-and-ask, never a re-bless.
- [x] 3.2 `poetry run python tools/arch_check.py` green (engine.plan → engine.stage_ordering licensed) and `poetry run ruff check slayer/ tests/` clean.
