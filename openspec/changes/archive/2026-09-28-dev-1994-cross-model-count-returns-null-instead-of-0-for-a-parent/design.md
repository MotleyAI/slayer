## Context

Every aggregate that needs its own rows compiles as a producer attached by a null-safe
LEFT JOIN (sql P10). An absent producer row means "no home rows for this cell", but
nothing maps that to the aggregation's empty-set value. The render side has no single
attach door: the combined phase hands `(cte, col)` tuples from `placeholder_to_cm`
(`slayer/sql/generator.py`) to five consumers (projection, outer composites, outer
WHERE, ORDER BY, transform chain); the row phase fills `attached_env` at three sites,
read through `ScopeFrame._anchor` (`slayer/sql/scope.py`). The windowed and association
kernels, multi-hop, and re-aggregation carriers are all instances.

Probe (chain fixture, SQLite = DuckDB): every count shape returns NULL for the childless
parent; the filter `count(orders.id) = 0` returns no rows; the re-aggregation carrier
holds the childless cell but reads NULL through the attach, so
`avg(count(orders.id, partition_by=[name]))` is 4/3 instead of 1.0.

## Goals / Non-Goals

**Goals:** make an attached aggregate total over the population's cells with its empty
value, via one planner decision and one renderer door, so a new consumer or attach site
cannot reintroduce the bug.

**Non-Goals:** an `Aggregation.empty_value` model field (custom count-like aggregations
stay NULL — decided); changing transform semantics for absent cells (`time_shift`,
`lag`, `change` of a missing prior bucket stay NULL); fabricating population cells.

## Decisions

1. **Empty value, one definition.** A function next to `INTEGER_AGGREGATIONS` in
   `slayer/core/enums.py`: 0 for `count` / `count_distinct` / `count_distinct_approx`
   (star included), None otherwise.
2. **Decided by the planner, carried as typed IR.** `RegroupSubstitution`
   (`slayer/ir/planned.py`) gains a **required** `empty_value: Optional[int]` (no
   default), so every synthesiser — cross-model (plain / windowed / associate / ranked),
   local regroup, re-aggregation producer and carrier, ORDER-BY wrap — decides
   explicitly. 0 only when the substituted value is an `AggregateKey` whose aggregation
   is a built-in count-family name **and** the owning model resolves no definition with
   a non-None `formula` (a formula-less declaration means "use the built-in"). The
   owning model comes from the existing owner resolution (`_resolve_agg_owner` in
   `slayer/engine/binding.py`, mirrored in `slayer/sql/generator.py`), lifted into one
   shared helper the planner calls — never the producer root. Transforms and every other
   key → None. Alternative rejected: renderer sniffing `original_key.agg` at render time
   (engine P6: placement from typed plan data, never key shape at render time).
3. **One attached-value record in the renderer.** The combined phase's `(cte, col)`
   tuple becomes a typed record carrying `cte_name`, `column_name` and `empty_value`
   with one method building the value expression (`COALESCE(<col>, <empty>)` as sqlglot
   AST when `empty_value` is not None, the bare column otherwise). Value consumers
   (projection value, outer composites, outer WHERE, ORDER BY, transform-chain inputs)
   read only the method; metadata consumers (dedup, public aliasing, join wiring,
   transform forwarding, order resolution) keep reading the names. The row phase's
   three `attached_env` sites and nested attaches inside producers build their entry
   through the same record. Alternative rejected: COALESCE only at the final projection
   (breaks position parity; filters, composites and re-aggregation carriers still see
   NULL).
4. **Shifted attaches unaffected** by construction: they carry no substitutions, and a
   transform's empty value is NULL.

### Normative harness edits (approved wording, land with the implementation)

`architecture/semantics.arc42.md` Axiom 4:
```diff
    so no fan-out can multiply its inputs (spec: `queries/semantics` › No double
-   counting). [enforced: test:tests/test_dev1836_producer_execution.py]
+   counting). A cell with no home rows takes the aggregation's **empty value**:
+   0 for the count family, NULL otherwise.
+   [enforced: test:tests/test_dev1836_producer_execution.py]
+   [enforced: test:tests/test_dev1994_empty_value.py]
```

`architecture/semantics.arc42.md` Axiom 10:
```diff
-    the population supplies the row set, a cell an operand lacks contributes
-    NULL, and combining never removes rows (spec: `queries/semantics` ›
+    the population supplies the row set, a cell an operand lacks contributes
+    its empty value (Axiom 4; NULL for a transform), and combining never removes rows (spec: `queries/semantics` ›
```

`architecture/sql.arc42.md` P10:
```diff
     as a producer (a plan-shaped CTE rooted where its rows live), attached
-    back by a null-safe LEFT JOIN on its complete grain and substituted into
+    back by a null-safe LEFT JOIN on its complete grain — an absent row reading
+    as the aggregate's empty value (semantics Axiom 4) — and substituted into
     expressions by structural identity, never text. [review]
```

Principles obeyed: semantics Axioms 4, 6, 10, 12, 13 and Law 4 (position parity);
engine P4, P6; sql P1 (AST-built COALESCE), P7/P10 (one attach door), P12
(cardinality-neutral: COALESCE never changes row count).

## Risks / Trade-offs

- [Result values change: NULL → 0 for count-family cells, and re-aggregations over them
  now include the zeros] → intended (spec scenarios pin both); existing goldens/tests
  asserting the NULL are re-blessed only where they assert this exact bug.
- [A synthesiser stamps the wrong value] → required field forces an explicit decision
  at every site; executed scenarios cover each synthesiser path.
- [Override resolved against the wrong model] → shared owner helper; test with the
  override on a model other than the producer root.
