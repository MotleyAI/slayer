## Context

The public save path validates and persists joins after populating a query-backed model's output columns. Execution strips those joins from its placeholder, then creates a flat synthetic model from the final StageSchema. Product-hop support can turn the missing edge into a successful uncorrelated filter rather than a refusal.

## Decisions

1. Repair the saved-relation boundary. Preserve only the query-backed model's own declared joins; never inherit its backing source's join graph.
2. Reuse `model_from_stage_schema` to carry optional declared joins for saved final relation names in source, extension and sibling/target paths. Keep StageSchema unchanged. Validate joins against the saved relation's real identity so a legal target named `_stage` is not mistaken for a self-join.
3. Select ModelScope for a materialized saved relation by its identity in the query-backed model map, including relations with incoming reverse edges and no outgoing joins. Ordinary/private query stages retain flat StageSchema binding.
4. Keep demand-based splicing. Merely declaring or reaching an unread query-backed model must not plan it or manufacture a cycle.
5. Reuse the existing logical join-key and extension semantics. Test distinct/named extension joins. An extension join equal to one of its base model's stored joins in all but metadata and pair order is that stored edge, applied once, for every model kind; the runtime overlay and the schema-drift stage graph share this one rule. A join differing in target, name, join type, cardinality or pairs stays a separate edge.

## Validation

Record failing segmented-total tests before the implementation. Execute the repaired paths on SQLite and DuckDB using renamed physical/logical keys. Check root and target consumption, aggregate attribution, extension parity, private-stage scopes and cycles. Run the required unit, lint, type and architecture gates and the documented CI integration selection, then inspect current-head CI and review results.

## Risks

An incomplete root-only fix leaves target/sibling reads broken. Passing saved relationships to ordinary stages breaks stage isolation. Eager traversal of retained edges can introduce false cycles. Preserve those distinctions and use existing fixtures and regression suites.
