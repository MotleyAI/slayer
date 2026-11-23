## Context

Two converters turn pre-2.0 query text into qualified references:
- `slayer/storage/qualified_references.py` (`_Stage._reference`) upgrades stored stages on load;
- `slayer/engine/legacy_references.py` (`convert_text`, `_converts_reference`, `_converts_argument`) converts fresh root-relative input.

Both keep a bare name bare whenever it matches an alias of the query. In 2.0 a bare name *is* an alias, so the converted text reads the alias wherever pre-2.0 read the population.

Probes of v1.9.1 and of main just before qualified references (`ac3de83e7^1`) established the pre-2.0 reading:
- Every release 0.10.2–1.9.1, and the dev line, refused a metric alias equal to a population field (`MeasureNameCollidesWithColumnError` / `MetricNameCollidesWithFieldError`), including the issue's own example. 1.9.1 dimensions and time dimensions had no alias. The dev line refused any dimension or time-dimension alias, and any computed-dimension name, equal to a population field or metric.
- The one clash pre-2.0 accepted is a metric alias equal to a population metric. Inside metrics the bare name read the population metric (`count(*)` aliased `revenue` with `revenue * 10` gave `sum(amount) * 10`). In filters and order it read the alias (`HAVING COUNT(*) > 1`).
- Metrics could not read a pure alias (`n * 10`: unresolved). `partition_by=` keys read computed-dimension names (1.x) and dimension aliases (dev line).
- The 1.x docs gave the reason for the refusal: an alias-form filter "would otherwise silently bind to the source column". The filter lookup order was therefore field, then query alias, then model metric.
- Stages with no population that filter on a metric alias were refused in 1.x too.

During stored qualification a query-backed population has no fields (`awaits_fields`). The live converter's `AnchorNames.members` covers only stored, expanded dataset populations, and lumps fields and metrics together.

Principles that apply:
- `architecture/system.arc42.md` §3.8, one reader of Mode B: the resolver works on `core.syntax` nodes and never re-tokenises.
- §3.2/§3.3, the import law and its ratchet: no new arrows.
- `architecture/python.storage.arc42.md` §3.3, versioned persistence: the upgrade runs on load, and migrated documents are written back as before.

No arc42 or `.c4` edit is needed.

## Goals / Non-Goals

**Goals:**
- One lookup order for a pre-2.0 bare name, shared by both converters:
  - entry positions: population field, then population metric, then an alias other than the entry's own;
  - filter and order positions: population field, then alias, then population metric.
- The order applies to every population: a stored dataset, a query-backed dataset before expansion, and an earlier stage.
- Every document pre-2.0 answered keeps its answer. The issue's refused clash loads and reads the population column.

**Non-Goals:**
- Population inference for stages with no population (1.x refused alias filters there too).
- Dotted references, `count(*)`, extension edges, keyword arguments and model-scoped metrics: their existing handling stays.
- Making a metric unable to read a pure alias (the literal pre-2.0 scope). See decision 6.

## Decisions

1. **Structural fix: one positional resolver for both converters (default).** A pure function in `slayer/core/query.py`, beside `declared_aliases`, returns *alias*, *population* or *unresolved*. Its inputs are the bare name and its quoted flag, the position (entry or filter/order), the query's aliases minus the entry's own, and the population's field and metric names. Each converter keeps its own action for *unresolved*: the stored converter prefixes the population; the live converter leaves the name for the binder to refuse. It also keeps its own rules for model-scoped metrics, keyword arguments and dotted references. Rejected: patching `_Stage._reference` alone. That leaves two hand-synced readings of the same 1.x text, which is the cause of this bug.
2. **The refused clash reads the population column (user).** A metric alias equal to a population field, read in a filter, reads the field: `rev >= 0` becomes `rev_f.rev >= 0` and `sum(rev)` becomes `sum(rev_f.rev)`. This is the pre-2.0 lookup order applied uniformly, and it is what the issue expects. Options weighed:
   - the filter reads the alias, as the 1.x docs said users meant (rejected: a second filter rule);
   - the document becomes unloadable (rejected: drops an expressible document).
3. **The live converter is in scope and shares the resolver (user).** Without it, the same 1.x text means different things typed and stored, and live `revenue * 10` stays broken. Options weighed: a separate issue, or leaving the live converter as specified.
4. **Unknown population fields are derived statically (user).** For a query-backed dataset awaiting expansion, or an earlier stage, a static helper derives the output column names a later stage reads, from that stage's last entries: the alias; a dimension's name with `__` flattening of its path; `expression_text_alias` for an unaliased metric, as `_qualified_metric` computes it. A law test asserts the helper equals the compiled stage schema's field names. Rejected: treating unknown fields as absent, so the alias wins. That contradicts decision 2 on the issue's own example.
5. **Every output column of a stage or query-backed population is a field (codex).** Metric outputs included. Otherwise the filter order (field, alias, metric) would bring the bug back over a metric-derived column.
6. **Inside an entry an alias is reachable in every argument, not only `partition_by` keys (user).** Today's stored `n * 10` keeps loading and answering. The literal pre-2.0 scope would make it read `orders.n` and fail. Options weighed: context-sensitive alias visibility via `core.syntax` (codex finding 1).
7. **Quoted names (codex).** The resolver takes each reference's quoted flag and matches every namespace with `match_name`: a double-quoted name matches only its exact spelling, and an ambiguous match keeps each converter's current treatment.
8. **Helper placement (codex).** The output-name helper needs `slayer.sql.naming.expression_text_alias`, so it lives in an engine module that `slayer/storage/qualified_references.py` already imports (`slayer.engine.extensions` or a sibling covered by the same arrows), not in `slayer.core`. A `core → sql` arrow would need a new edge. The pure resolver stays in `slayer.core`. `la-arch-check` confirms.
9. **The main time dimension counts as an entry position (default).** Both orders agree on every document pre-2.0 answered: a field that is also an alias was always refused. Rejected (codex finding 5): a separate position, which buys nothing for any loadable document.
10. **No-population stages are untouched (default).** 1.x refused their alias filters as well.
11. **The law test's cases (codex):** relationship paths with `__` flattening, unaliased computed dimensions, time dimensions with granularity, simple and composite unaliased metrics, explicit aliases, deduplicated projections, and anchorless entries with an injected alias. It compares the set of names; order and duplicates are not part of the law.

## Risks / Trade-offs

- [The static output names drift from the compiler's stage schema] → the law test of decision 11 pins them together.
- [A document pre-2.0 refused gains a reading (decisions 2 and 6)] → it is the pre-2.0 lookup order's reading, and no answered document changes.
- [`AnchorNames` gains a field/metric split and stage/query-backed members] → existing live-conversion tests cover the dataset path. An existing test that pins "the alias wins over a population field" is aligned only with the owner's approval.
