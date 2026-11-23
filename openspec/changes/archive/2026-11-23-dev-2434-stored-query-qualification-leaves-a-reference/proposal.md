## Why

A pre-2.0 query that reads a population name equal to one of its own aliases converts to the wrong qualified spelling. In 2.0 a bare name is an alias, so after conversion the reference reads the alias. A stored stage with `sum(rev)` aliased `rev` over `rev_f` refers to itself and fails. A query that reads a population metric `revenue` beside a metric aliased `revenue` (`revenue * 10`) stops answering, where 1.x returned the population metric. This blocks 2.0: a 1.x store must load in 2.0.0 as it loaded in 1.x.

Both converters, the stored-query upgrade and the live root-relative conversion, keep a bare name bare whenever it matches an alias. The pre-2.0 reader decided by position instead, as probes of v1.9.1 and of the last pre-qualified main show:
- inside an entry, the population's names came first;
- in filters and order, a population field came first, then the query's alias, then a population metric.

## What Changes

- A bare name in a pre-2.0 query reads what it read before 2.0, decided by one lookup order shared by both converters:
  - inside a metric, dimension or time-dimension entry, or the main time dimension: the population's field, else its metric, else an alias of the query other than the entry's own;
  - in a filter or order: the population's field, else an alias of the query, else the population's metric.
- A metric alias equal to a population field, read in a filter (refused before 2.0), reads the population's field.
- A query-backed population, or an earlier stage, offers as fields the output columns of its stage, so the lookup order applies to it before its fields are expanded.
- Stored documents converted this way are written back like other migrated documents; memories convert on each load, as before.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `models/stored-upgrade`: "Stored queries are upgraded to qualified references" states the positional reading of a bare name.
- `queries/references`: "Pre-2.0 root-relative spellings convert when a population is named" replaces "a bare name that is not an alias of the same query becomes `P.<name>`" with the same positional reading.

## Impact

- `slayer/storage/qualified_references.py`: stored-stage reference qualification.
- `slayer/engine/legacy_references.py`: live root-relative conversion; `AnchorNames` separates field and metric names and covers stage and query-backed populations.
- `slayer/core/query.py`: the shared bare-name reading.
- An engine module the storage converter already imports: the static output column names of a stage.
- `docs/upgrading/2.0.md`: the stored-upgrade and root-relative paragraphs.
- No API, wire-format or stored-version change. No architecture model change.
