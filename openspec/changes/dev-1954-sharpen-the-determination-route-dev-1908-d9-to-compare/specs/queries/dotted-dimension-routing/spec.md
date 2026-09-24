## MODIFIED Requirements

### Requirement: Non-routing resolution is unchanged
A directly-joined dotted path, a self-prefixed root reference, and a full explicit path SHALL resolve
exactly as before, with byte-identical result keys whenever the typed path is already canonical (every
unnamed edge, and every named edge spelled by its name). A fully valid path whose terminal column does not
exist SHALL keep failing as an unknown reference, not as a routing failure.

#### Scenario: Existing full and self-prefixed paths are unchanged
- WHEN a query uses a full explicit join path or a self-prefixed root column, spelled canonically
- THEN resolution and result keys are identical to prior behaviour

## ADDED Requirements

### Requirement: Stale path spellings resolve across stage boundaries
A flat name referenced across a stage boundary — by a downstream stage of a multistage query, by a query
against a query-backed model, or by another model's SQL referencing a query-backed model's columns — that
matches no column exactly SHALL bind to the upstream column whose name it would be under a non-canonical
spelling of that column's path (a named hop spelled by its target model), provided exactly one column
matches. Each such binding SHALL surface one typed `STALE_PATH_SPELLING` normalization warning per
referencing position, naming the stale and the canonical name. An exact name always wins; a name matching
several columns' respellings SHALL fail as an unknown reference.

#### Scenario: A downstream stage written against the old spelling keeps working
- **WHEN** a multistage query's first stage selects `customers.regions.rname` (now answered as
  `customers__hr__rname` downstream) and its second stage references `customers__regions__rname`
- **THEN** the reference binds to `customers__hr__rname`, the results equal the canonical spelling's, and
  the response carries a `STALE_PATH_SPELLING` warning naming both names

#### Scenario: A stale spelling survives a chain of stages
- **WHEN** a three-stage query's first stage selects `customers.regions.rname` and the second and third
  stages both reference `customers__regions__rname`
- **THEN** both bind to `customers__hr__rname`, each with a `STALE_PATH_SPELLING` warning

#### Scenario: A query-backed model's consumer written against the old spelling keeps working
- **WHEN** a query-backed model's query selects `customers.regions.rname`, and both a query on that model
  and another model's `Column.sql` reference its column `customers__regions__rname`
- **THEN** both resolve to the canonical column with a `STALE_PATH_SPELLING` warning

#### Scenario: An edge named later does not break an existing reference
- **WHEN** a downstream reference `customers__regions__rname` was written while the `customers`→`regions`
  edge was unnamed, and the edge is later named `hr`
- **THEN** the reference still resolves, with a `STALE_PATH_SPELLING` warning

#### Scenario: An ambiguous stale spelling fails closed
- **WHEN** a stale flat name matches the respelling of two different upstream columns
- **THEN** the reference fails as an unknown reference

#### Scenario: A cached result carries the requesting query's warnings
- **WHEN** a result is served from the cache for a query that uses a stale spelling after the canonical
  spelling populated the cache (or the reverse)
- **THEN** the response carries exactly the requesting query's `STALE_PATH_SPELLING` warnings
