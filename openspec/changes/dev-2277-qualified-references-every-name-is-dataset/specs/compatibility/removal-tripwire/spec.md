## MODIFIED Requirements

### Requirement: One marker for code scheduled for removal

Every piece of compatibility code scheduled for removal in a future major version SHALL be marked through one shared mechanism that records the removal version as a literal, and that mechanism SHALL be the only way such code is marked and the only emitter of `FutureWarning` in the package. A marked form that still works SHALL emit exactly one `FutureWarning` per use (a deprecated Mode-B spelling: per spelling per query or save), attributed to the line that used it (for an input form, the first line outside SLayer that supplied it), and the CLI SHALL print it as one `Deprecated: …` line on stderr; a marked module path that moved SHALL raise an `ImportError` instead; a marked informational notice SHALL emit no `FutureWarning` — it is carried only as its structured response warning. The covered workarounds SHALL be every module path moved in 2.0 by the database-access, live-schema, row-supply and cardinality carve-outs, the Mode-B grammar's `slayer.engine.syntax` and `slayer.core.formula`, and the OSI importer's `slayer.osi.expression`; the `strict` query input form, the extension object in `population` and a field's `sql` input form (`models/field-expressions` › A field's sql converts to its expression); the deprecated Mode-B spellings `==`, `is None` / `is not None`, bare `None`, a `[…]` list after `IN` and the method-call aggregation form (`queries/expression-grammar` › Legacy spellings keep their meaning); the root-relative reference spellings converted in a query that names its population (a bare field or dataset metric, `count(*)`, and a path whose first segment is not a dataset, `queries/references` › Pre-2.0 root-relative spellings convert when a population is named); the date-only comparison notice (`queries/time-points` › A date-only comparison that changed meaning is announced); and the changed-reference notice (`queries/references` › A reference whose meaning changed announces it); each removed in 3.0. Stored-data upgrade paths, the colon aggregation spelling, the Flight pyarrow adapter modules and the pg facade's `token=` argument SHALL NOT be marked.

#### Scenario: Every covered workaround is marked
- **WHEN** the marked sites in the package are enumerated
- **THEN** they are exactly the pinned set of covered workarounds, each with its pinned removal version, and none of the excluded paths

#### Scenario: A marker without a literal removal version is rejected
- **WHEN** a marked site gives its removal version as anything but a string literal
- **THEN** the tripwire test fails naming that site

#### Scenario: A deprecation names its caller's line
- **WHEN** code calls a function that emits through the shared mechanism
- **THEN** exactly one `FutureWarning` is recorded, attributed to the calling line

#### Scenario: The CLI prints a deprecation on one line
- **WHEN** a deprecation is emitted while a CLI command runs
- **THEN** stderr carries it as exactly one `Deprecated: <message>` line

#### Scenario: A deprecated spelling used twice in one query
- **WHEN** a query has the filters `status == 'paid'` and `region == 'EU'`
- **THEN** it emits exactly one `FutureWarning` naming `=` and "SLayer 3.0"

#### Scenario: A marked notice emits no FutureWarning
- **WHEN** a query triggers the date-only comparison notice, and another triggers the changed-reference notice
- **THEN** no `FutureWarning` is recorded, each response carries its notice, and the tripwire enumerates both notices' sites with removal version 3.0

#### Scenario: A root-relative spelling used twice in one query
- **WHEN** a query with population `orders` measures `count(*)` and filters `count(*) > 1`
- **THEN** it emits exactly one `FutureWarning` naming `count(*)`, `count(orders.*)` and "SLayer 3.0"
