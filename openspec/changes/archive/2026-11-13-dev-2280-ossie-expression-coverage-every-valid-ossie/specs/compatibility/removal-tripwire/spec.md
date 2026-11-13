## MODIFIED Requirements

### Requirement: One marker for code scheduled for removal

Every piece of compatibility code scheduled for removal in a future major version SHALL be marked through one shared mechanism that records the removal version as a literal, and that mechanism SHALL be the only way such code is marked and the only emitter of `FutureWarning` in the package. A marked form that still works SHALL emit exactly one `FutureWarning` per use (a deprecated Mode-B spelling: per spelling per query or save), attributed to the line that used it, and the CLI SHALL print it as one `Deprecated: …` line on stderr; a marked module path that moved SHALL raise an `ImportError` instead; a marked informational notice SHALL emit no `FutureWarning` — it is carried only as its structured response warning. The covered workarounds SHALL be every module path moved in 2.0 by the database-access, live-schema, row-supply and cardinality carve-outs, the deprecated Mode-B spellings `==`, `is None` / `is not None`, bare `None`, a `[…]` list after `IN` and the method-call aggregation form (`queries/expression-grammar` › Legacy spellings keep their meaning), and the date-only comparison notice (`queries/time-points` › A date-only comparison that changed meaning is announced), each removed in 3.0. Stored-data upgrade paths, the colon aggregation spelling, the Flight pyarrow adapter modules and the pg facade's `token=` argument SHALL NOT be marked.

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
- **WHEN** a query triggers the date-only comparison notice
- **THEN** no `FutureWarning` is recorded, the response carries the notice, and the tripwire enumerates the notice's site with removal version 3.0
