# ontologies/ontology-use Specification

## Purpose
Uses stored Ossie Layer 4 ontologies and mappings as context for agents (concept annotations in inspect and search) and as report-only checks of the semantic model and its data against what the ontology declares.

## Requirements

### Requirement: Mapped elements are annotated with their concepts
Every mapping of a datasource that resolves SHALL annotate the datasets of the semantic model it resolves against, as the caller sees the mapping (entries reading a dataset hidden from the caller annotate nothing). A field SHALL carry one annotation per mapped position whose expression reads it, naming the ontology, the concept expected at that position and its chain of supertypes up to the built-in, the field's role there — *value* (a value-type position), *identifies* (an identifying position of an object mapping of the dataset's own concept), or *references* (any other entity position) — the relationship the position takes part in, and, when the expression is not a plain field, the expression. A dataset's own concepts SHALL be the entity concepts it produces objects of that no mapped `ManyToOne` or `OneToOne` link of another entity produced in the same dataset reaches (identifying links included); every other entity it produces SHALL be listed as also populated. A dataset relationship SHALL line up with a binary ontology relationship `C.r` whose last role is an entity `E` when, paired in order by `E`'s identifying relationships, its fields on one side are the plain fields the `E` referents under `C`'s object mapping read and its fields on the other side are the plain fields the object mapping of `E` or of a subtype of `E` reads, in either orientation. Relationships SHALL resolve through `extends` and qualified `Concept.name` references; a mapping that does not resolve SHALL annotate nothing.

#### Scenario: A value field
- **WHEN** `flights_mapping` is stored for the flights semantic model
- **THEN** `FLIGHT.dep_delay` is annotated as a value of `Delay` (`Delay → NrMinutes → Decimal`) in `Flight.departure_delay`

#### Scenario: Identifying and referencing fields
- **WHEN** `flights_mapping` is stored
- **THEN** `AIRCRAFT.tail_nr` identifies `Aircraft` through `tailnum` (a `TailNr`), and `FLIGHT.tail_nr` references `Aircraft` through `Flight.aircraft`

#### Scenario: A dataset's own concept
- **WHEN** `flights_mapping` is stored
- **THEN** `AIRCRAFT`'s own concept is `Aircraft` and it also populates `Model`, `Manufacturer` and `Carrier`; `AIRPORT`'s own concept is `Airport` and it also populates `City`, `State` and `Market`

#### Scenario: A relationship lines up
- **WHEN** dataset `FLIGHT` declares a relationship to `AIRCRAFT` joining `tail_nr` to `tail_nr`
- **THEN** that relationship is annotated with `Flight.aircraft`

#### Scenario: A relationship to a subtype's dataset lines up
- **WHEN** the role of `C.r` is entity `E`, a dataset is mapped to `S` extending `E`, and the dataset of `C` declares a relationship to it on the fields the `E` referents under `C`'s object mapping read
- **THEN** that relationship is annotated with `C.r`

#### Scenario: Hidden datasets annotate nothing
- **WHEN** a caller who cannot see `AIRCRAFT` inspects `FLIGHT`
- **THEN** no annotation names `AIRCRAFT`, `Manufacturer`, `Model` or an expression reading `AIRCRAFT`

### Requirement: Verbalizations are filled with mapped names
A relationship's verbalizations SHALL be shown with each placeholder `{Concept}` or `{Concept:role}` filled by the concept name followed by the expression(s) mapping that role, and with the hyphen of a hyphen-bound word removed.

#### Scenario: Departure delay
- **WHEN** `FLIGHT.dep_delay` is inspected
- **THEN** its annotation reads "Flight `FLIGHT.id` has departure Delay `FLIGHT.dep_delay`"

### Requirement: Value-type constraints are shown as domains
A value type's `requires`, its supertypes' included, SHALL be shown on every field annotated with it: each in the plain subset (the value type as the only variable, literals, comparisons, `AND`, `OR`, `NOT`, `IN`) as a domain — a closed range as `low–high`, a one-sided bound as the comparison, an enumeration as `one of …` — and any other as written, marked not checked.

#### Scenario: A range and an enumeration
- **WHEN** `ROUTE.dist_grp` and `FLIGHT.cancel_code` are inspected
- **THEN** the first shows the domain `1–10` and the second `one of A, B, C, D`

### Requirement: Inspect shows annotations and the concept graph
`inspect` of a dataset SHALL show its own and also-populated concepts, of a field its annotations with each concept's description, filled verbalization and domain, and the relationships of a dataset SHALL show the ontology relationships they line up with, their filled verbalizations and multiplicity, in markdown and json. `inspect` of an ontology SHALL return its concept graph: for each concept its type, supertypes, description, identifying relationships, `requires`, and its relationships with their roles, multiplicity, verbalizations and `requires`, and the fields annotated with it in the caller's visible mappings.

#### Scenario: Inspect a field
- **WHEN** `FLIGHT.dep_delay` is inspected after `flights_mapping` is stored
- **THEN** the output names `Delay`, its supertypes and the filled verbalization of `Flight.departure_delay`

#### Scenario: Inspect the ontology
- **WHEN** ontology `Flights` is inspected
- **THEN** the output lists concept `Delay` extending `NrMinutes`, relationship `Flight.departure_delay` with multiplicity `ManyToOne` and verbalization `{Flight} has departure- {Delay}`, and the fields `FLIGHT.dep_delay` and `FLIGHT.arr_delay` annotated with `Delay`

### Requirement: Search reads concepts
The text search indexes for a field or dataset SHALL include its concept names, supertype chains, the descriptions of its concepts and their supertypes, and filled verbalizations, in every retrieval channel. Saving, editing or deleting a mapping or an ontology SHALL refresh the search entries of the datasets whose annotations change. A search's `entities` SHALL accept `concept:<Concept>` and `concept:<Ontology>.<Concept>`, selecting every field and dataset the caller can see that is annotated with that concept or a subtype, each listed once; an unqualified name SHALL match the concept in every ontology the caller's visible mappings use. A search SHALL never return concept text derived from a dataset hidden from the caller.

#### Scenario: Every delay field
- **WHEN** `search` is called with `entities=["concept:Delay"]` in datasource `flights`
- **THEN** the results include `FLIGHT.dep_delay` and `FLIGHT.arr_delay`

#### Scenario: A question finds a concept
- **WHEN** `search` is called in datasource `flights` with the question "unit of measure for time in minutes"
- **THEN** `FLIGHT.dep_delay` and `FLIGHT.arr_delay` are among the results

#### Scenario: Deleting a mapping removes its concepts
- **WHEN** `flights_mapping` is deleted and `search` is called with `entities=["concept:Delay"]`
- **THEN** no result is returned for the concept

### Requirement: Identifiers are checked against dataset keys
For each dataset's own concept whose identifying expressions in that dataset are all plain fields of it, the identifying fields SHALL be unique by the dataset's declared keys: its primary key, its `unique` fields and its enforced key sets, a key set that is a subset of the identifying fields making them unique. A mismatch SHALL be reported naming the mapping, the concept, the dataset and the fields, with a suggestion: declaring the fields the primary key when the dataset has none; marking the field `unique` for a single field; and otherwise stating that a composite unique key beside a different primary key cannot be declared.

#### Scenario: No key declared
- **WHEN** `validate_datasets` runs for `flights`, whose datasets declare no keys
- **THEN** it reports an identifier mismatch for `AIRCRAFT` and `Aircraft` suggesting `tail_nr` as the primary key, and none for `Model` or `Manufacturer`

#### Scenario: A composite key agrees
- **WHEN** `RUNWAY` declares `designator` and `airport_code` its primary key
- **THEN** no identifier mismatch is reported for `Runway`

### Requirement: Multiplicities are checked against lined-up relationships
For a dataset relationship lined up with a binary ontology relationship `C.r` to an entity: `ManyToOne` SHALL require the relationship to be to-one from `C` to `E`, and `OneToOne` to be to-one in both directions; a declared cardinality contradicting this SHALL be reported naming the mapping, the ontology relationship and the dataset relationship. A lined-up relationship with no declared cardinality SHALL be reported with the suggestion to declare the ontology's cardinality, never to mark it trusted. The ontology's multiplicity SHALL NOT change how any query is evaluated.

#### Scenario: A contradicting cardinality
- **WHEN** `FLIGHT → AIRCRAFT` on `tail_nr` is declared `one_to_many`
- **THEN** a multiplicity mismatch names `Flight.aircraft`, `ManyToOne` and the relationship

#### Scenario: No cardinality
- **WHEN** `FLIGHT → AIRCRAFT` on `tail_nr` declares no cardinality
- **THEN** the report suggests declaring it `many_to_one`, and every query across it evaluates as before

### Requirement: Model checks warn on save and never refuse
Saving or editing a mapping, an ontology, a semantic-model document or a dataset SHALL return, through every surface that reports save warnings, the identifier and multiplicity mismatches of the mappings and datasets the change touches. No ontology check SHALL refuse a save or change a stored model; the refusal of explicit changes that break a stored mapping is unchanged.

#### Scenario: A dataset save warns
- **WHEN** `FLIGHT` is saved with its relationship to `AIRCRAFT` declared `one_to_many`
- **THEN** the save succeeds, stores the dataset as given, and returns the multiplicity mismatch as a warning

### Requirement: Value constraints are checked against the data
`validate_datasets` SHALL check each value-type `requires` in the plain subset, its supertypes' included, at every mapped role position whose concept is that value type or a subtype and whose expression reads one dataset, against the database: rows where the mapped value is not NULL, the mapping's predicate holds and the constraint does not hold. It SHALL report the number of such rows and up to five distinct violating values, naming the mapping, the concept, the constraint and the position. A `requires` list SHALL be read as the conjunction of its entries.

#### Scenario: An out-of-range latitude
- **WHEN** one `AIRPORT` row has `latitude` 95 and `validate_datasets` runs for `flights`
- **THEN** it reports one row violating `DegreesLatitude <= 90` at `AIRPORT.latitude`, with sample value 95

#### Scenario: NULL is not a violation
- **WHEN** `FLIGHT.cancel_code` is NULL for every non-canceled flight
- **THEN** none of those rows violates the `CancelationCode` constraint

### Requirement: Required links are checked against the data
`validate_datasets` SHALL check each concept-level `requires` entry that is a single path `E.r` (resolved through `extends` and qualified names): for every mapped position producing `E` objects, the objects it produces (identifier tuples with no NULL part, under the position's predicate) that no mapped position links through `E.r` with a non-NULL value. It SHALL report, per producing position, the number of such objects and up to five sample identifiers. Entries of the same `requires` list SHALL be checked separately.

#### Scenario: An airport with no latitude
- **WHEN** an `AIRPORT` row has a `code` and a NULL `latitude`
- **THEN** `validate_datasets` reports one `Airport` without `Airport.latitude` at the position mapped from `AIRPORT`

#### Scenario: An airport known only from routes
- **WHEN** `ROUTE.dest_airport_code` holds a code no `AIRPORT` row has, and no stored relationship joins `ROUTE` to `AIRPORT`
- **THEN** `validate_datasets` reports that `Airport` without `Airport.latitude` and without `Airport.longitude` at the position mapped from `ROUTE.dest_airport_code`

### Requirement: Every requires not checked is listed
`validate_datasets` SHALL list, naming the mapping, the ontology and the reason: every `requires` entry at document, concept or relationship level outside the checked forms; every position whose expression reads more than one dataset; every check whose query cannot be bound or that the database rejects, with the message; and every mapping that does not resolve, with its problems as the caller sees them. `derived_by` SHALL NOT be listed.

#### Scenario: The flights rules not checked
- **WHEN** `validate_datasets` runs for `flights`
- **THEN** it lists `COUNT[Airport] > 0`, `COUNT[Carrier] > 0`, `Flight.scheduled_departure < Flight.scheduled_arrival`, `Flight.departs_at < Flight.arrives_at`, `Flight.operated_by(Aircraft.carrier)`, `NOT Route.departure(Airport)` and `NOT Route.destination(Airport)` as not checked

### Requirement: Ontology findings are report-only validation entries
Every ontology finding SHALL be a `validate_datasets` entry naming its check (`identifier`, `multiplicity`, `constraint`, `required_link`, `not_checked`), its mapping and its ontology, returned by the Python API, REST, the CLI and MCP; cleanup (`--force-clean`) SHALL never act on one.

#### Scenario: Force-clean ignores ontology findings
- **WHEN** `slayer validate-datasets --force-clean` runs for `flights` with ontology findings present
- **THEN** no dataset, mapping or ontology is changed because of them, and they are printed
