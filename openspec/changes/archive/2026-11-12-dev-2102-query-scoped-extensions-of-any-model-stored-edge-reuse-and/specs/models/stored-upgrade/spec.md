## ADDED Requirements

### Requirement: Stored root extensions move to query extensions

On load, every stored query written before this change whose `source_model` is an extension object SHALL become a query with `source_model` naming the extension's base model and that extension as its one `extensions` entry. This SHALL apply recursively through stored `source_queries`, saved queries and memories' queries, including a query nested in a document whose own version is current.
- The upgrade SHALL emit no deprecation warning and SHALL lose no column, measure or join.
- Repeated loads SHALL change nothing further.
- The upgraded query SHALL return the rows it returned before.
- A save SHALL write the upgraded form.

#### Scenario: A stored query-backed model's stage
- **WHEN** a stored query-backed model has a stage with `source_model` = `{"source_name": "orders", "joins": [...]}`
- **THEN** it loads with `source_model` `orders` and one extension of `orders` carrying those joins, without a deprecation warning, and returns the rows recorded before the upgrade

#### Scenario: A memory's query in a current-version memory
- **WHEN** a memory at the current memory version holds a query marked with a version before this change whose `source_model` is an extension object
- **THEN** the query loads in the upgraded form, without a deprecation warning, with its extension intact

#### Scenario: An unversioned stored memory query
- **WHEN** a stored memory without a version holds a query whose `source_model` is an extension object
- **THEN** it loads in the upgraded form, without a deprecation warning

#### Scenario: Upgrade is idempotent
- **WHEN** an upgraded document is saved and loaded again
- **THEN** it is unchanged
