## MODIFIED Requirements

### Requirement: Fields

Every field whose definition reads only its own dataset SHALL export as a field named by the field's name. Its expression SHALL read the dataset source's physical columns, with each path to another field of the same dataset replaced by that field's definition, on the syntax of the form it appears in. A field with a `filter` SHALL export with the expression `CASE WHEN <filter> THEN <value> END`, so that it is masked in every position, and every filter SHALL be applied exactly once however many definitions are replaced into one another. A field's `label`, `description` and imported Ossie `ai_context` SHALL export as the field's `label`, `description` and `ai_context`; text that the importer appended to a description from `ai_context` SHALL NOT be exported a second time.

#### Scenario: Renamed column
- **WHEN** field `total_amount` has `expression: o_totalprice`
- **THEN** exported field `total_amount` has the expression `o_totalprice`

#### Scenario: Derived column over another derived column
- **WHEN** `net` is `orders.gross - discount` and `gross` is `price * quantity`
- **THEN** field `net`'s expression reads only `price`, `quantity` and `discount`

#### Scenario: Filtered column
- **WHEN** field `completed_amount` has `expression: amount` and `filter: status = 'completed'`, over rows (completed, 10), (open, 5), (completed, 7)
- **THEN** exported field `completed_amount` evaluates to 10, NULL and 7 on those rows

#### Scenario: Imported description is stable
- **WHEN** a document with a field carrying `description` and `ai_context` instructions is imported and then exported
- **THEN** the exported field's `description` equals the original `description`

## REMOVED Requirements

### Requirement: Field and source expressions use a portable form only when exact
**Reason**: Writing a datasource-dialect text as portable `sql` because it reads back identically can change its meaning (Ossie's true division and `ROUND` ties differ from many databases'), and a field now carries its portable and dialect forms itself.
**Migration**: See "Each expression form exports as itself".

## ADDED Requirements

### Requirement: Each expression form exports as itself

A field's portable `sql` SHALL export as the field's `sql`, and each `dialects` entry SHALL export as a `dialects` entry with its text; an entry tagged with the datasource's own dialect, or a field's `sql` not yet converted, SHALL export under the Ossie token for the datasource's type. Every supported datasource type SHALL map to exactly one Ossie dialect token, which the importer reads as that datasource's own dialect. Datasource-dialect text SHALL never be exported as portable `sql`. When a field's definition has another field's definition replaced into it, the result SHALL be portable `sql` only when every replaced part is portable, and otherwise the entry for the datasource's dialect.

#### Scenario: Portable expression
- **WHEN** a DuckDB field is `expression: "price * quantity"`
- **THEN** its exported field has `sql: price * quantity` and no `dialects`

#### Scenario: Dialect-only expression
- **WHEN** a Postgres field is `{dialects: [{dialect: POSTGRES, expression: "amount / qty"}]}`
- **THEN** its exported field has no `sql` and one `dialects` entry `POSTGRES` holding `amount / qty`

#### Scenario: A portable field over a dialect field
- **WHEN** a Postgres field `ratio` is `expression: "orders.part * 2"` and `part` is `{dialects: [{dialect: POSTGRES, expression: "amount / qty"}]}`
- **THEN** exported `ratio` has no `sql` and one `POSTGRES` entry reading `(amount / qty) * 2`

#### Scenario: Export and re-import keep values
- **WHEN** a dataset with a derived, a filtered, a quoted and a dialect-only field is exported and imported again into the same DuckDB datasource
- **THEN** every field returns the same values as before
