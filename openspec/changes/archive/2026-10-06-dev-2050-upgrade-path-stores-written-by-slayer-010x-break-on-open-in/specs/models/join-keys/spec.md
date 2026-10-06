## MODIFIED Requirements

### Requirement: Stored physical spellings are canonicalised on load

WHEN a stored model below the current schema version carries a `join_pairs` entry that names no declared column of its side but equals exactly one declared base column's unquoted `sql` rename, THEN the entry SHALL be rewritten to that column's `name` on load. The source side is matched against this model's columns, the target side against the target model's stored columns, and the model SHALL be written back at the current version. An entry naming a declared column SHALL be left untouched.

An entry that matches no declared column and no rename, and that is a valid column name, SHALL become a declared hidden base column of its own side's model when that side's document is loaded below the current schema version:
- a source-side key is added to the loading model;
- a target-side key is added to the target model, when that model is loaded, from the incoming joins stored in its datasource's sibling documents.

The added column's type SHALL be the stored type of the opposite key column when that is a valid type, and the default type otherwise. A document SHALL only ever add columns to itself. Query-backed models SHALL NOT receive such columns, and a sibling document that cannot be read SHALL be skipped by that scan. An ambiguous entry, or one that is not a valid column name (e.g. dotted), SHALL be left verbatim for validation to judge.

The canonicalisation SHALL precede exact-inverse join deduplication, and the counterpart comparison SHALL use canonicalised spellings on both sides.

#### Scenario: A source-side physical spelling is rewritten on load

- **WHEN** a v10 stored model declares `Column(name="customer_id", sql="cust_fk")` and its join carries `join_pairs: [["cust_fk", "id"]]`
- **THEN** after loading, the join carries `[["customer_id", "id"]]` and the stored document is at the current version with the rewritten entry, on both the YAML and the SQLite backends

#### Scenario: A target-side physical spelling is rewritten on load

- **WHEN** a v10 stored `orders` carries `join_pairs: [["customer_id", "customer_pk"]]` and the stored `customers` declares `Column(name="id", sql="customer_pk")`
- **THEN** after loading `orders`, the join carries `[["customer_id", "id"]]`

#### Scenario: A mirror pair spelled two ways collapses to one edge

- **WHEN** a v10 store holds `orders → customers` on `[["customer_id", "customer_pk"]]` and `customers → orders` on `[["id", "customer_id"]]`, where `customers.id` has `sql="customer_pk"`
- **THEN** after loading, exactly one edge connects the pair

#### Scenario: An unmatched entry is left for validation

- **WHEN** a v10 stored model's join names a source-side key `orders.customer_id`, which is not a valid column name and neither is declared nor equals any column's rename
- **THEN** loading fails with the construction error naming the column and the remedy, and sibling models still load

#### Scenario: An undeclared source-side key becomes a hidden column

- **WHEN** a v10 stored `orders` declares columns `order_id, status, ordered_at, amount` and joins `customers` on `[["customer_id", "id"]]`, as SLayer 0.10.2's Cube importer wrote it for a cube without a `customer_id` dimension
- **THEN** `orders` loads with a hidden base column `customer_id` and is written back at the current version, on both the YAML and the SQLite backends
- **AND** a query of `customers.region` by `sum(amount)` returns the rows SLayer 0.10.2 returned, with a join condition carrying no casts
- **AND** loading it again changes nothing, and re-saving it passes save-time validation

#### Scenario: An undeclared target-side key becomes a hidden column

- **WHEN** a stored `orders` joins `customers` on `[["customer_id", "id"]]` and the stored `customers`, at v10 or at v13, declares no `id`
- **THEN** after loading, `customers` carries a hidden base column `id`, and the `customers.region` by `sum(amount)` query returns the rows SLayer 0.10.2 returned

#### Scenario: An unreadable sibling does not block the target-side scan

- **WHEN** a SQLite store holds one model whose stored JSON is corrupt, beside valid models that are join targets
- **THEN** every valid model loads, and only the corrupt model fails to load
