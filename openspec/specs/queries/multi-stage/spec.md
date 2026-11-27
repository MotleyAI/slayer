# queries/multi-stage Specification

## Purpose
Defines how a multi-stage query list — named sibling stages plus a final root —
is ordered and validated so every stage can read the siblings it references.

## Requirements

### Requirement: Stages are ordered by every sibling reference
A multi-stage query list SHALL be ordered so each stage follows every sibling it
references — as its `population`, as an extension's `dataset`, as the `to` of one
of its extensions' relationships, or as a dataset named by the first segment of any reference in any of its
expressions (dimensions, metrics, filters, order) — regardless of the order or the
names of the non-root stages. The last entry SHALL stay the root. A stage's join graph
SHALL contain exactly the sibling stages it references in one of these ways and the siblings
its inherited edges target, so no route passes through or into a sibling the stage neither
names nor reaches by an inherited edge. Duplicate stage names,
an unnamed non-root stage, a self-reference, a non-root stage referencing the root, and
a cycle SHALL each be rejected with an error naming the offending stages.

#### Scenario: A stage joining a sibling that reads another sibling
- **WHEN** the list `[x (population customers), c (population x), b (population orders, extended with a relationship to c), root (population b)]` is executed
- **THEN** it returns the hand-computed result instead of failing to resolve `c`

#### Scenario: Supply order of non-root stages does not matter
- **WHEN** the non-root stages of that list are supplied in any permutation, root last
- **THEN** every permutation returns the same result

#### Scenario: Sibling reached through an inline model
- **WHEN** a stage's source is an inline model whose relationships reference a sibling stage
- **THEN** the list is rejected at construction with the inline-model error, and the sibling is reached instead through an extension of a named source relating to it

#### Scenario: Sibling referenced inside nested `source_queries`
- **WHEN** a stage's source is an inline model carrying `source_queries` (which could only reach a sibling through nested `source_queries`)
- **THEN** the query is rejected at construction with the inline-model error, with a hint to write those queries as named stages

#### Scenario: One stage reading several siblings
- **WHEN** a stage's population is one sibling and its extensions relate to two other siblings, or extend a third
- **THEN** it is ordered after all of them and executes correctly

#### Scenario: Malformed lists are rejected
- **WHEN** a list has an unnamed non-root stage, a stage referencing itself, a non-root stage referencing the root, or a cycle
- **THEN** it is rejected with an error naming the offending stages

#### Scenario: A sibling named only in an expression
- **WHEN** a non-root stage `a_reader` (population `customers`) measures `max(z_ev.ne)` of a sibling `z_ev` (population `events`, metric `count(*)` aliased `ne`) that it reaches by no relationship, and the same list is run again with the stages named `z_reader` and `a_ev`
- **THEN** both lists execute and return the same hand-computed value

#### Scenario: A relationship named like a sibling is not a sibling reference
- **WHEN** stage `a` (population `orders`) groups by `orders.ship_region.name` through `orders`' relationship `ship_region`, and a sibling stage named `ship_region` reads `a`
- **THEN** the list executes, and `a` is not ordered after `ship_region`

#### Scenario: A first segment naming a sibling reads the sibling
- **WHEN** that stage `a` groups by `ship_region.name` instead
- **THEN** `a` reads the sibling `ship_region`, and the list is rejected as a cycle

#### Scenario: A route hop is no sibling reference
- **WHEN** a stage over `customers` measures `max(customers.z_ev.ne)` and a sibling is named `z_ev`
- **THEN** `z_ev` is a hop of the route from the anchor `customers`, not a sibling the stage reads

#### Scenario: An inherited edge reaches its sibling target
- **WHEN** stage `keyed` inherits its extension's relationship `totals_link` to sibling `totals`, and the root reads `keyed.totals_link.total` from `keyed` without naming `totals`
- **THEN** the root returns the hand-computed values

#### Scenario: Routing never passes through an unnamed sibling
- **WHEN** a sibling stage inherits edges to `customers` and `regions`, and a later stage over `customers` that never names that sibling selects `regions.name`
- **THEN** the sibling is not a route candidate: the reference resolves as it would without the sibling

### Requirement: Stage names are query-local
A stage name SHALL be visible only within its own query list, where a name the
query writes (`population`, an extension's `dataset`, the `relationships[].to`
of an extension) SHALL resolve to the stage in preference to a
same-named model. A stored definition — a stored model's joins, a stored
query-backed model's `source_queries` — SHALL never see a stage name: its
references resolve against stored models (and, inside a stored query-backed model,
its own private stages) only. A stage name SHALL NOT capture a physical table the
statement reads. Result keys, warnings and errors SHALL spell stages by the user's
name only.

#### Scenario: A stored join resolves to the model beside a same-named stage
- **WHEN** a list has a stage named `customers` (over `orders`) and a stage over `orders` asks for `customers.tier` through `orders`' stored join to the model `customers`
- **THEN** `customers.tier` reads the model and the result equals the hand-computed amount by tier

#### Scenario: A query-written join to a stage wins over a same-named stored edge
- **WHEN** a stage extends `orders` with a join to the stage `customers` while `orders` also stores an edge to the model `customers`, and asks for `customers.<field>`
- **THEN** the hop reaches the stage

#### Scenario: A stage named like a physical table does not hide it
- **WHEN** a stage is named after the physical table of a model that another stage reaches through a stored join
- **THEN** that stage reads the table, and the result equals the hand-computed value

#### Scenario: A stored query-backed model's private stage names are local
- **WHEN** a stored query-backed model has a private stage named like a storage model that one of its stages reaches through a stored join
- **THEN** the join reads the storage model
- **WHEN** a consumer query references a stored query-backed model's private stage name
- **THEN** it does not reach the private stage

#### Scenario: User spelling in results and messages
- **WHEN** any multi-stage query executes, warns or fails
- **THEN** its result keys, warnings and errors name stages by the user's spelling and never show an internal identity

#### Scenario: An inferred population is never a stage
- **WHEN** a rootless stage's population is inferred to a model that shares its name with a stage in the list
- **THEN** the stage reads the model

### Requirement: Stage errors name the stage
An error about a query stage's output schema SHALL name the stage by its user-facing name (its name in the query list, or the query-backed model's name), never by the stage's source model.

#### Scenario: Schema mismatch names the stage
- **WHEN** a stage named `cr` over source model `orders` renders output columns that do not match its expected schema
- **THEN** the error message names `stage 'cr'` and does not name `stage 'orders'`

### Requirement: Stages inherit edges on pass-through keys
A stage's output column is a **pass-through** of a field when its value is that field
unchanged, read at the field's position on a path the stage reads: a plain dimension or
raw-row column, renamed by an alias or not. A time bucket, a cast, any other computed
expression, a metric and a hidden column are not pass-throughs.

A stage SHALL inherit every edge touching a dataset it reads — its population, or a
dataset on a path one of its output columns reads — in either direction and wherever the
edge was declared: a stored relationship, a relationship declared by an extension of that
stage's own query object, or an edge an earlier stage inherited. Taking the edge from the
read dataset `D` to the dataset `T` on key pairs `(dᵢ, tᵢ)`, the stage SHALL inherit it as
an edge from the stage to `T` on pairs `(cᵢ, tᵢ)` when, for every pair, an output column
`cᵢ` passes through either `D.dᵢ`, or `T.tᵢ` where `T` is reached from `D` through that
edge on a path the stage reads. Each source edge at a position SHALL be inherited once, a
pass-through of `D.dᵢ` taking precedence over one of `T.tᵢ`, and an edge reaching a stage
through several upstream stages SHALL still be inherited once.

An inherited edge SHALL keep the source edge's name and target dataset (never re-resolved
to a same-named stage), keep a stored edge's join type, and be a LEFT edge when inherited
from a query-declared relationship. Filters SHALL NOT affect inheritance: neither the
stage's filters nor its population's model filters remove an edge, and the target's model
filters apply on the inherited hop as on any hop. A stage that restricts its population — by a
filter (a metric filter included), or by reading a restricted population or upstream stage —
SHALL be read as its own rows, exactly as a filtered stored dataset: an aggregation whose home
reads the stage on its own path to a grain dimension or filter sees only the stage's rows, a home
row no stage row matches landing in the NULL tuple; a home that reads the stage on none of its own
paths is never narrowed by it, even when the stage is the query's population (`queries/population`
› Population may be omitted); an unrestricted stage restricts nothing. Inherited edges SHALL apply to ordinary
stages wherever a later stage reads them (as its population, as an extension target, as a
relationship target, or through routing) and to the final stage of a saved query-backed
model wherever it is consumed. They are derived on every read and never stored.

#### Scenario: A stage grouped by a foreign key reaches its target
- **WHEN** stage `per_customer` (population `orders`) has dimension `customer_id` and metric `sum(amount)` aliased `revenue`, and the next stage (population `per_customer`) has dimension `customers.region` and metric `sum(revenue)`
- **THEN** each region's total equals the hand-computed sum, with no relationship declared and no broadcast or unrelated-dataset warning

#### Scenario: A time-bucketed key is not inherited
- **WHEN** a stage over `orders` buckets `ordered_at` by month, and `orders` relates to a `calendar` dataset on `ordered_at`
- **THEN** the stage inherits no edge to `calendar`, and a later stage reaches `calendar` by no join path

#### Scenario: A cast or computed key is not inherited
- **WHEN** a stage projects `CAST(customer_id AS TEXT)`, or `customer_id + 0`, under an alias
- **THEN** the stage inherits no edge to `customers`

#### Scenario: A renamed key is inherited
- **WHEN** a stage projects `customer_id` under the alias `cid`
- **THEN** a later stage reaches `customers` through the inherited edge on `cid`, with the same values as the unrenamed key

#### Scenario: The target's own key is inherited
- **WHEN** a stage over `orders` has dimension `customers.customer_id`
- **THEN** a later stage reaches `customers` through that column, and onward through every edge of `customers`

#### Scenario: Onward traversal through an inherited edge
- **WHEN** `customers` relates to `regions`, and a later stage over a stage grouped by `orders.customer_id` selects `customers.regions.name`
- **THEN** it returns the hand-computed values

#### Scenario: An edge is inherited in the reverse direction
- **WHEN** a stage over `customers` has dimensions `customer_id` and `region`, `orders` stores the relationship to `customers`, and the next stage over that stage measures `sum(orders.amount)` by `region`
- **THEN** it reaches `orders` through the inherited edge and returns the hand-computed totals

#### Scenario: A relationship declared by the stage's own extension is inherited
- **WHEN** a stage over `orders` declares, in an extension of `orders`, a relationship to `regions` on `region_id` and has dimension `region_id`, and the next stage over that stage selects `regions.name`
- **THEN** it reaches `regions` with no second declaration

#### Scenario: Inheritance is transitive
- **WHEN** three stages chain, each grouping by `customer_id` passed through from the one before, and the last selects `customers.region`
- **THEN** it returns the hand-computed values

#### Scenario: One source edge reaching a stage twice is inherited once
- **WHEN** two upstream stages both pass `customer_id` through, and a stage relating to both passes it on
- **THEN** that stage carries one inherited edge to `customers`, never a parallel pair

#### Scenario: Filters do not remove an inherited edge
- **WHEN** the stage has a filter on `status`, and `orders` carries a model filter
- **THEN** the edge to `customers` is inherited; a model filter on `customers` applies on the inherited hop

#### Scenario: A filtered stage restricts what is reached through it
- **WHEN** stage `pc` (population `orders`, filter `amount > 25`) groups by `customer_id`, and the next stage (population `pc`) measures `sum(orders.amount)` grouped by `pc.customer_id`, and separately `count(customers.id)` with no dimension
- **THEN** the grouped query's rows are the customers `pc` keeps, each summing all of its own orders (the home `orders` reaches `pc` through the inherited edge, an order of a customer `pc` drops landing in the NULL tuple, which is no row); the dimensionless count counts every customer, its home `customers` reading no `pc` row — exactly as from a stored `orders` root carrying the model filter `amount > 25`

#### Scenario: A stored target is never re-resolved to a same-named stage
- **WHEN** a list has a stage named `customers` and another stage grouped by `orders.customer_id` passes the key on
- **THEN** the inherited edge reaches the stored `customers` dataset, not the stage

#### Scenario: Equivalent to an explicitly declared relationship
- **WHEN** a query reads a dataset through an inherited edge, and the same query instead declares that edge as an extension relationship of the stage
- **THEN** both return the same rows and values

### Requirement: Inherited edges re-derive their cardinality from the stage grain
An inherited edge's arity SHALL be judged per direction. The target side SHALL keep the
source edge's arity in that direction (its declared cardinality, or structural proof on the
target). The stage side SHALL be "one" exactly when the stage is provably unique on the
inherited key columns — a single-column grain equal to the key, or a composite grain the
key columns fully cover — and "many" otherwise, including for a raw-row stage. When the
source edge declares a cardinality, the inherited edge SHALL declare the composition of
the two sides; otherwise it SHALL declare none and structural proofs decide.

#### Scenario: Grouping by the key makes the edge one-to-one
- **WHEN** a stage grouped by `orders.customer_id` inherits the many-to-one `orders` → `customers` edge
- **THEN** the inherited edge is one-to-one: a `customers`-rooted query reading the stage's metric through it gets exact per-customer values with no broadcast warning

#### Scenario: A grain wider than the key keeps the stage side many
- **WHEN** a stage grouped by `customer_id` and `status` inherits that edge
- **THEN** the stage side is many: a metric of the stage read from `customers` follows the to-many rules

#### Scenario: A raw-row stage keeps the stage side many
- **WHEN** a raw-row stage (`distinct_dimension_values: false`) projects `customer_id`
- **THEN** the inherited edge's stage side is many

### Requirement: Declared edges win over inherited ones
A declared edge — a saved query-backed model's own relationship, or a relationship a
consuming query object declares in an extension of the stage — SHALL take precedence over
an inherited edge:
- an inherited edge with the same target and key pairs as a declared edge, whose name
  equals the declared one's or where either is unnamed, SHALL be applied once, as the
  declared edge; an inherited name the declared edge lacks SHALL remain usable as an alias;
- an inherited edge with the same name as a declared edge on different key pairs SHALL be
  dropped;
- distinct parallel edges, including two differently named edges on the same key pairs,
  SHALL follow the existing parallel-edge rules: a named edge is chosen by spelling its
  name, and an unnamed parallel pair is refused naming the pair.

#### Scenario: An identical declared edge applies once
- **WHEN** a saved query-backed model grouped by `customer_id` declares the relationship to `customers` on `customer_id` that it would also inherit
- **THEN** a query through `customers` applies one edge, with no ambiguous-hop error, and the values equal the hand-computed ones

#### Scenario: A declared edge of the same name wins
- **WHEN** the inherited edge is named `buyer` and the consuming query's extension declares a relationship named `buyer` from the stage on other key pairs
- **THEN** `buyer` resolves to the declared relationship

#### Scenario: Differently named edges on the same keys stay parallel
- **WHEN** the inherited edge is named `buyer` and a declared edge on the same key pairs is named `payer`
- **THEN** a reference to `customers.<field>` is an ambiguous hop, and `buyer.<field>` and `payer.<field>` each resolve

#### Scenario: Parallel distinct edges need their names
- **WHEN** a stage passes through both `billing_customer_id` and `shipping_customer_id`, each a named edge to `customers`
- **THEN** `customers.<field>` raises the ambiguous-hop error, and spelling the edge name resolves it

### Requirement: A stage is read as the dataset its schema defines
A stage read by a later stage SHALL bind as the dataset its schema defines — its output
columns, its grain, its inherited edges and, for a saved query-backed model, its declared
relationships — through the same scope as any stored dataset. Reading a stage that
inherits no edge SHALL behave as before, except that a dotted reference that no edge or
route resolves SHALL fail with the error a stored dataset gives — its suggestion naming
the stage's flat column when the reference's tokens joined with `__` name one — and
short-form routing SHALL apply from the stage. List-element references (`L.value`, `L.*`), stale flat spellings, the
root qualifier, time-dimension re-bucketing and population inference SHALL behave as they
did against the stage's flat schema.

#### Scenario: An edge-less stage reads as before
- **WHEN** a later stage reads a stage that inherits no edge by flat names, list elements, a stale flat spelling and a re-bucketed time dimension
- **THEN** results and result keys equal those before this change

#### Scenario: A dotted spelling of a flattened stage column
- **WHEN** a later stage reads stage `s`, which projects `customers.region` but not a key of `customers`, and selects `customers.region`
- **THEN** it fails with a typed error whose suggestion names `customers__region` first

### Requirement: Every stage column carries a type
Every column a stage emits SHALL carry a type, derived from its value by the same rule
that types the value in the query plan; a later stage, and a consumer of a saved
query-backed dataset, SHALL read the column with exactly that type. An explicitly
declared type (a measure's `type`, a saved metric's `datatype`, an aggregation
definition's `datatype`) SHALL win over the derived one. The derived type SHALL be:
- a field's declared type, through any path, an upstream stage's column included;
- a cast's target type;
- a value-returning conditional's joined branch type (per `queries/mixed-type-coercion`);
- a comparison, `IN` or other predicate: BOOLEAN;
- a literal's type; a value that can only be NULL: INT;
- a scalar function's result type; arithmetic as `queries/mixed-type-coercion` and
  `aggregations/boolean-inputs` type it;
- an aggregation's result type (per `aggregations/boolean-inputs` and, for a custom
  aggregation, `aggregations/formula-templates`), its input typed through any path;
- `lag`, `lead`, `time_shift`: the input's type; `first`, `last` over rows or over an
  aggregate: the picked value's type; `cumsum`: `sum`'s
  result type over the input; `change`: the type of the input minus itself;
  `change_pct` and `percent_rank`: DOUBLE; `rank`, `dense_rank`, `ntile` and
  `consecutive_periods`: INT;
- a value computed from a value the query re-aggregates (a partitioned or second-order
  aggregate): the type of that value.

A conditional in a later stage SHALL therefore see each stage column's real type: a
mixed-type conditional over stage columns SHALL be refused exactly as over model columns,
and a NULL-only stage column SHALL read as INT, not as an absorbed NULL.

#### Scenario: Computed measures keep their type downstream
- **WHEN** a stage over `orders` projects `sum(orders.amount) / count(orders.*)`, `lower(orders.status)`, `cumsum(sum(orders.amount))`, `rank(sum(orders.amount))` and `orders.status IN ('paid')`, and a later stage reads each column
- **THEN** the later stage reads them as DOUBLE, TEXT, the type of `sum(orders.amount)`, INT and BOOLEAN respectively

#### Scenario: A cross-dataset aggregate keeps its type downstream
- **WHEN** a stage over `orders` projects `max(customers.created_at)` and `sum(customers.spend)`, and a later stage reads both
- **THEN** the later stage reads them as `created_at`'s type and `spend`'s aggregated type

#### Scenario: A re-aggregated value keeps its type downstream
- **WHEN** a stage projects `sum(orders.amount, partition_by=orders.region)` and a value computed from it, and a later stage reads both
- **THEN** the later stage reads each with the type the query plan gives it

#### Scenario: Branch typing over stage columns
- **WHEN** a later stage reads a stage's TEXT column `u` and INT column `n` and has the dimensions `iif(n > 1, u, 'x')` and `coalesce(n, 0)`
- **THEN** they are typed TEXT and INT

#### Scenario: A mixed-type conditional over stage columns is refused
- **WHEN** a later stage reads a stage's TEXT column `s` and DATE column `d` and has the dimension `coalesce(s, d)`
- **THEN** the query fails at plan time with the typed conditional-branch error naming TEXT and DATE

#### Scenario: A NULL-only stage column is INT
- **WHEN** a stage projects `null` as `nul` and a later stage reads `sum(s.nul)`, `count(s.nul)` and `coalesce(s.nul, 0)`
- **THEN** the column is INT and the results are NULL, 0 and 0, on SQLite and DuckDB

#### Scenario: An explicit type wins
- **WHEN** a stage measure declares `type` Decimal over a value derived as DOUBLE, and a later stage reads it
- **THEN** the later stage reads it as Decimal

#### Scenario: A saved query-backed dataset reads its columns' real types
- **WHEN** a saved query-backed dataset projects `sum(orders.amount) / count(orders.*)` and a query reads it
- **THEN** the column reads as DOUBLE
