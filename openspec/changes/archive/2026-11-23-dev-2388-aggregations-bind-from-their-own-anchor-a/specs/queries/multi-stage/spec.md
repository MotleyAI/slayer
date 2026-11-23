## MODIFIED Requirements

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

