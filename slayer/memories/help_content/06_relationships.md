# Relationships and the population

Never write joins. Every reference starts at a dataset (`regions.name`,
`customers.regions.name`) in any expression position; the engine reaches that dataset
from the population over the declared relationships, in either direction: by its only
to-one path, else its only route. Two to-one paths, or several routes and none to-one,
error naming each explicit path (`orders.customers.regions.name`): write one of them.
A relationship name never starts a reference; a role is a route from its dataset
(`orders.buyer.name`). Result keys are the reference as written, never the population.

## Choosing the root

`population` decides which rows exist: every row of the root appears unless a
condition removes it. Rooted at `orders` and enriched from `customers`, customers
without orders are absent; rooted at `customers` they appear with counts of 0 and
other aggregates NULL. When the question lists entities ("every region", "all
customers"), root the query at that dataset. Omit `population` to let the engine pick
the smallest dataset determining the dimensions (the choice is reported).
`recommend_population` proposes a root for a list of `dataset.field` items.

## Aggregating joined fields

Each aggregation runs over its own dataset's rows exactly once, whatever the root:
`sum(customers.credit)` from `orders` counts each customer once (no fan-out), and
`count(orders.id)` from `customers` counts orders. When such an aggregate is sliced by
a dimension it cannot be attributed to (an order's status for customer credit),
`to_many_handling` decides:

- omitted (default): each row gets the aggregate over the entities associated with
  it (the credit of the customers having orders in that status), with an
  `associated` warning; `window=` and `first`/`last` repeat the total instead, with
  a `broadcast` warning naming why;
- `associate`: the same, but refuses `window=` and `first`/`last`;
- `broadcast`: the total repeats on every row, with a warning;
- `error`: the query is refused.

## Patterns

- Rows without a match (anti-join): rooted at `customers`, the condition
  `orders.id is null` keeps customers without orders.
- Conditional counts over a joined dataset: from `regions`,
  `{"metric": "sum(iif(customers.orders.order_date >= '2025-01-01', 1, 0))",
  "alias": "orders_2025"}`; NULL where a region has no orders, so add a `count(...)`
  beside it when a 0 matters.
- A list field `orders.tags` is a child with one row per element, `orders.tags.value`: the
  condition `orders.tags.value = 'x'` keeps the rows having element x; it as a dimension with
  `associate` counts the rows per element. Every `orders.tags.value` condition reads the same
  element, so `= 'x'` with `= 'y'` matches nothing: write
  `contains_all(orders.tags, ['x', 'y'])`; `not contains(orders.tags, 'x')` keeps the rows without x.
- A per-query field: an expression where it is used, e.g. the computed dimension
  `{"field": "orders.amount > 100", "alias": "is_big"}`. A per-query relationship, on any dataset the
  query reads: `extensions: [{"dataset": "orders", "relationships": [...]}]`; repeating a stored
  relationship's `from_columns` / `to_columns` reuses that edge, and `"join_type": "inner"` keeps only the rows with
  a match.
