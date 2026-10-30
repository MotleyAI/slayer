# Joins and the population

Never write joins. Reference a joined model's column by dotted path
(`customers.regions.name`) in any expression position; the engine finds the route
over the declared joins, in either direction. A short path (`regions.name`) works when
exactly one route exists; an ambiguous route errors naming the candidates, so write
the longer path. Result keys carry the full routed path.

## Choosing the root

`source_model` decides which rows exist: every row of the root appears unless a
condition removes it. Rooted at `orders` and enriched from `customers`, customers
without orders are absent; rooted at `customers` they appear with counts of 0 and
other aggregates NULL. When the question lists entities ("every region", "all
customers"), root the query at that model. Omit `source_model` to let the engine pick
the smallest model determining the dimensions (the choice is reported).
`recommend_root_model` proposes a root for a list of `model.column` items.

## Aggregating joined fields

Each aggregation runs over its own model's rows exactly once, whatever the root:
`sum(customers.credit)` from `orders` counts each customer once (no fan-out), and
`count(orders.id)` from `customers` counts orders. When such an aggregate is sliced by
a dimension it cannot be attributed to (an order's status for customer credit),
`to_many_handling` decides:

- `broadcast` (default): the total repeats on every row, with a warning;
- `associate`: each row gets the aggregate over the entities associated with it
  (the credit of the customers having orders in that status);
- `error`: the query is refused.

## Patterns

- Rows without a match (anti-join): rooted at `customers`, the condition
  `orders.id is null` keeps customers without orders.
- Conditional counts over a joined model: from `regions`,
  `{"formula": "sum(iif(customers.orders.order_date >= '2025-01-01', 1, 0))",
  "name": "orders_2025"}`; NULL where a region has no orders, so add a `count(...)`
  beside it when a 0 matters.
- A list column `tags` is a child with one row per element, `tags.value`: the condition
  `tags.value = 'x'` keeps the rows having element x; `tags.value` as a dimension with
  `associate` counts the rows per element. Every `tags.value` condition reads the same
  element, so `tags.value = 'x'` with `tags.value = 'y'` matches nothing: write
  `contains_all(tags, ['x', 'y'])`; `not contains(tags, 'x')` keeps the rows without x.
- A per-query column: an expression where it is used, e.g. the computed dimension
  `{"expression": "amount > 100", "name": "is_big"}`. A per-query join: `source_model`
  as an inline extension `{"source_name": "orders", "joins": [...]}`.
