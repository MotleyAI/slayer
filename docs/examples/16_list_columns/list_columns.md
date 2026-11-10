# List Columns

Some columns hold a list per row: a product's ingredients, the SKUs on an order, the tags on a ticket. SLayer types such a column as `ARRAY` with an `element_type`, and gives it three tools:

- the **list functions** test a whole list;
- the list's **elements** are a child dataset you filter, group and aggregate without fanning out the parent;
- **`collect(x)`** aggregates values into a list.

The examples use the Jaffle Shop demo's `products.ingredients` (the supplies a product is made from) and `orders.item_skus` (the SKU of each unit on an order, repeats kept).

## Testing a whole list

`contains(list, x)`, `contains_any(list, [...])`, `contains_all(list, [...])` and `length(list)` work wherever an expression does:

```json
{
  "source_model": "products",
  "dimensions": ["name", {"expression": "length(ingredients)", "name": "ingredient_count"}],
  "filters": ["contains_all(ingredients, ['bread', 'cheese'])"]
}
```

Absence is `NOT contains(ingredients, 'bread')`, and a list-valued variable fills a list argument: `contains_any(item_skus, {wanted})` with `"variables": {"wanted": ["JAF-004", "JAF-005"]}`.

A whole list groups, compares and counts distinctly as the set of its distinct elements (so `length(item_skus)` counts distinct SKUs and `count(item_skus.*)` every unit), comparing it with a scalar or `sum(item_skus)` is refused with an error naming the alternative, and `contains` on a text column is a substring test.

## Elements as rows

`ingredients.value` is one row per element and `count(ingredients.*)` counts them. `count(*)` grouped by an element counts the parents holding it:

```json
{
  "source_model": "orders",
  "dimensions": ["item_skus.value"],
  "measures": ["count(*)", {"formula": "count(item_skus.*)", "name": "units"}]
}
```

Reading elements never multiplies the parent rows, so `sum(order_total)` by store is the same with or without `count(item_skus.*)` beside it. Every `item_skus.value` condition in one query reads the same element: two element filters asking for `JAF-001` and `BEV-001` match no order, while `contains_all(item_skus, ['JAF-001', 'BEV-001'])` finds the orders holding both.

## Collecting into a list

`collect(x)` returns the distinct non-NULL values of `x` per cell, sorted, and `[]` for a cell with none. A collected list keeps its list semantics in a later stage:

```json
[
  {"name": "baskets", "source_model": "orders", "dimensions": ["customer_id"],
   "measures": [{"formula": "collect(item_skus.value)", "name": "skus"}]},
  {"source_model": "baskets", "measures": ["count(*)"], "filters": ["contains(skus, 'JAF-005')"]}
]
```

The [notebook](list_columns_nb.ipynb) runs every query on this page; the full rules are in [List columns](../../concepts/queries.md#list-columns) and [Lists](../../concepts/references.md#lists).
