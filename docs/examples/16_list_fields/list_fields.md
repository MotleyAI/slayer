# List Fields

Suppose your database stores contractors, and each contractor has a variable-length list of skills. If your database supports array-valued columns, these are very likely stored in such a column rather than in a separate table with a foreign key to contractors.

Now suppose you want to filter for contractors who have two specific skills — will your semantic layer allow that? How about counting the contractors that have each particular skill? Or showing the complete list of skills available from contractors in each city?

This is now easy in SLayer. SLayer types the field over such a column as `ARRAY` with an `element_type` (recognizing these automatically when ingesting schemas), and gives it three tools:

- the **list functions** test a whole list;
- the list's **elements** are a child dataset you can filter, group and aggregate without fanning out the parent;
- **`collect(x)`** aggregates values into a list.

Let's look at some examples, using the Jaffle Shop demo's `products.ingredients` (the supplies a product is made from) and `orders.item_skus` (the SKU of each unit on an order, repeats kept).

## Testing a whole list

These functions test what an array-valued field contains: `contains(list, x)` (a single value), `contains_any(list, [...])`, `contains_all(list, [...])` and `length(list)` work as you'd expect:

```json
{
  "population": "products",
  "dimensions": ["name", {"field": "length(ingredients)", "alias": "ingredient_count"}],
  "filter": ["contains_all(ingredients, ['bread', 'cheese'])"]
}
```

Absence is `NOT contains(ingredients, 'bread')`, and you can use variables in the comparisons: `contains_any(item_skus, {wanted})` with `"variables": {"wanted": ["JAF-004", "JAF-005"]}`.

A whole list can also be grouped by, compared with another list and counted distinctly: each list is read as the set of its distinct elements (so `length(item_skus)` counts distinct SKUs, while `count(item_skus.*)` counts every unit). Comparing a list with a scalar, or `sum(item_skus)`, is refused with an error naming the alternative, and `contains` on a text field is a substring test.

## Elements as rows

`ingredients.value` is a virtual field with one row per element, and `count(ingredients.*)` counts them. `count(orders.*)` grouped by an element counts the parents holding it:

```json
{
  "population": "orders",
  "dimensions": ["item_skus.value"],
  "metrics": [
    {"metric": "count(orders.*)", "alias": "orders_with_sku"},
    {"metric": "count(item_skus.*)", "alias": "units"}
  ]
}
```

## Collecting into a list

`collect(x)` returns the distinct non-NULL values of `x` per group-by cell, sorted, and `[]` for a cell with none. A collected list keeps its list semantics in a later stage:

```json
[
  {"name": "baskets", "population": "orders", "dimensions": ["customer_id"],
   "metrics": [{"metric": "collect(item_skus.value)", "alias": "skus"}]},
  {"population": "baskets", "metrics": ["count(*)"], "filter": ["contains(skus, 'JAF-005')"]}
]
```

The `collect` function also works on regular fields, for example to list the products of each type:

```json
{
  "population": "products",
  "dimensions": ["type"],
  "metrics": [{"metric": "collect(name)", "alias": "product_names"}]
}
```

It also collects a field of a joined dataset, one list per parent row. Here are each customer's order statuses, fewest orders first:

```json
{
  "population": "customers",
  "dimensions": ["name"],
  "metrics": [
    {"metric": "count(orders.*)", "alias": "order_count"},
    {"metric": "collect(orders.status)", "alias": "statuses"}
  ],
  "order": [{"field": "order_count", "direction": "asc"}, {"field": "name"}],
  "limit": 5
}
```

The [notebook](list_fields_nb.ipynb) runs every query on this page; the full rules are in [List fields](../../concepts/queries.md#list-fields) and [Lists](../../concepts/references.md#lists).

So next time you need to ask a question that involves a list-valued field, fear not: SLayer can do that for you!
