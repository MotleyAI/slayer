# List Columns

Suppose your database stores contractors, and each contractor has a variable-length list of skills. If your database supports array-valued columns, these are very likely stored in such a column rather than in a separate table with a foreign key to contractors.

Now suppose you want to filter for contractors who have two specific skills — will your semantic layer allow that? How about counting the contractors that have each particular skill? Or showing the complete list of skills available from contractors in each city?

This is now easy in SLayer. SLayer types such a column as `ARRAY` with an `element_type` (recognizing these automatically when ingesting schemas), and gives it three tools:

- the **list functions** test a whole list;
- the list's **elements** are a child dataset you can filter, group and aggregate without fanning out the parent;
- **`collect(x)`** aggregates values into a list.

Let's look at some examples, using the Jaffle Shop demo's `products.ingredients` (the supplies a product is made from) and `orders.item_skus` (the SKU of each unit on an order, repeats kept).

## Testing a whole list

These functions test what an array-valued column contains: `contains(list, x)` (a single value), `contains_any(list, [...])`, `contains_all(list, [...])` and `length(list)` work as you'd expect:

```json
{
  "source_model": "products",
  "dimensions": ["name", {"expression": "length(ingredients)", "name": "ingredient_count"}],
  "filters": ["contains_all(ingredients, ['bread', 'cheese'])"]
}
```

Absence is `NOT contains(ingredients, 'bread')`, and you can use variables in the comparisons: `contains_any(item_skus, {wanted})` with `"variables": {"wanted": ["JAF-004", "JAF-005"]}`.

A list is not a scalar: grouping by it, comparing it with `=`, or `sum(item_skus)` are refused with an error naming the alternative, and `contains` on a text column points at `LIKE`. Some of these will be made possible in SLayer 2.0.

## Elements as rows

`ingredients.value` is a virtual column with one row per element, and `count(ingredients.*)` counts them. With `to_many_handling: "associate"`, `count(orders.*)` grouped by an element counts the parents holding it:

```json
{
  "source_model": "orders",
  "dimensions": ["item_skus.value"],
  "measures": [
    {"formula": "count(orders.*)", "name": "orders_with_sku"},
    {"formula": "count(item_skus.*)", "name": "units"}
  ],
  "to_many_handling": "associate"
}
```

## Collecting into a list

`collect(x)` returns the distinct non-NULL values of `x` per group-by cell, sorted, and `[]` for a cell with none. A collected list keeps its list semantics in a later stage:

```json
[
  {"name": "baskets", "source_model": "orders", "dimensions": ["customer_id"],
   "measures": [{"formula": "collect(item_skus.value)", "name": "skus"}]},
  {"source_model": "baskets", "measures": ["count(*)"], "filters": ["contains(skus, 'JAF-005')"]}
]
```

The `collect` function also works on regular columns, for example to list the products of each type:

```json
{
  "source_model": "products",
  "dimensions": ["type"],
  "measures": [{"formula": "collect(name)", "name": "product_names"}]
}
```

The [notebook](list_columns_nb.ipynb) runs every query on this page; the full rules are in [List columns](../../concepts/queries.md#list-columns) and [Lists](../../concepts/references.md#lists).

So next time you need to ask a question that involves a list-valued column, fear not: SLayer can do that for you!
