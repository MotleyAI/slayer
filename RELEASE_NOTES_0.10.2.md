# SLayer 0.10.2

A release that makes time-based dimensions easier to write, tightens how source models are validated, and fixes cross-model counts when some entities have no matching rows.

## Functional time granularity in dimensions

You can now write a time granularity as a function call directly in your query's `dimensions`, for example `"month(orders.created_at)"`, instead of spelling out a `TimeDimension`. The granularity is any of the nine supported values (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`), matched case-insensitively, and the column can be bare or dotted. The same string form is accepted in `time_dimensions` and in `order`, and a mistyped one gives you a clear error naming the valid granularities instead of a bare type error. When you bucket the same column at more than one granularity, the result keys are disambiguated (for example `orders.created_at.month`).

Breaking: a custom aggregation whose name matches one of the granularity values is now rejected when you save the model, so a granularity can never shadow an aggregation.

## Typed source models

`source_model` on a query is now a proper union of a model name (a string), an inline model extension, or a full inline model, and it is validated when you build the query rather than failing later. Malformed values are caught early, and over the REST API a bad `source_model` is rejected as an HTTP 422 like any other invalid field. `TimeDimension` also now accepts `column` as an alias for `dimension`.

Breaking: an inline model extension now rejects unknown keys, so a payload carrying a key that used to be silently ignored - notably the never-implemented `filters` key - now fails loudly at construction.

## Correct cross-model counts under "associate"

When you aggregate one model by a dimension from another under `to_many_handling: "associate"`, home entities that have no matching population row - for example a customer with no orders - are now counted in the cells their own join path reaches. Previously they were dropped, giving totals lower than the true value. Reachable filters now also apply directly on the association's joins, so a filter that shares a hop with an association dimension is matched against the same related row.
