## ADDED Requirements

### Requirement: Stored AGG calls in field expressions are unwrapped
On load, a stored model written before this change whose field's Mode-B `expression` calls `AGG(x)` in any case SHALL have each such call replaced by its argument `x`, with one logged warning naming the model and the field. A query reading the field SHALL return what it returned before the change. A call the earlier `agg` upgrade (`Stored agg aggregations are renamed`) already rewrote to `agg_aggregation(` SHALL be left unchanged, and Mode-A text SHALL be left unchanged.

#### Scenario: A stored AGG field keeps its value
- **WHEN** a stored model written by the previous release has the field `a` with expression `AGG(amount) + 1` and a query reads `orders.a`
- **THEN** after load the field's expression is `amount + 1`, a warning names the model and `a`, and the query returns `amount + 1` for each row as before

#### Scenario: A model written before the agg upgrade
- **WHEN** a stored model written before the `agg` upgrade has the metric `m` with formula `agg(amount)`
- **THEN** after load the metric's expression is `agg_aggregation(amount)`, as that upgrade defines, and this upgrade leaves it unchanged
