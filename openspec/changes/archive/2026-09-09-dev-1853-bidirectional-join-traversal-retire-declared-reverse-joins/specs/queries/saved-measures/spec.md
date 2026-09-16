# queries/saved-measures Delta

## MODIFIED Requirements

### Requirement: Round-trip expansions are rejected
A dotted saved-measure expansion whose re-anchored references cross a join back toward a model already on the host-to-target join chain SHALL fail with an error naming the saved measure and the revisited model — matching the behavior of the identical hand-written dotted path, which is rejected as circular.

#### Scenario: Target measure crossing back to the host errors
- WHEN `customers.order_total` is saved as `orders.amount:sum` (over the reverse orientation of the declared `orders → customers` join) and an `orders`-rooted query references `customers.order_total`
- THEN the query fails with an error naming `order_total` on `customers` and the revisited `orders` model, not with wrong or double-counted values
