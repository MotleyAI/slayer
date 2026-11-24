## ADDED Requirements

### Requirement: Relationships stored before trust upgrade as trusted
Loading a stored model, saved query or memory written before relationships carried
`trusted` SHALL set `trusted: true` on every relationship it stores, its query-extension
relationships and those of its backing queries included, so its results and SQL are
unchanged. Stored semantic-model documents are not upgraded.

#### Scenario: A stored model keeps its inline join
- **WHEN** a model stored at the previous version declares `orders → customers` as
  `many_to_one`, and the data holds customer C1 twice
- **THEN** after loading, the relationship is trusted and `orders.total_revenue` by
  `customers.region` returns EAST 650 as before

#### Scenario: A stored query's extension relationship is trusted
- **WHEN** a saved query stored at the previous version declares a join in its query extension
- **THEN** after loading, that relationship is trusted

#### Scenario: A stored memory's query is upgraded
- **WHEN** a memory stored at the previous version carries a query declaring a join in its
  query extension
- **THEN** after loading, that relationship is trusted
