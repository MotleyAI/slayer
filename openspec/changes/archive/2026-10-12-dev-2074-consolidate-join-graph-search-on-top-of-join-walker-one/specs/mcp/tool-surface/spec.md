## ADDED Requirements

### Requirement: Cypher filters are time-bounded
A search `cypher_filter` SHALL execute under a fixed time limit and without blocking other requests served by
the same process while it runs. A filter exceeding the limit SHALL fail with an error stating that the
`cypher_filter` timed out and naming the limit. Concurrent filters SHALL never share one in-flight database
connection, and rebuilding the search graph SHALL wait for an in-flight filter to finish.

#### Scenario: A runaway variable-length traversal times out
- **WHEN** a search runs `MATCH (a)-[:JOINS*]->(b) RETURN DISTINCT b.id AS id` over a model graph with cycles
  that would take longer than the limit
- **THEN** the search fails within the limit with the timed-out error

#### Scenario: Other requests proceed during a slow filter
- **WHEN** a slow `cypher_filter` is running and another request is served by the same process
- **THEN** the other request completes without waiting for the filter

#### Scenario: Concurrent filters each return their own result
- **WHEN** two searches with different `cypher_filter` values run concurrently
- **THEN** each returns exactly the ids its own filter selects
