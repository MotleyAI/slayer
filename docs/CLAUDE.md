# Docs CLAUDE.md

## Style Rules

- Use JSON/dict syntax for all query objects in docs and examples — not Python class constructors. 
Write `{"field": "stores.name"}` not `QueryDimension(field="stores.name")`, `{"metric": "count(*)", "alias": "n"}` not `QueryMetric(metric="count(*)", alias="n")`. 
This keeps examples portable across Python, REST API, and MCP interfaces.
- `tests/test_doc_examples.py` runs every query in README, `docs/index.md`, `getting-started/`, `examples/` and `reference/` on the demo: put `!!! note "Illustrative schema"` before a query on a hypothetical schema, and `<!-- doc-check: not-a-query -->` before a block that mentions `population` without being a query.
