# Docs CLAUDE.md

## Style Rules

- Use JSON/dict syntax for all query objects in docs and examples — not Python class constructors. 
Write `{"name": "stores.name"}` not `ColumnRef(name="stores.name")`, `{"formula": "count"}` not `Field(formula="count")`. 
This keeps examples portable across Python, REST API, and MCP interfaces.
- `tests/test_doc_examples.py` runs every query in README, `docs/index.md`, `getting-started/`, `examples/` and `reference/` on the demo: put `!!! note "Illustrative schema"` before a query on a hypothetical schema, and `<!-- doc-check: not-a-query -->` before a block that mentions `source_model` without being a query.
