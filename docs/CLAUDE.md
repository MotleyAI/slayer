# Docs CLAUDE.md

## Style Rules

- Use JSON/dict syntax for all query objects in docs and examples — not Python class constructors. 
Write `{"name": "stores.name"}` not `ColumnRef(name="stores.name")`, `{"formula": "count"}` not `Field(formula="count")`. 
This keeps examples portable across Python, REST API, and MCP interfaces.
