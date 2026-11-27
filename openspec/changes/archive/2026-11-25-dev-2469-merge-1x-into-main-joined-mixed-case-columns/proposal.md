## Why

#653 (DEV-2405) fixed two bugs on `1.x`: a joined dataset's mixed-case field was unquoted in every clause but `WHERE`, so case-folding dialects (Postgres, Snowflake, Oracle) failed with an undefined-column error; and filter scopes minted join aliases from a fresh allocator, so a two-hop path alias colliding with a dataset literally named `<a>__<b>` made `WHERE` read the wrong table. Both reproduce on `main`, which additionally reads a joined dataset through an untrusted to-one hop's `SELECT DISTINCT` projection whose inner base-table read is unquoted — the read Postgres rejects first. Merging `1.x` into `main` brings the fix over and keeps `main` containing every `1.x` commit.

## What Changes

- Merge `origin/1.x` (with #653) into `main`; never rebase.
- Every plain field reference renders through `ScopeFrame.resolve`: the generator's second renderer (`_joined_or_local_dim_expr`) and the separate join-path collector (`_collect_joined_paths_for_base`) are deleted; base-select row fields, and order-only row fields, resolve through the host scope before the `FROM` is built.
- A joined mixed-case field is quoted in every clause, including the inner reads of the distinct projection; lowercase fields emit byte-identical SQL.
- A plain field whose relationship path or leaf does not resolve fails closed with `UnknownReferenceError` at emission in every clause, filters and semi-join `EXISTS` included.
- Filter, semi-join and order scopes mint join aliases through the generation allocator, as the `FROM` does.
- `openspec/forward-merge-ledger.yaml` lists #653's requirement, which `main` words in its own vocabulary.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `sql/statement-assembly`: adds the requirement that a column reference renders the same in every clause of one relational scope, including an untrusted hop's distinct projection.

## Impact

- Code: `slayer/sql/generator.py`, `slayer/sql/scope.py`; `slayer/sql/untrusted_projection.py` unchanged.
- Tests: `tests/test_joined_column_quoting.py` (new; dry-run emission on Postgres and Snowflake, Postgres execution via pytest-postgresql), `TestFilterJoinAliasMatchesFrom` in `tests/test_sql_generator.py`; tests aligned with per-file approval.
- OpenSpec: `openspec/forward-merge-ledger.yaml`; #653's archived DEV-2405 folder joins `main`'s archive.
- No API, wire, storage, docs or architecture change.
