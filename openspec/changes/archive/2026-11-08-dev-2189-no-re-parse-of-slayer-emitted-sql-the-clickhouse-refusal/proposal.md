## Why

Seven production paths render a SLayer statement to SQL text and then parse that text back:
the ClickHouse ARRAY-JOIN refusal, response `expected_columns`, the session-policy (RLS)
rewrite on four statement kinds, cache invalidation, and the type probe of a query-backed
model. python.sql §3.1 forbids text round-trips of emitted SQL (dotted aliases corrupt on
re-parse), and the existing harness law cannot see them: it intercepts renders inside
builders, never parses after the statement is finished.

## What Changes

- The engine carries the statement syntax tree from generation through the ClickHouse
  refusal, the policy rewrite, response metadata and cache invalidation, and renders it to
  text once, after the last rewrite.
- `correlated_subquery_refusal` takes the statement tree, not SQL text.
- **BREAKING (internal API)**: `apply_session_policy` takes and returns a syntax tree; the
  text entry point is removed (no caller passes SQL SLayer did not generate). Every
  SLayer-built statement the policy scopes — data query, refresh-key scan, join-cardinality
  side stats, type probe — is rewritten before its single render.
- One physical-table walk serves both the policy and the cache; the cache reads the
  post-policy tree's tables.
- Response `expected_columns` come from the statement tree's outer projection;
  `expected_columns_from_sql` is removed.
- The type probe of a query-backed model plans from the model's statement tree without
  rendering the backing statement.
- A runtime harness law fails any execution that parses a statement it rendered (equality
  match), outside the emitted-text validators and the value layer tracked by DEV-1972.
- Policy-scoped SQL now goes through the dialect's normal single render (pretty-printed,
  fitted, scope-validated); its text may differ from today's only in whitespace.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `sql/statement-assembly`: "Statements are composed as a syntax tree and rendered once"
  extends to every consumer of a finished statement — no parse-back for any purpose.
- `sql/forced-filters`: adds a requirement that forced filters rewrite the statement tree
  before its single render, with parity to the previous rewrite and every fail-closed
  refusal preserved on every scoped statement kind.

## Impact

- `slayer/engine/query_engine.py` (`_prepare_pipeline`, `_plan_and_render_statement`,
  `_Rendered`, `_Prepared`, `_apply_policy`, `_require_correlated_subqueries`, refresh-key
  scan, `get_column_types`, `_expand_query_backed_model`, `_side_stats_sql`).
- `slayer/sql/generator.py` (statement build vs. single render), `slayer/sql/session_policy.py`,
  `slayer/sql/scope_check.py` (post-RLS validation), `slayer/sql/dialects/base.py` +
  `clickhouse.py`, `slayer/engine/response_meta.py`, `slayer/engine/cache.py`.
- Tests: new runtime law + self-tests, RLS parity and fail-closed matrices, cache
  applicability; mechanical input changes in tests that feed SQL literals to the changed
  functions.
- architecture/python.sql.arc42.md §3.1 gains `[enforced: test:tests/test_law_no_emitted_reparse.py]`.
