## Why

Ossie's Layer 3 conformance (`wide_table_semantics.md` §9) is strict: an engine may answer a query Layer 3 rejects only when the query or the model asks for it explicitly, it conforms only through every binding it offers, and unsupported JSON keys are malformed. SLayer's native query language is a superset that answers many such queries with warnings, so it is not offered as a Layer 3 binding. A separate strict binding, selected outside the query object, gives SLayer a conformant Layer 3 interface (the 2.0 compliance statement, DEV-2220, reads "conformant under `conformance="ossie-l3"`").

Two native routing rules also break the superset itself: a reversed hop native proves to-one can make an Ossie-unambiguous route ambiguous, and it can win the dimension anchor on hop count over the one anchor Ossie qualifies. Ossie-valid queries are then refused or answered with other rows, under every conformance.

## What Changes

- A `conformance` argument, `"slayer"` (default) or `"ossie-l3"`, on `engine.execute`, `SlayerClient.query` / `query_sync` / `sql` / `explain` (and sync twins), REST `POST /query?conformance=…`, the MCP `query` tool, and `slayer query --conformance`.
- Under `"ossie-l3"`: only the §3.7 query shape is accepted; the 17 §8 error codes are raised in §8's precedence with the required diagnostics; the filter must be an OSSIE_SQL_2026 expression; admitted queries are answered by the native root-less row rule (= Ossie §5 lowering); result columns carry Ossie output names (alias, else the reference's last segment), with `E_NAME_COLLISION` raised before the query runs. Saved queries run by name are judged and answered the same way. A typed `OssieQueryError` carries the code on every surface.
- **Ossie precedence** in native resolution: a route Ossie determines uniquely (a sole path of declared many-to-one steps) and an anchor Ossie qualifies uniquely are taken over every other candidate. **BREAKING** (narrow): on models where a reversed provably-to-one hop gave another dimension-anchor candidate fewer hops than Ossie's unique anchor, queries depending on the dimension anchor (dimension-only, raw rows, coarser-grain extension, the reported `population`) now use Ossie's anchor; route ambiguities Ossie settles now resolve instead of failing.
- A user-written alias may equal a field name under every conformance: the computed-dimension name-collision refusal and `MetricNameCollidesWithFieldError` for user aliases are lifted. **BREAKING** only in that formerly refused queries now run.
- Model-scoped metrics get one typed analysis (components, home datasets, Ossie problem code) computed once after load; `load_warning` derives from it.
- Upstream's wide-table compliance cases run under strict (error cases included) and, for the row cases, also under the default.

## Capabilities

### New Capabilities
- `queries/ossie-l3-binding`: the strict Ossie Layer 3 binding — selection on every surface, shape, error codes and precedence, model view and paths, filter grammar, output names, run-by-name, variables, explain, errors on the wire.

### Modified Capabilities
- `queries/references`: the anchor routing rule gains Ossie precedence for routes.
- `queries/population`: the dimension anchor gains Ossie precedence.
- `queries/computed-dimensions`: a named dimension whose alias equals a host field is accepted.
- `queries/type-errors`: the measure-name collision requirement is removed and `MetricNameCollidesWithFieldError` leaves the error families.
- `mcp/query-tool`: the `query` tool's wrapper arguments gain `conformance`.
- `cli/commands`: `slayer query` gains `--conformance`.
- `models/semantic-model-documents`: the wide-table oracle runs every case under strict.

## Impact

- Code: `slayer/engine` (new strict-binding module, `query_engine.execute`, `population/dimension_routing.py`, `population/inference.py`, `compile/projection.py`), `slayer/core` (error class and code enum, relationship-orientation helper shared with `slayer/osi/exporter.py`, Mode B conformance metadata and parse diagnostics in `syntax.py` / `keys.py`), `slayer/storage/semantic_models.py` (typed model-scoped metric analysis), `slayer/api/server.py`, `slayer/mcp/server.py`, `slayer/client/slayer_client.py`, `slayer/cli.py`.
- Harnesses: `architecture/semantics.arc42.md` (Ossie precedence, approved wording), `architecture/index.yaml` (`queries.touches` gains `python.surfaces`).
- Tests: new `tests/test_dev2217_*.py`; `tests/test_dev2243_wide_table_oracle.py` gains the strict run.
- Docs: new `docs/interfaces/ossie-layer-3.md` (nav), reference pages for the client, REST, MCP and CLI, a pointer from `docs/concepts/queries.md`.
