## 1. Tests first (pr-tests stage)

- [x] 1.1 `tests/test_dev2217_ossie_precedence.py`: the three probe models as native documents on in-process DuckDB — route (`b.name` × `a.id`, `c.a_id` unique → rows `(x, A1)`, `(y, A2)`), anchor tie (`a.b_id` × `c.b_id` → anchor `c`), anchor by precedence over fewer hops (`a.id` × `b.name` via `b → r → a` → anchor `a`, rows `(A1, p)`, `(A2, p)`), each under both conformances; plus "Two paths of Ossie steps stay ambiguous" and "Several anchors Ossie qualifies leave native rules in charge". Probe scripts are in this session's scratchpad only; rebuild the models in the test. Verify: fail before implementation.
- [x] 1.2 Oracle (`tests/test_dev2243_wide_table_oracle.py`): add the strict run of every case with `query.json` verbatim and the header (names and order) checked; keep the default-conformance `a*` run; flip the `e*` xfails; keep `a16_shared_dimensions_stores` a strict xfail naming DEV-2388 under both conformances. Keep the file in `.github/workflows/ossie-corpus-weekly.yml`'s pytest list (add any new strict-suite file there too). Verify: strict cases fail before implementation.
- [x] 1.3 `tests/test_dev2217_shape.py`: `E_INVALID_QUERY` for every native-only key, `null`, the list form, an expression entry, a non-identifier alias, a filter list, an entry with an extra property, a `SlayerQuery` instance with a native field, and raw inputs native conversion would rewrite or reject with a Pydantic error (legacy keys, `filters`, deprecated spellings); empty lists → `E_EMPTY_AGGREGATION_QUERY`.
- [x] 1.4 `tests/test_dev2217_errors.py`: every §8 code on the retail / stores fixtures beyond the corpus cases; precedence pairs (1>3, 3>7 incl. window-vs-unknown-name, 6>8, 9>13, 13>14, 16>17); diagnostics attributes for 13/14/16/17; the model-scoped restrictions raise their codes, not `UnevaluableEntityError`, including precedence against earlier rows.
- [ ] 1.5 `tests/test_dev2217_filter.py`: acceptance of each Ossie Layer 1 construct Mode B implements (comparison, BETWEEN, IN list, LIKE/ILIKE, IS [NOT] NULL, IS [NOT] DISTINCT FROM, CASE, arithmetic, CAST, ISO date literal vs a date field, each flagged scalar); rejection of each SLayer-only construct (`~`, `->>`, list functions/children, single-string IN, `matched()`, deprecated spellings, transforms, period and relative time tokens, unbound placeholder) with `E_INVALID_QUERY`, each still answering under `slayer`.
- [ ] 1.6 `tests/test_dev2217_output_names.py`: aliases name columns; `orders.id` + `customers.id` → `E_NAME_COLLISION` with no SQL executed; mixed-quote normalization (`foo`/`"FOO"` collide, `foo`/`"foo"` answer with labels `FOO`, `foo`); a quoted dotted field keeps its whole name; Ossie column order; native keys unchanged under `slayer`; an alias equal to a field name runs under both conformances (Ruling 1, dimension and metric).
- [x] 1.7 `tests/test_dev2217_appendix_b.py`: every Appendix B.1–B.5 example with exact rows or error, including B.4 unfiltered (`orders.total_revenue` + `tickets.ticket_count`, no dimensions → 650, 0); B.3 shared dimensions a strict xfail naming DEV-2388.
- [x] 1.8 `tests/test_dev2217_model_view.py`: one-to-many standalone relationship as a backward step; many-to-many → `E_NO_PATH`; a document relationship whose `SLAYER` entry declares one-to-many keeps `from → to`; a SLayer-only field and transform dataset metric are honoured.
- [x] 1.9 `tests/test_dev2217_surfaces.py`: engine, embedded and remote client (`query`, `query_sync`, `sql`, `explain`), REST (`?conformance=`, 400 body with `detail.code` and attributes, body read as the query alone), MCP (schema lists `conformance`; a call), CLI (`--conformance`, one stderr line, output columns) all reach the same binding and codes; an unknown conformance value is refused on each.
- [x] 1.10 `tests/test_dev2217_run_by_name.py`: saved Ossie-shaped query run strictly, with `refine`; explicit population → `E_INVALID_QUERY`; multi-stage → `E_INVALID_QUERY`; `variables` binding a filter placeholder; query-level `variables` part → `E_INVALID_QUERY`; explain and dry run judge first.
- [x] 1.11 Align existing tests the plan contradicts (approval per file at pr-tests): the computed-dimension name-collision test and the `MetricNameCollidesWithFieldError` suggestion tests (Ruling 1); any test pinning the MCP `query` schema's exact wrapper arguments; any routing/inference test pinning an ambiguity or anchor that Ossie precedence now settles.

## 2. Core types and shared helpers

- [x] 2.1 `OssieErrorCode` StrEnum and `OssieQueryError(SlayerError)` in `slayer/core` (code, dimension, filter, metric, anchor; stable-format first line `OssieQueryError: <code>: <summary>`); `Conformance` StrEnum. Verify: 1.4 / 1.9 type assertions pass.
- [x] 2.2 Extract the export relationship orientation (`slayer/osi/exporter.py` `_relationship_shape`) into one `slayer/core` helper giving a relationship's Ossie step (document relationships `from → to`); the exporter uses it. Verify: exporter suites unchanged; 1.8 passes.
- [x] 2.3 One Layer 1 identifier-normalization function (regular → upper-cased, quoted → exact). Verify: 1.6 normalization cases.

## 3. Ossie precedence in native resolution

- [x] 3.1 `slayer/engine/population/dimension_routing.py` `resolve_route`: a sole path of Ossie steps (a new hop filter beside `TO_ONE`, via 2.2) wins before the to-one and unique-route rules. Verify: 1.1 route cases; routing suites (`tests/test_dev2277_routing.py`, `tests/test_dev1743_resolution.py`, `tests/test_dev2212_home_routing.py`) stay green.
- [x] 3.2 `slayer/engine/population/inference.py`: the anchor Ossie qualifies uniquely (a determination item's dataset reaching every other's along Ossie steps, and viable) is the dimension anchor before the fewest-hops rule. Verify: 1.1 anchor cases; population suites stay green.
- [x] 3.3 Apply the approved arc42 wording (design decision 36) verbatim to `architecture/semantics.arc42.md`. Verify: `la-arch-check` green.

## 4. Model-scoped metric analysis

- [x] 4.1 `slayer/storage/semantic_models.py`: one classifier computes, after decomposition and `SLAYER`-entry restoration, each `ModelScopedMetric`'s components (home datasets) and Ossie problem (code + reason); `load_warning` derives from it with today's messages. Verify: 1.4 model-scoped cases; DEV-2243 suites unchanged.

## 5. Parser support

- [x] 5.1 Ossie-conformance metadata on every Mode B construct: node kinds and operators, and an `ossie` flag on `SCALAR_SPECS` entries Ossie Layer 1 defines with SLayer's meaning (check each against `expression_language.md`). Verify: 1.5.
- [x] 5.2 The parser reports each non-Ossie spelling it rewrites as a typed parse diagnostic on the channel legacy spellings use. Verify: 1.5 spelling cases.

## 6. The strict binding

- [x] 6.1 New engine module: raw-input shape check (row 1, incl. the filter visitor of 5.1/5.2 and recording a window for row 7) and row 2, run at the top of `execute` before any conversion; stored stages via a typed projection stripping `name`, `version` and an inferred `population`. Verify: 1.3.
- [x] 6.2 Model identification, then rows 3–17 as an ordered table over one model-view load (datasets, relationships via 2.2, model-scoped analysis of 4.1); diagnostics per design decision 15. Verify: 1.4, 1.7, 1.8.
- [x] 6.3 Wire `conformance` through `SlayerQueryEngine.execute` (incl. run-by-name after the `refine` merge, `variables` substitution before judging, explain / dry run / cache judging first). Verify: 1.10.
- [x] 6.4 Response relabelling at the end of `execute`: rows, `columns`, attributes keyed by output names (labels per design decision 17), each entry's native key taken from what the engine reports for it. Verify: 1.6, oracle headers.

## 7. Ruling 1

- [x] 7.1 Lift the computed-dimension `NameCollisionError` for user aliases in `slayer/engine/compile/projection.py` and `MetricNameCollidesWithFieldError`; remove the class if nothing else raises it (confirm with LSP references). Verify: 1.6 Ruling 1 cases; `queries/computed-dimensions` scenario.

## 8. Surfaces

- [x] 8.1 REST: `conformance` URL parameter on `POST /query`; `OssieQueryError` → 400 with structured `detail`. Verify: 1.9 REST.
- [x] 8.2 Client: `conformance` on `query`/`query_sync`/`sql`/`explain` (+ sync); remote mode sends the parameter and rebuilds `OssieQueryError` from a 400 body carrying a code. Verify: 1.9 client.
- [x] 8.3 MCP: `conformance` wrapper argument on `query` (enum; tool description). Verify: 1.9 MCP; `mcp/query-tool` scenarios.
- [x] 8.4 CLI: `slayer query --conformance`. Verify: 1.9 CLI; `cli/commands` scenarios.

## 9. Docs and harnesses

- [x] 9.1 `docs/interfaces/ossie-layer-3.md` (selection per surface, shape, codes with precedence, filter grammar and the DEV-2282 limitation, output names, run-by-name, errors on the wire) linked in `zensical.toml`'s Interfaces nav; update `docs/reference/python-client.md`, `rest-api.md`, `mcp.md`, `cli.md`; a pointer from `docs/concepts/queries.md`; an upgrade note for Ossie precedence and Ruling 1 under `docs/upgrading/`.
- [x] 9.2 `architecture/index.yaml`: `queries.touches` gains `python.surfaces`.
- [x] 9.3 Gates: local test scope (`la-affected-tests`), `poetry run ruff check slayer/ tests/`, `la-typecheck`, `la-arch-check`, `npx -y likec4@1.47.0 validate architecture`, `openspec validate dev-2217-strict-ossie-layer-3-binding-conformanceossie --strict`. If the bundle asks for a `.c4` change, stop and ask.

## 10. Review fixes

- [x] 10.1 Export one relationship per join; a relationship's `SLAYER` entry declaring several declarations loads as the plain relationship with a load warning. Verify: the two new scenarios.
