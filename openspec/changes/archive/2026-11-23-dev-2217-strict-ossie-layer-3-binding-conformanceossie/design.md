## Context

See proposal.md for the motivation. Normative sources: Ossie `core-spec/wide_table_semantics.md` (Layer 3; §§3, 4, 5, 8, 9 normative) and `core-spec/expression_language.md` (Layer 1), at the vendored commit `94cf86b` (`tests/fixtures/ossie_corpus/provenance.yaml`). Upstream's `compliance/wide_table` cases are the oracle. Native evaluation already implements the §5 lowering for root-less queries (DEV-2212); on the e-cases native answers, raises native errors, or refuses differently (probed on `a5b1e5477`). Model-scoped metrics resolve bare (DEV-2243). apache/ossie#778 (dataset-scoped metric columns named `dataset.metric`) is open, so §3.3's last-segment rule holds for metrics too.

Applicable principles: system §3 P4 (SQL as AST), P5 (async), P6 (Pydantic), P8 (one Mode B reader), P9 (one value writer); core P5 (typed, stably formatted errors), P6 (structured warnings); engine P1 (typed pipeline), P3 (storage read once per stage of work), P9 (applies to algebra type errors only — strict refusals are binding-conformance errors, not `QueryTypeError`s); semantics §4 P1 (routing), P3 (logical names), P4 (Ossie superset; strict only refuses and relabels, never changes an admitted answer), axiom 12.2 (dimension anchor). Spec attachment: `queries/ossie-l3-binding` under the cross-cutting `queries` group, whose `touches` gains `python.surfaces`.

## Goals / Non-Goals

**Goals:**
- One strict checker evaluating §8 as an ordered table; one response relabelling; one shared relationship-orientation helper; one typed model-scoped metric analysis.
- Ossie precedence in native routing and anchor selection, so strict admission implies native answers with Ossie's rows by construction.

**Non-Goals:**
- The SQL API's conformance option (DEV-2103).
- Ossie Layer 1 functions Mode B does not implement yet (DEV-2282); under strict they are `E_INVALID_QUERY` (§3.5).
- The `a16_shared_dimensions_stores` / Appendix B.3 shared-dimension planner fix (DEV-2388); strict xfail until then.
- Respelling native result keys (DEV-2279, canceled).

## Decisions

The decisions log; each entry gives the choice, the options weighed where there were several, and who took it.

1. **Selection.** A `conformance` argument (`slayer` default, `ossie-l3`), never a query field: engine `execute`, client `query`/`sql`/`explain` (+ sync), REST, MCP `query` wrapper argument; the SQL API later (DEV-2103). Unversioned. (user)
2. **REST carries it as a URL parameter** (`POST /query?conformance=ossie-l3`), as the issue's "request parameter"; the body stays the query; existing body wrapper keys keep working. (default)
3. **CLI** `slayer query --conformance` added, extending the issue's surface list. (user)
4. **Model view.** The model is the identified datasource with all it holds, SLayer-only content honoured: it is stored as `SLAYER` `nonportable_extensions`, §9's example of the model requesting behaviour explicitly. Fields belong to their declaring dataset; a dataset-scoped metric is one component, whatever its expression (§5.2). (default; Ossie §9)
5. **Steps.** A document relationship is a step `from` → `to` whatever its `SLAYER` entry declares; any other relationship as Ossie export orients it (many-to-one/one-to-one forward, one-to-many backward, undetermined in the direction its keys prove, else none); many-to-many and self-relationships never. The export orientation is one helper shared by the exporter and the binding. (default; (codex) for the document-relationship clause)
6. **Filter grammar.** OSSIE_SQL_2026 only (§1.4; §9 forbids answering beyond Layer 3 without explicit request, and an undeclared dialect is none). (default; Ossie decides)
7. **Filter check on the one Mode B parse** (system P8). Ossie-conformance metadata covers every Mode B construct — node kinds and operators, plus a flag on the one scalar library (`SCALAR_SPECS`, never a second list); one exhaustive visitor with a fail-closed tail checks the parse. (default; (codex) for coverage beyond scalars)
8. **Spellings.** The parser reports each non-Ossie spelling it rewrites as a typed parse diagnostic (the channel legacy spellings use); strict refuses on any. No source-spelling identity on parse nodes (engine P2). (codex)
9. **Unsupported Ossie constructs** are `E_INVALID_QUERY` under §3.5 until DEV-2282, documented. (default)
10. **References** resolve as `dataset.field`, `dataset.metric` or a bare model-scoped metric; three or more segments and bare fields resolve to nothing (`E_NAME_NOT_FOUND`); non-reference text is `E_INVALID_QUERY`. (default)
11. **Alias** optional in the object form; must be a Layer 1 identifier, regular or double-quoted. (default)
12. **Model identification** after rows 1–2: `data_source`, else native resolution; ambiguity raises the native error, not a §8 code (§3.7: "The binding identifies the model"). (default)
13. **§8 as an ordered table**, each row judged across the whole query, first that holds wins. (Ossie §8)
14. **Raw input first.** Row 1 judges the input as submitted, at the top of `execute`, before `_validated_run_input`, `SlayerQuery.model_validate`, migrations and vocabulary conversion; stored stages go through a typed projection stripping only enumerated bookkeeping, then the same checker. (codex)
15. **Diagnostics** name the first offending dimension (or the filter) in query order and the first failing metric in query order (or the anchor); `E_AMBIGUOUS_PATH` names the datasets and paths. Exposed as attributes. (default)
16. **Output names by relabelling the response** once, at the end of `execute` (rows, columns, attributes); every surface consumes that response. Each entry's native key comes from what the engine reports for it, never from splitting strings. Alias injection into the native query was considered and dropped: it changes native input (aliases are names during model-scoped expansion and binding). (codex; the owner's 2026-11-21 mechanism)
17. **Output-name spelling**: alias with quotes removed, else the model's spelling of the parsed reference's last segment. Where two distinct output names would give one label, a regular identifier's column is labelled by its normalized spelling. (default; §3.3's SHOULD)
18. **Normalization** is one Layer 1 function: regular identifiers upper-cased, quoted taken exactly (`foo` collides with `"FOO"`, not `"foo"`). (codex)
19. **Ruling 1**: a user-written alias may equal a field name under every conformance; the computed-dimension refusal and `MetricNameCollidesWithFieldError` go; `CanonicalAliasShadowsFieldError` stays. (user, 2026-11-21 comment)
20. **Ruling 2**: `E_NAME_COLLISION` after identifier normalization, the last segment from the parsed reference. (user, 2026-11-21 comment)
21. **apache/ossie#778 open** → metrics collapse to the last segment. (user: follow Ossie)
22. **Variables**: the `variables` wrapper stays under strict, placeholders substituted as literals before judging; a query-level `variables` part is `E_INVALID_QUERY`. (default)
23. **Run-by-name**: one stage, judged after the `refine` merge; `name`, `version` and an inferred `population` ignored; a one-condition stored filter is the filter string. (default)
24. **Explain / dry run / cache** judge first; explain exposes the admitted lowering (§9 SHOULD). (default)
25. **Errors**: one `OssieQueryError(SlayerError)` with an `OssieErrorCode` StrEnum (17 values) and `dimension` / `filter` / `metric` / `anchor`; first line `OssieQueryError: <code>: <summary>` via the stable formatter; REST 400 with a structured `detail`; the remote client rebuilds it; MCP and CLI print its text. (default; core P5)
26. **Not `QueryTypeError`**: binding-conformance refusals, outside engine P9. (default)
27. **Model-scoped metric analysis**: one classifier computes, after decomposition and restoration finalize the metric scope (document and `SLAYER`-entry metrics alike), each metric's components with home datasets and its Ossie problem (code + reason), independent of `load_warning`, which derives from it; the checker reads it before any `ModelScope.tree()` call can raise. (default; (codex) for timing and independence)
28. **A native error after admission** propagates unchanged (fail closed, S5). (default)
29. **Oracle**: every case under strict with `query.json` verbatim and the header (names, order) checked; `a*` also under the default; stays in the weekly upstream-corpus workflow. (default; DEV-2243 comment on this issue)
30. **`a16` and the Appendix B.3 shared-dimension test** are strict xfails against DEV-2388 under strict too. (user)
31. **Appendix B.4's unfiltered two-metric no-dimension query** gets its own test. (default)
32. **Docs page** `docs/interfaces/ossie-layer-3.md` in the nav's Interfaces section. (default)
33. **`index.yaml`**: `queries.touches` gains `python.surfaces`. (default)
34. **Empty `dimensions`/`metrics` lists** count as absent (row 2); one unaliased reference twice is row 9. (default / Ossie)
35. **Ossie precedence in native resolution, in this slice.** Native routing crosses hops it proves to-one in reverse; probes showed it refusing an Ossie-unambiguous route (`b.name` × `a.id` with `c.a_id` unique), tying anchors Ossie separates (`a.b_id` × `c.b_id`), and picking a different anchor by hop count with no tie (`a.id` × `b.name` via `b → r → a`). A route Ossie determines uniquely and an anchor Ossie qualifies uniquely take precedence. For routes this only replaces "ambiguous" refusals (an Ossie path is to-one, so native's unique-route and sole-to-one rules can only pick it); for anchors it can change answers of queries depending on the dimension anchor on such models — a 2.0 meaning change. Alternatives: forward-first search (equivalent outcomes), shortest route (breaks the superset), strict-only path constraints (default conformance stays non-superset), a separate issue (strict xfail meanwhile). (user)
36. **Approved arc42 wording**, applied verbatim to `architecture/semantics.arc42.md` (user, approved wording):
    - §4 Principle 4, appended line: `   **Ossie precedence**: a route or anchor Ossie determines uniquely — a sole path of declared many-to-one steps, a sole anchor Ossie qualifies — is taken over every other candidate. [enforced: test:tests/test_dev2217_ossie_precedence.py]`
    - §4 Principle 1: `datasets: by its only to-one path, else its only route, else the reference is` / `refused as ambiguous.` → `datasets: by its sole Ossie path (Ossie precedence, Principle 4), else its only` / `to-one path, else its only route, else the reference is refused as ambiguous.`
    - Axiom 12.2: `    - **12.2 Dimension anchor.** `A` is the smallest dataset determining every` / `      dimension, read from dimensions only, an unproven hop counting by` → `    - **12.2 Dimension anchor.** `A` is the anchor Ossie qualifies uniquely` / `      (Ossie precedence, Principle 4), else the smallest dataset determining every` / `      dimension, read from dimensions only, an unproven hop counting by`
37. **Codex finding #1 (do DEV-2388 here) rejected**: the user ruled `a16` a strict xfail against DEV-2388. rejected (codex)
38. **Codex finding #10 folded**: tests for raw inputs native conversion would rewrite, mixed-quote collisions, and the reversed-hop routing cases (now passing via decision 35). (codex)
39. **One Ossie relationship per native edge.** Codex: the strict checks counted one exported relationship's restored declarations as separate steps. Root cause: the export merged same-shape joins into one relationship while native keeps them as parallel edges (`models/join-traversal`), so the two disagreed on paths. Export never merges, and a `SLAYER` entry restores one relationship, so path counts agree by construction, native stays a superset, and DEV-2219's parallel-edges ruling holds. Collapsing same-shape edges in native routing was rejected: it overturns that ruling. (user)

## Risks / Trade-offs

- [Anchor precedence changes answers on reversed-hop models] → limited to queries depending on the dimension anchor where a reversed provably-to-one hop beat Ossie's unique anchor; pinned by `tests/test_dev2217_ossie_precedence.py`; called out in the upgrade notes. Saved queries keep their pinned anchors.
- [Mode B constructs mis-flagged as Ossie] → the visitor fails closed on unflagged kinds; acceptance and rejection tests per construct.
- [Native disagrees with an admitted query for another reason (e.g. DEV-2388)] → fails closed with the native error; strict xfails name the issue.
- [Response relabelling misses a carrier of keys] → relabelling happens once on `SlayerResponse` before any surface reads it; surface tests check columns on REST, MCP, CLI and the client DataFrame.

## Migration Plan

No stored data changes. `MetricNameCollidesWithFieldError` disappears from the error families (its queries now run). Ossie precedence ships in 2.0 with an upgrade note.
