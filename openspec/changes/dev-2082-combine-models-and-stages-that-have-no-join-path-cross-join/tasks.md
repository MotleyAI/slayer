## 1. Tests first (pr-tests)

- [x] 1.1 Write `tests/test_unrelated_datasets.py` covering every scenario of `specs/queries/unrelated-datasets/spec.md` on in-process SQLite (unit tests, not `integration`), and verify each fails on the current code for the right reason
- [x] 1.2 Strengthen the no-explosion scenarios per design D1/D5: sentinel sums with non-trivial row counts, each value checked against the same measure queried alone, two aggregates from one dataset, several datasets in one expression, empty dataset, all three modes; SQL-shape assertions parse the emitted SQL with sqlglot and check each unrelated table occurs only in its own producers and uncorrelated `EXISTS`; verify they fail today
- [x] 1.3 Add the root-recommendation scenarios of `specs/queries/population/spec.md` (unrelated attachment, unrelated column, attachments alone) next to the existing recommendation tests, plus the `unrelated` field on the REST / CLI / MCP / client outputs; verify they fail today
- [x] 1.4 Add the MCP scenarios of `specs/mcp/tool-surface/spec.md`: the unrelated-model marker owned only by `measures` (drift-guard table), the unrelated-model ratio idiom, budgets intact; verify they fail today
- [x] 1.5 Add the RLS scenario (a tenant policy scopes the unrelated dataset's producer) and the access-context scenario (a hidden model is reported as unknown); verify they fail today

## 2. Product hop and resolution

- [ ] 2.1 Add the typed product-hop discriminator (design D1), move the spine product and detached edges onto it, and make join rendering, rerooting, association building, row scopes and semi-join correlation fail closed on an unrelated product hop; verify the full existing suite stays green
- [x] 2.2 Add the pre-binding collection of unresolved first-hop dataset names into the binding bundle (design D4); verify the "references in every position" and "names that are not reachable datasets" scenarios
- [x] 2.3 Mint the unrelated product hop in strict first-hop resolution only (design D2); verify the resolution scenarios and that route-search / inference / routing suites are unchanged

## 3. Checker and emission

- [x] 3.1 Implement the central product rule on dependency closures and the new typed row-level error (design D3); verify the row-level and mixed-conjunct scenarios
- [x] 3.2 Compile aggregates across the hop as producers over the unrelated dataset at their attributable grain (empty or spine bucket), attached by `CROSS JOIN` / bucket join, with the dedicated associate disposition (design D5); verify the aggregation scenarios and the SQL-shape tests
- [ ] 3.3 Lower one-sided filters across the hop to producer predicates and uncorrelated `EXISTS` (design D5); verify the filter scenarios, including zero-match and empty dataset
- [x] 3.4 Add `UnrelatedDatasetWarningPayload` and emit it once per dataset per stage (design D6); verify the warning scenarios
- [x] 3.5 Change the empty-`join_pairs` validation message to name the aggregate remedy; verify the empty-join-pairs scenario
- [x] 3.6 Reject a `Column.sql` or model filter whose qualifier names a dataset no join path reaches, at save time and (stored unvalidated) at query time; verify the stored-definitions scenario

## 4. Root recommendation

- [x] 4.1 Add `ItemPath.unrelated` and the attachment rule of design D7 across the Python / REST / CLI / MCP surfaces; verify the recommendation tests

## 5. Docs, agent surface, architecture

- [x] 5.1 Add one sentence to `docs/concepts/queries.md` › Cross-model measures and one to › Choosing a root model (the `unrelated` flag); verify every touched page stays in the `zensical.toml` nav
- [x] 5.2 Add the unrelated-model clause to the MCP `measures` field description within the 2048-character budget; verify the tool-surface tests
- [x] 5.3 Land the approved Axiom 3.1 in `architecture/semantics.arc42.md` exactly as approved (below), with its `[enforced: test:tests/test_unrelated_datasets.py]` tag; verify `poetry run la-arch-check` is green

  Approved wording, inserted between the existing Axiom 3 text and its two `[enforced: …]` tags:

  ```
     - **3.1 Unrelated datasets.** A dataset no join path reaches from a query's
       population meets it as the product `P × D`, every row with every row,
       reached only by naming `D` as a query path's first hop, never by route
       search. Only aggregates and filters (Axiom 14) read across `×`; any other
       row-level read is a type error, so `×` never multiplies the population.
       [enforced: test:tests/test_unrelated_datasets.py]
  ```

## 6. Gates

- [ ] 6.1 Run `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs the baseline) and `poetry run la-arch-check`; all green
- [ ] 6.2 Run the integration suite with the CI invocation from `.github/workflows/ci.yml`; green
