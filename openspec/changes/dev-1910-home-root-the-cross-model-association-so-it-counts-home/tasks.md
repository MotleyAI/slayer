## 1. Failing tests (spec-tests stage)

- [x] 1.1 `tests/test_dev1910_home_rooted_association.py` executed oracles on the DEV-1900 graph (SQLite + DuckDB, own seed helper adding a NULL-status order for c1 where needed): issue bar (`customers.spend:sum` by `bad_pop`, South = 195, `amount:sum` unchanged, associated warning); host filter + home-side dim (`channel = 'app'` by `bad_pop`: North 250, South 140); mixed `bad_pop` × `status` ((230, ok) = 140; c7 in no cell); presence guard (NULL-status cell = c1 only, never + c7); customers-rooted twin keeps its NULL cell (c1 + c7); dice–slice on `customers.regions.bad_pop = 230` vs the 230 slice (195 both); two-hop home by `bad_pop` (`customers.regions.pop:sum`: 100 / 200); local coupling (customers-rooted, `orders.channel = 'app'` by `orders.status`, ok = 140 not 390); composite back hop (`stores.rent:sum` by `status`: ok 1100, new 800); parameter typing positive (`weight=customers.spend`, South incl. c7) and a fanning host column picked across the reverse hop (`weight=amount`: South 75, typing accepted per decision 2 — c7's NULL weight drops it; NOT a new raise site); the `semi_join_pushed` entry present for an inlined host-branch conjunct. Verify: done — the distinguishing cases fail (wrong value) before implementation; the behaviour-preserving cases (where c7 has a population row, or home == host) are green guards.
- [x] 1.2 Plan shapes (`plan_query`): association attach `producer_root_model == "customers"`; kernel entity keys `ColumnKey(path=(), leaf="id")`; `producer_plan.semi_join_filters == []` with the weak-plans conjunct as a bound filter of the producer; `present_keys == [orders.customer_id]` for `status`, `[]` for `bad_pop`, both store columns for `stores.rent:sum`; two-hop home `producer_root_model == "regions"` with grain key path `("customers", "orders")`; DEV-1859 headline: nested parameter producer rooted at `orders` inside the customers-rooted body; a pydantic round-trip of `present_keys` + `association_restricted_filter_texts` (task 3.1). Verify: done — fail before implementation.
- [x] 1.3 `tests/test_dev1910_back_path.py`: `_back_path` / `attributable_from_root` / `reroot_from_root` / `broadcast_reason` on unnamed two-hop, named-edge, mixed named/unnamed and ambiguous (raises) reverse paths; single-hop results byte-identical to today. Verify: done — fail before implementation (function absent).
- [x] 1.4 Existing tests (consented): remove the strict xfail in `tests/test_dev1900_dimensions.py::TestAssociate`; flip `tests/test_dev1841_association_plan.py::test_pushdown_reaches_the_association_producer` to "inlined, no semi-join, entry present"; reword the module docstring of `tests/test_dev1841_association_filters.py`. Verify: done — the two flipped tests fail before implementation.
- [x] 1.5 Golden manifests: add every `assoc/` (and association-carrying) key of dev1841 / dev1859 / dev1892 / dev1900 (dev1847 only if it shifts) to `ALLOWED_DELTAS` with the reason "DEV-1910 home-rooted association". Verify: done — golden modules report the pending deltas, nothing else drifts.

## 2. Reverse path (join_safety)

- [ ] 2.1 `_back_token` → `_back_path` (forward walk from the host, edge name else source model per hop, reversed, `(host_name,)` fallback); callers `attributable_from_root`, `_reroot_leaf_via_host`, `broadcast_reason` use `(*back, *hp)`. Verify: 1.3 green; DEV-1840/1841/1853 suites and every non-association golden byte-identical.
- [ ] 2.2 `crossing_local_root_predicate` excludes `locus == "host"`. Verify: DEV-1859 plan-structure and exec suites green.

## 3. IR and rendering

- [ ] 3.1 `AssociationProducerKernel.present_keys: List[ValueKey]`; `RegroupAttachPlan.association_restricted_filter_texts: List[str]`. Verify: pydantic round-trip in 1.2 plan tests.
- [ ] 3.2 `_render_association_producer_body`: `NOT (<col> IS NULL)` for every present key in level 1 next to the entity clause. Verify: 1.1 presence-guard cases green.
- [ ] 3.3 `query_engine._attach_semi_join_texts` also yields the attach's restricted texts. Verify: 1.1 entry case green; DEV-1841 warnings suite green.

## 4. Compiler fold

- [ ] 4.1 Association arm inside `_synthesize_cross_model_producer` (design decisions 2–4, 8) and the shared tail; delete `_synthesize_association_producer`. Verify: 1.1 / 1.2 / 1.4 green; DEV-1841 / 1847 / 1859 / 1892 / 1900 suites green; DEV-1906 strict xfail still xfails; `tests/test_law_dice_slice.py` green.
- [ ] 4.2 Re-bless goldens (`SLAYER_UPDATE_GOLDEN=1`), review each delta is an association case only, empty the manifests. Verify: golden modules green with empty `ALLOWED_DELTAS`.

## 5. Docs and durable records

- [ ] 5.1 One sentence in the associate paragraph of `docs/concepts/queries.md` (home-rooted; an entity with no population row still counts in the cells its own path reaches; filters bind to the same related row as the dimension). Verify: sentence present, page still linked in `zensical.toml`.
- [ ] 5.2 `architecture/semantics.arc42.md` Axiom 8: add `[enforced: test:tests/test_dev1910_home_rooted_association.py]` to the associate clause (approved). Verify: `poetry run python tools/arch_check.py` green.
- [ ] 5.3 Comment on DEV-1906 recording the host-root twin residue (customers-rooted local root with an attached parameter reading orders columns is refused by the crossing-local-root routing; repro query in the design Context). Verify: comment posted.

## 6. Gates

- [ ] 6.1 `poetry run pytest -m "not integration" -n auto`; `poetry run ruff check slayer/ tests/`; `poetry run python tools/arch_check.py`; `poetry run basedpyright` (no new errors vs baseline); `~/.claude/skills/process-reviews/scripts/check-conventions.sh`. Verify: all green.
- [ ] 6.2 Codex pass over the working tree before the push. Verify: no open findings.
- [ ] 6.3 With go-ahead: commit, push, open the PR against the DEV-1900 branch while PR #398 is open (retarget to `main` once it merges). Verify: PR URL.
