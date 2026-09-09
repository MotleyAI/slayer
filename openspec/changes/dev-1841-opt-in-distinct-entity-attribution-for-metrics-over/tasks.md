# DEV-1841 tasks

## 1. Mode field and strict retirement

- [ ] 1.1 Add `to_many_handling` to `SlayerQuery` (Literal, default `"broadcast"`),
  remove `strict`, add the `model_validator(mode="before")` rejecting `strict` with
  the replacement-naming typed error; verify via model unit tests (default, each
  value, invalid value, strict rejection).
- [ ] 1.2 Storage migration `SlayerQuery` v3→v4 (`strict: true` → `"error"`, else
  dropped) with `CURRENT_VERSIONS` bump; verify via migration tests loading v3
  payloads in all three strict states.
- [ ] 1.3 Swap the parameter in REST `QueryRequest` and the MCP `query` tool
  (signature, docstring, dict emit, run-by-name disallow list); verify via API/MCP
  surface tests including strict rejection through both surfaces.
- [ ] 1.4 Thread the mode through `PreboundQuery` and `StrictQueryCarrier` into every
  recursive `plan_query` site; verify via a nested-producer test where an inner
  aggregate resolves per a non-default mode.

## 2. Uniform classification and error mode

- [ ] 2.1 Extend attribution classification to local aggregates with unattributable
  grain dimensions in every consumer role (measure, composite leaf, filter, order,
  computed dimension, nested), routing them through the safe-grain producer +
  broadcast path by default; verify via the duplicate-child-row regression matrix
  (each role) — the multiplied values must be unrepresentable.
- [ ] 2.2 Replace the strict gate with the `error`-mode gate covering broadcasts
  (cross-model and local) and excluded filters; verify via error-mode parity tests
  ported from the retired strict tests plus the local-shape error case.
- [ ] 2.3 Fix the broadcast reason for reachable-but-fanning dimensions (names the
  hop classification, not "unreachable"); verify via warning-payload tests.

## 3. Association producer

- [ ] 3.1 Add the `association` `ProducerKernel` variant (entity-key columns +
  level-2 aggregate spec) and its planning in the regroup desugar (implicit grain and
  explicit `partition_by=` under associate); verify via plan-shape unit tests.
- [ ] 3.2 Emit the two-level GROUP BY in the generator (one picked column per input
  expression; `*:count` without a value column; measure-local filter on the entity
  side); verify via golden SQL for representative shapes.
- [ ] 3.3 Route filters into the association producer via the existing producer
  filter routing with root = host (inline / EXISTS / drop+warn); verify via executed
  values for host-local, sibling-branch (incl. NULL-sensitive), and dropped conjuncts.
- [ ] 3.4 Typed errors for `window=`/`first`/`last` combinations and missing root
  unique key; verify via error tests and an unchanged guard-ratchet baseline.
- [ ] 3.5 Warnings: `associated` payload kind (surfaced twice, suppressed for explicit
  `partition_by=`), dice–slice hint field on broadcast payloads, response-only
  informational entry for semi-join-pushed conjuncts in every mode; verify via
  warning-payload tests updating the dev-1840 silence assertions (approved).

## 4. Executed-value and law coverage

- [ ] 4.1 Associate-mode executed-value suite on SQLite + DuckDB: cross-model,
  local, multi-hop, `*:count`, percentile, measure-local filter, explicit
  `partition_by=`, mode-invariance of fully attributable queries, cardinality
  neutrality; verify all pass by hand-computed values.
- [ ] 4.2 Regression tests for the multiplication finding (duplicate same-status
  orders: default broadcasts — never 350; associate returns 250/100/60); verify they
  fail on the pre-change engine.
- [ ] 4.3 Rewrite the dev-1853 pinned local-shape test on duplicate-carrying data
  asserting both modes (approved); verify the dev-1853 suite passes.
- [ ] 4.4 `tests/test_law_dice_slice.py` in the generative law harness (filter `d=v`
  ≡ slice cell `v` under associate); verify across the harness shape family on
  SQLite + DuckDB stride.
- [ ] 4.5 Golden baselines: byte-identity of cross-model broadcast defaults; new
  association goldens across every Tier-1/2 dialect emission path (composite entity
  keys, percentile/median included); re-bless flipped local-shape goldens with
  divergence-ledger entries; verify golden suites pass.

## 5. Architecture and docs

- [ ] 5.1 `architecture/semantics.arc42.md`: reword axiom 8 (drop the override
  clause) and flip its `[target: DEV-1841]` to enforced test ids; flip law 5 to
  `[enforced: test:tests/test_law_dice_slice.py]`; `index.yaml` `queries.touches`
  gains `storage`; verify `tools/arch_check.py` passes.
- [ ] 5.2 Update `docs/concepts/queries.md` (strict row → `to_many_handling`,
  cross-model section gains the modes and an associate example) and
  `.claude/skills/slayer-query.md`; verify no stale `strict` references remain in
  docs/skills (grep).
- [ ] 5.3 Divergence ledger (`divergences.md`) enumerating the local-shape value
  flip, strict rejection, new warning payloads, and re-blessed goldens; verify every
  behavior-flipping test cites its entry.

## 6. Gate

- [ ] 6.1 Full non-integration suite green (`poetry run pytest -m "not
  integration"`), integration SQLite/DuckDB green, `ruff` clean; verify in CI.
