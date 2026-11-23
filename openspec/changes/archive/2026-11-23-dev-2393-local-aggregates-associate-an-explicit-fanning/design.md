## Context

See proposal.md › Why. The current state the approach builds on:

- **The bind-time guard.** `bind_inputs.py::_validate_partition_keys` calls `relationship_safety.assert_partition_key_attributable` for every aggregate and rank-family transform partition key, re-aggregation outers excepted. A path-less key is judged from the host. A path-bearing key is judged from the aggregate's source anchor, the host for a local aggregate. Any fan or unproven hop in the key's closure raises `elaborate_env.check_partition_key_attributable` (`PartitionKeyError`) in every mode. An unanalysable closure is folded into the same "not attributable" result. A product hop is exempt; the unrelated-dataset path handles it per mode.
- **The mode gate.** `stages.py::_synthesize_cross_dataset_producer` is the home-rooted synthesis. Its association arm (DEV-1910/1914) handles unattributable grain members for plain, ranked, windowed and windowed-ranked reducers. Its broadcast/error arm raises `check_cross_dataset_partition_keys_attributable` for an explicit key. An orders-rooted cross-model aggregate with `partition_by=orders.status` reaches it because `status` is path-less there.
- **Routing.** `discovery.py::_Walker.local_broadcasts` sends a local aggregate to that synthesis (`target_rooted`) when an implicit grain member is unattributable or a partitioned window's axis fans. An explicit partition key never routes it ("requested grain never broadcasts").
- **The probe.** Two probe edits were made and reverted: the guard returned early, and `local_broadcasts` routed an unattributable explicit key. With them, every oracle in the specs matched on SQLite. The probe also showed three defects the implementation must not repeat: it broke `tests/test_reagg_outer_grain_attribution.py` (regroup grain-slot asserts), it lost the unanalysable fail-closed check, and it returned values for an attached parameter whose grain the enclosing rows do not determine.

## Goals / Non-Goals

**Goals:**
- One code path decides whether an explicit partition key is attributable from the home. It is the mode gate, and it is shared with implicit grain members.
- Local and cross-model aggregates behave identically for explicit keys, in every position and for every reducer.
- No residual mode-invariant refusal of a partition key, except an unanalysable one.

**Non-Goals:**
- A NULL time bucket's place in a time-ordered transform, where the dialects diverge today. Recorded on DEV-2218. Tests here avoid NULL-bucket populations under time-ordered transforms.
- Deprecating `"broadcast"` (DEV-2385).
- Association along the time-spine axis (DEV-2218).
- Changing the positional rules, the input-safety rules for sources, parameters and ranking keys, the window-axis `TimeAxisError`, or unrelated-dataset semantics.

## Decisions

The planning decisions log, tags preserved.

1. **One mode gate for every explicit partition key; `bad_pop` is no longer refused in every mode.** Options were (A) narrow the guard to keys whose own definition crosses a fan, keeping `bad_pop` refused in every mode as the issue said, or (B) delete the guard. Chose B: the same grain associates as a dimension, the 2.8 justification contradicts 2.4 and 2.9, and B leaves one code path. (user)
2. **DEV-1941 folded in.** Options were (A) include fanning time keys and the bucket-spanning oracle, or (B) add a time-key-only refusal until DEV-1941. Chose A, because B would be a new carve-out under decision 1. The cross-model DEV-1941 shape already associates on main (DEV-1914). This change adds the local fanning time key and the bucket-spanning oracle. DEV-1941 is linked as related with a hand-off comment, and the PR lists it as a closing reference. (user)
3. **NULL time bucket in time-ordered transforms** (SQLite orders it first, DuckDB last): a pre-existing, unrelated gap, recorded on DEV-2218 as a scope note. This change's transform tests avoid NULL-bucket populations. (user)
4. **Axiom 8 wording** (user, approved wording). This passage:

   > an explicit partition_by naming a dimension unattributable only from a further (cross-model) root is an error outside associate mode [enforced: test:tests/test_dev1841_association_errors.py], but one whose own dependency closure (engine P10) crosses a fanning hop from its home is a mode-invariant input-safety error — raised in every mode, associate included, since it can never be counted without multiplying rows (Axiom 2.8). [enforced: test:tests/test_dev1911_fanning_partition_key.py]

   becomes:

   > an explicit partition_by key unattributable from the home is a dimension like any other (2.4: not an input) — a requested grain, so it associates without warning and is an error outside associate mode [enforced: test:tests/test_dev1841_association_errors.py] [enforced: test:tests/test_dev2393_partition_key_mode_gate.py]

   The rest of Axiom 8 is unchanged.
5. **Unproven hops resolve like fanning hops at the gate,** as they already do for dimensions. (default)
6. **Unanalysable keys still fail closed in every mode,** with today's typed error, never naming a hop. (default; avoids a regression)
7. **No warning for an associated explicit key** (requested grain), consistent with the cross-model rule. (default)
8. **One shared partition-key checker for ordinary local and cross-model producers.** The wording is generalised (never "cross-dataset"), and the suggestion names omitting `to_many_handling` (or `'associate'`) beside the cardinality / unique-key remedy, matching DEV-1914's `TimeAxisError` and the re-aggregation outer-key suggestion. The re-aggregation outer-key checker stays separate, with its operand-grain remedy. (default; scope from codex 9)
9. **Positional, input-safety, window-axis and unrelated-dataset rules are unchanged.** A key across a product hop already resolves per mode on its own path (`queries/unrelated-datasets`) and is not routed through the ordinary association entity. (default; wording corrected by codex 1)
10. **Spec layout.** The DEV-1911 requirement is REMOVED and replaced by an ADDED one (a rename plus a rewrite). The other requirements are MODIFIED with their full text. (default)
11. **Three-way dependency analysis.** The partition-key check classifies a key as attributable, unattributable or unanalysable. Only unanalysable fails at bind; unattributable goes to the mode gate. This prevents an unanalysable closure from silently associating. (codex 2)
12. **Transform partition keys are not aggregate grains.** A rank-family transform's own `partition_by=` is validated only against its operand grain (Axiom 11.2), and the bind guard stops judging transform keys. A fanning key reaches a transform's operand grain only through an inner aggregate or query dimension that resolves per the mode. (codex 3)
13. **Routing by node role.** `local_broadcasts` routes a row-level local aggregate with an unattributable explicit key to the home-rooted synthesis. A second-order re-aggregation outer stays excluded (its outer grain is judged against its operand dataset by the re-aggregation planner). Row-level inners and attached-parameter producers are each routed by their own key. (codex 4)
14. **A fanning partition key and a fanning window axis bind to one related row.** Proven by the same-row fixture: c8's `ok` order lies outside the frame, so c8 is not in (ok, 2024-03). (codex 5)
15. **Mixed and multi-key grains are covered.** These are attributable + unattributable (`[customers.tier, orders.status]`), two unattributable keys on one branch (one related row supplies both), and keys on separate branches. Under broadcast/error the first offending key is named deterministically. (codex 6)
16. **Representation parity.** Keyless homes, list children, saved measures and stages resolve alike, by executed values. (codex 7)
17. **Attached parameters.** A parameter's own unattributable key resolves per mode inside the parameter's producer. The existing host-keyed shape (`OWN_FAN_STATUS`) is the associated oracle. Where the enclosing rows do not determine the parameter's grain (`OWN_FAN_BADPOP` by `status` or by `bad_pop`), the query keeps the typed determination rejection under the default and `associate`, never a value. (codex 8; determination outcome is the default from the existing rule)

## Risks / Trade-offs

- [Behaviour change for queries that raised under the default] → Additive: every affected query raises today. The spec states the new outcomes, and the release notes mention them.
- [Routing a local explicit-key aggregate to the home-rooted synthesis could disturb re-aggregation plans, as the probe did] → Decision 13's role rule, plus the full re-aggregation suite (`tests/test_reagg_outer_grain_attribution.py`, `tests/test_dev1847_reaggregation_exec.py`, `tests/test_dev1928_*`) as regression gates.
- [Error precedence between a parameter producer's partition-key refusal and the enclosing determination rule] → Pinned per mode by the partitioned-aggregates scenario.
- [Golden SQL churn] → Only shapes that refused today change kernel, so existing goldens should stay byte-identical. Any divergence is reviewed in pr-implement.

## Migration Plan

None: there are no stored-shape changes.
