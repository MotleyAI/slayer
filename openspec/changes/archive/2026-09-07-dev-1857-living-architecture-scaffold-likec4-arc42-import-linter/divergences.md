# Divergences from the plan

1. **No real-repo-green pytest.** A `test_real_repo_is_green` asserting
   `run_checks(REPO_ROOT) == []` was written, then removed: the unit suite runs
   in CI, so it would wire arch_check enforcement into CI by proxy — DEV-1857
   defers CI enforcement to a later decision. Enforcement stays in the flow
   gates only.
2. **`layers` baseline 15 → 14.** Merging origin/main (post-plan) brought
   DEV-1833, which retired the legacy `parse_filter` and killed the
   `slayer.core.formula -> slayer.sql.window_detect` edge; the ignore entry was
   deleted and the baseline lowered per the ratchet. The node-level relation set
   (37 edges) is unchanged.
3. **Two new spec groups mapped.** The same merge archived DEV-1751 and
   DEV-1810 specs into new top-level groups `facade` and `mcp`; they are owned
   by the `protocols` and `surfaces` buckets respectively (`specs:` in
   index.yaml), not cross-cutting.
4. **`specs/DEV-1743-*.md` references kept.** The plan's "no stale DECISIONS.md
   references" sweep covered live docs/tests; the two pre-OpenSpec historical
   plan documents mention DECISIONS.md as part of their completed-plan narrative
   and were left as history.
5. **Baseline check tightened from ≤ to ==** (review round): a count below the
   baseline would leave slack for a different grandfathered edge to appear
   later, so arch_check now requires the exact count and a removal must lower
   the baseline in the same commit.
