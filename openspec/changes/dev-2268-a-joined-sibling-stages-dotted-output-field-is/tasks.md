## 1. Tests (pr-tests)

- [x] 1.1 Flip `tests/test_dev2102_static_reads_harness.py::test_an_inline_source_is_local_to_its_own_stage[inline-root]`: drop the strict xfail, spell `s.customers__name`, key `customers.s.customers__name` (design decision 9); verify it passes today
- [x] 1.2 New test file for DEV-2268 behaviour: executed stored-root reads (`addresses`, `regions`) of `s.customers__name` on SQLite and DuckDB with matching join keys and exact rows; inline `customers` root likewise; verify they pass today (they pin existing behaviour)
- [x] 1.3 Failing error tests for every scenario of "A dotted spelling of a flattened column is refused with its flat name" (stored addresses, stored regions without `customers.name` suggestion, inline root not circular, longest flat wins, authored `a__b` column, stale respelling suggests canonical with no warning, ambiguous respelling gives no hint, quoted segments, downstream stage illegal-scope reason, unrelated short form suggestion, genuine join through a stage unchanged); verify each fails for the right reason
- [x] 1.4 Failing tests for "Reference errors name models as the user spells them" (`s.nope` from a stored root, `nope` over stage `s`: no `__slayer_` in the text) and the MODIFIED broken-chain scenario (`s.customers.nope` names `s` and its columns)
- [x] 1.5 List every existing test that pins an error type or text this plan changes; get the user's approval per file before aligning it

## 2. Implementation (pr-implement)

- [ ] 2.1 Non-recording flat-name probe in `slayer/core/scope.py` (design decision 6); verify the stale/ambiguous tests from 1.3
- [ ] 2.2 Flat-name hint at the dotted walk's failure doors in `slayer/engine/binding.py` (multi-hop miss, circular revisit; dotted star too), nearest-root first, quoting per decision 7; verify 1.3's model-scope tests
- [ ] 2.3 Broken-chain error names the model the failing hop leaves and its columns (decision 8); verify 1.4's broken-chain test
- [ ] 2.4 Stage-scope illegal-scope reason names the flat column; verify 1.3's downstream test
- [ ] 2.5 Typed plumbing of the population model into the unrelated-read check (decision 4); verify 1.3's unrelated short-form test
- [ ] 2.6 User spellings in binding-raised reference errors, walker fields kept canonical (decision 5); verify 1.4
- [ ] 2.7 Docs: one sentence in `docs/concepts/queries.md` sibling-stage paragraph (decision 12)
- [ ] 2.8 Gates: full non-integration suite, `ruff check slayer/ tests/`, `basedpyright` (no new errors vs baseline), `la-arch-check`, `openspec validate dev-2268-a-joined-sibling-stages-dotted-output-field-is --strict`
