## 1. Tests first (pr-tests)

All executed-value tests parametrised over SQLite and DuckDB on the DEV-1847 fixture
(`tests/_dev1847_fixtures.py`), in `tests/test_dev2013_expression_attribution.py` (this
exact name — the arc42 tags point at it). Verify: each new test fails on the pre-change code.

- [ ] 1.1 Outer-dimension shapes under `avg(sum(amount, partition_by=[city, region]))`:
  `[region, city == 'Alpha']` in default, `error` and `associate` modes → (North,T)=30,
  (North,F)=60, (South,T)=40, (South,F)=100, (East,F)=60, (Gap,NULL)=12, (Gap,F)=8,
  (Void,F)=NULL, no broadcast/associated warning, no error; `iif(city=='Alpha',1,0)` →
  North 1→30 0→60, South 1→40 0→100, East 0→60, Gap 0→10, Void NULL; `upper(city)` → the
  per-city cells; `region == 'North'` → 45/70/60/10 with no warning; `city == 'Alpha'` as
  the only dimension → false 58, true 35, NULL 12; controls already correct today:
  `iif(city=='Alpha', P, 0)` and `iif(city=='Alpha', P*0+1, 0)` with
  `P = amount:sum(partition_by=region)` → 30/60
- [ ] 1.2 Chain shapes (`avg(sum(amount, partition_by=customer_id))` rooted at `corders`):
  `customers.regions.name == 'North'` → true 35, false 100; `customers.region_id * 10` →
  10→35, 20→100; `sum(amount, partition_by=customers.regions.name) > 80` → false 35,
  true 100; all without warnings
- [ ] 1.3 Cross-model attached parameter grained by `rid10 = customers.region_id * 10`
  executes with values identical to `partition_by=[customers.region_id]` (choose a
  non-degenerate aggregation so the oracle distinguishes)
- [ ] 1.4 Negative controls: `is_p = product == 'P'` still broadcasts, warning names `is_p`
  with the "not determined by the operand grain" reason + `partition_by=` remedy, and
  `error` mode refuses naming `is_p`; explicit outer `partition_by=region` stays 45/70 with
  no warning; adding a plain `amount:sum` measure changes no other value or row
- [ ] 1.5 Naming: broadcast/error over `customers.regions.name`
  (`avg(sum(amount, partition_by=amount))` rooted at `corders`) names it dotted — never
  `customers__regions__name`, never `grain_<hash>`; a `ParameterGrainError` grain listing
  uses dotted names
- [ ] 1.6 Broadcast-reason categories from the witness: to-one-reachable undetermined column
  (remedy names it), fanning-closure witness (reason names the hop), unanalysable closure /
  unsupported kind ("cannot be analysed")
- [ ] 1.7 Parameter typing via definition defaults (`SqlFragmentKey`): multi-ref default with
  one undetermined ref → typed parameter error; unanalysable `ColumnSqlKey` dependency →
  fails closed; zero-ref (literal-only) default → determined
- [ ] 1.8 Combinator unit tests, one per kind: literal; `TimeTruncKey` via its column;
  arithmetic / scalar-conditional / BETWEEN / IN / `SqlFragmentKey` all-determined vs
  mixed (one determined + one undetermined, fanning or unknown-kind child); nested
  composite; aggregate grained (determined / undetermined partition key) vs ungrained;
  `StarKey`, `TransformKey`, unknown kind fail closed; an expression grain member is
  determined as itself
- [ ] 1.9 `key_display` unit tests for every `KIND_POLICY` kind (time bucket keeps its
  granularity; BETWEEN, IN, SQL fragment, embedded aggregate render as formula text);
  `dotted_key_display` never returns a Pydantic repr

## 2. Implementation (pr-implement)

- [ ] 2.1 Witness-form determination combinator in `slayer/engine/join_safety.py`;
  `grain_determines` as its boolean wrapper (membership short-circuit at every node) —
  verify 1.8
- [ ] 2.2 `_synthesize_reaggregation_producer`: attributable ⇔ `grain_determines`; delete
  `_grain_expression_determined`, `_non_aggregate_leaf_check`, `_reaggregation_determined`,
  the `expression_determined` list/loop and the `g in union_grain` pre-check — verify 1.1,
  1.2, 1.4 and the existing DEV-1847 suites
- [ ] 2.3 `_param_is_determined` → `grain_determines(value)` for non-transform values;
  `_home_determines_grain_member` over the combinator — verify 1.3, 1.7
- [ ] 2.4 Public total `key_display` in `slayer/core/refs.py`; `dotted_key_display` falls
  back to it — verify 1.9
- [ ] 2.5 Diagnostic display map (first declared name, else `key_display`) at every
  user-facing site in `stages.py`; `_regroup_grain_name` internal-only; witness-derived
  broadcast reason — verify 1.4, 1.5, 1.6. Existing tests asserting old spellings/reasons:
  list them for user OK before any expected-string update
- [ ] 2.6 Remove non-forward issue references and now-unused imports in touched code;
  `poetry run ruff check slayer/ tests/` clean

## 3. Docs, architecture, gates

- [ ] 3.1 `docs/concepts/formulas.md` re-aggregation paragraph: add "An outer dimension that
  is a row-level expression over fields the operand grain determines (`city == 'Alpha'`
  over `[city, region]` cells) partitions the cells exactly." before "An outer dimension
  not determined by…"
- [ ] 3.2 `architecture/semantics.arc42.md` (approved tag-only edits): append
  `[enforced: test:tests/test_dev2013_expression_attribution.py]` to Axiom 2's tag list and
  to Axiom 7 — verify `la-arch-check` green
- [ ] 3.3 Full unit suite (`poetry run pytest -m "not integration"`), integration suite with
  the CI invocation, `poetry run basedpyright` no new errors vs baseline, lint clean
