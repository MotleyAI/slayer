## 1. Tests (pr-tests stage; all fail before implementation)

- [x] 1.0 Earlier failing tests (commit aa60f90f) merged with main 2.0 (a6493b303): mechanical renames done; their
      expectations predate 2.0 and are rewritten below
- [x] 1.1 `tests/_stored_upgrade_fixtures.py`: regions fixtures in 2.0 shape (a TIMESTAMP `opened_at` beside the DATE
      `opened_on` for the whole-day cases); verify the existing stored-upgrade tests still pass on them
- [x] 1.2 Legacy repair, YAML and SQLite: short-form two hops, reverse edge, reversed operands, own qualifier then route —
      each loads with its qualified path and repaired literal and returns the expected total; extension DATE column on
      routed `regions` loads as `CAST(... AS DATE) >= '2024-01-01'` and runs; verify they fail on the current code
- [x] 1.3 Operator layouts: one case per layout of decision 14 with main's qualified and whole-day output
- [x] 1.4 Verbatim / refusal: routed TEXT column verbatim with its rows; ambiguous short-form route refused by
      qualification; unloadable peer not blocking an unrelated repair
- [x] 1.5 Quoting: quoted hop and leaf in exact case repaired; wrong-case hop or leaf never repaired; the qualified leaf
      stays double-quoted
- [x] 1.6 Stage population (bare reference verbatim, extension-join hop repaired); root-less population (unique inference
      through a route, ambiguous inference refused)
- [x] 1.7 Memories: routed literal repaired; two datasources with priority both ways; the rewritten spine-memory scenario
- [x] 1.8 Whole-day upgrade typed by the binder: routed TIMESTAMP `= '2024-01-01'` reads `in`, a routed slashed
      `= '2024/01/01'` the same; quoted operand by exact spelling; a current model holding an older nested stage with
      extension `columns` and a typed literal is typed, inlined and qualified
- [x] 1.9 Conversion scope: loading the document itself and a two-model cycle neither recurse nor write a peer twice; raw
      reads unmutated and bytes equal across load orders
- [x] 1.10 `core.syntax.literal_comparisons` reader tests (spans, parts, quoting, every layout, function-argument literal
      not reported, unparseable empty, splice round-trip); verify they fail (reader absent)
- [x] 1.11 `resolve_path` / `resolve_anchored_path` quoting tests under the 2.0 keyword names: quoted root qualifier,
      edge name, short-form dataset, mixed segments, case-colliding names; no flags unchanged; verify they fail

## 2. `core` readers

- [ ] 2.1 Record each string-literal token's offset outside node identity; verify the `core.syntax` suite passes unchanged
- [ ] 2.2 Add `literal_comparisons` (Pydantic item type), empty on unparseable text; verify 1.10 passes
- [ ] 2.3 `core.date_only` sites carry per-segment quoting; verify the date-only suites pass unchanged

## 3. Shared resolver quoting

- [ ] 3.1 Thread optional per-hop `quoted` flags through `resolve_path`, `resolve_anchored_path`, `drop_root_hop`,
      `find_anchor`, `resolve_hop`, short-form route selection and `walk`; verify 1.11 and the drift / memory-resolver /
      qualification suites pass
- [ ] 3.2 The qualifier passes each reference's flags and keeps a quoted leaf double-quoted; verify the qualification
      suites pass

## 4. Storage

- [ ] 4.1 Share qualification's population inference, datasource choice and stage universe with the typed step; expose
      the inliner's extension-column lookup
- [ ] 4.2 The typed step: legacy sites (filters) and whole-day sites (every text `upgrade_query` covers), each under its
      version gate, typed by `resolve_path` from the population (stage host for a stage population), DATE / TIMESTAMP
      only for the repair, TIMESTAMP for whole-day, span splice; verify 1.2–1.8 pass
- [ ] 4.3 One guarded conversion scope (typing → inlining → qualification) for model stages, the current-document branch
      and memory queries, each operation run when any nested stage needs it; verify 1.9 passes
- [ ] 4.4 SlayerQuery v14→15 and Memory v10→11 become version stamps; `typing_with` / `needs_typing` serve a dataset's own
      texts only; delete `_query_host`'s query use of `StoredDatasets.field_type`; verify every whole-day scenario passes
- [ ] 4.5 `storage/legacy_time_literals.py` without sqlglot (normalisation, date-range repair, span splice); verify the
      filter-literal and date-range tests pass

## 5. Finish

- [ ] 5.1 Local test scope, `ruff check slayer/ tests/`, `la-typecheck` (no new errors vs baseline) and the enforcement
      bundle (`la-arch-check`, LikeC4 validate) green
- [ ] 5.2 `openspec validate dev-2255-stored-legacy-time-literal-repair-resolve-the --strict` green
