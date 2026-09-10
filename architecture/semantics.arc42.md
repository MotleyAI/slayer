# Query semantics — the algebra

## 1. Purpose & context

The complete end-target algebra for SLayer queries — every axiom and every law,
each clause tagged with its status (see the tag vocabulary in
`system.arc42.md` §4). Currently-true behaviour is specified with executable
scenarios in `openspec/specs/queries/semantics` and
`openspec/specs/models/join-cardinality`; clauses not yet true carry a
`target:` tag naming the issue that flips them in its own PR. The carrier: an
aggregate is a partial function from its grain's coordinate space to scalars,
typed by that grain; grains form a lattice under set inclusion, and broadcast
is the coercion from coarser to finer.

## 2. Axioms

1. **Determination**: a dataset determines what a chain of provably to-one join
   hops reaches; determined fields have one value per row and behave as its own
   fields, and a reference never leaves its join path ambiguous (spec:
   `models/join-cardinality` › Determination through to-one chains).
   [enforced: test:tests/test_dev1836_producer_execution.py]
2. **Home dataset**: a row-level expression mixing several datasets' fields is
   legal only when one dataset determines them all — its home dataset, the one
   place it has exactly one value per row. [review]
3. **Association**: any join path — to-one or not — defines which rows belong
   together; everything that crosses a non-determining path is defined in terms
   of it. [review] Association is derivable from forward join declarations
   alone (bidirectional traversal).
   [enforced: test:tests/test_dev1853_mirror_parity.py]
4. **The home-dataset axiom**: an aggregation runs over the rows of its home
   dataset, each counted exactly once — never over the row product of a join,
   so no fan-out can multiply its inputs (spec: `queries/semantics` › No double
   counting). [enforced: test:tests/test_dev1836_producer_execution.py]
5. **Grain and cells**: an aggregate is typed by its grain — its partition_by
   dimension set, defaulting to the query's dimensions; one combination of
   grain values is a cell. [review]
6. **An aggregate is a dataset**: it can be filtered, queried, and aggregated
   again; a strictly coarser grain is reached only through an explicit
   second-order aggregation — the implicit collapse is a typed error
   [enforced: test:tests/test_dev1824_partitioned_execution.py]; second-order
   aggregation over attached values
   [enforced: test:tests/test_dev1847_reaggregation_exec.py]; aggregation sources
   mixing row-level columns with attached values [target: DEV-1859].
7. **Attributability**: a dimension is attributable to an aggregation iff the
   home dataset determines it — the cells then partition the home rows and sum
   to the total (spec: `queries/semantics` › Attribution by determination).
   [enforced: test:tests/test_dev1836_producer_execution.py]
8. **Mode axis**: an unattributable dimension resolves per the query-level
   `to_many_handling` mode — broadcast (the default: the value repeats across
   the dimension's cells, with a self-announcing warning; spec:
   `queries/semantics` › Loud degradation)
   [enforced: test:tests/test_dev1836_broadcast_strict.py]; associate (per-cell
   aggregation over the distinct associated home rows)
   [enforced: test:tests/test_dev1841_association_exec.py]; and error (refuse)
   [enforced: test:tests/test_dev1841_error_mode.py]; an explicit partition_by
   naming an unattributable dimension is an error outside associate mode.
   [enforced: test:tests/test_dev1841_association_errors.py]
9. **Closure**: every operator consumes and produces aggregates and may
   inspect only its operands' types (grain, home dataset), never how they were
   constructed — any "not supported inside" refusal of a well-typed term is a
   closure violation. [target: DEV-1868]
10. **Grain-union broadcast**: combining aggregates unions their grains, each
    operand broadcast from its own grain to the union — coarser to finer only;
    the population supplies the row set, a cell an operand lacks contributes
    NULL, and combining never removes rows (spec: `queries/semantics` ›
    Grain-union broadcasting). [enforced: test:tests/test_dev1739_execution.py]
11. **Transforms are typed**: transforms act on aggregates, preserving or
    dropping grain dimensions; a time-ordered transform requires the time axis
    in its operand's grain and fails with the remedy otherwise. [review]
12. **Population**: the population is the query's quantifier — exactly one
    result row per combination of dimension values among its row-filtered rows
    (raw-row mode is the one documented exception; spec: `queries/semantics` ›
    Grain guarantee)
    [enforced: test:tests/test_distinct_dimension_values.py]; it may be named
    explicitly, may be any dataset, is reported back, and defaults to the
    smallest dataset determining every queried dimension — inferred from
    dimensions and row-level filters only, never measures.
    [enforced: test:tests/test_law_population_invariance.py]
13. **Positions**: every query expression is a field or a measure; dimensions
    and measures return the value, filters mask on it, order sorts by it — the
    expression evaluates identically in every position, compiled by
    construction as a hidden field/measure where not projected.
    [enforced: test:tests/test_dev1865_value_parity.py]
    [enforced: test:tests/test_dev1865_stratification.py]
    [enforced: test:tests/test_dev1865_order.py]
14. **Filters**: a field-typed filter masks population rows before any
    aggregation; a measure-typed filter masks result cells after all values
    are computed, never changing a surviving cell's values; valid-as-both
    types as field, valid-as-neither is a type error; evaluation is stratified
    — aggregate-free field filters fix the population every aggregation runs
    over [enforced: test:tests/test_dev1839_measure_execution.py]; a filter
    reaching a home dataset across a non-determining path restricts by
    association or is loudly excluded — a stated restriction is never silently
    ignored (spec: `queries/semantics` › Filters restrict by association or
    fail loudly). [enforced: test:tests/test_dev1840_execution.py]

## 3. Laws

Laws are ∀-quantified equations between evaluations; their honest enforcement
is the generative law harness (DEV-1869), which upgrades `review` laws to
enforced test ids as its instances land.

1. **Grain union**: grain(a ⊕ b) = grain(a) ∨ grain(b).
   [enforced: test:tests/test_law_grain_union.py]
2. **Broadcast coherence**: coercing then combining equals combining then
   coercing, and coercions compose along chains of grains.
   [enforced: test:tests/test_law_broadcast_coherence.py]
3. **Compositionality**: a term's denotation depends only on the term, the
   population, the row-level filters, and the mode — never on sibling terms;
   sibling-measure independence
   [enforced: test:tests/test_dev1837_dimension_measure_matrix.py];
   single-measure splits [enforced: test:tests/test_law_split_invariance.py];
   measure-typed filters and order entries. [review]
4. **Position parity**: the same expression yields the same value as a
   measure, a filter, or an order key — positions differ only in what happens
   to the value. [review]
5. **Dice–slice**: filtering to `d = v` equals slicing the `v` cell of a
   groupby on `d` — exact in associate mode; the broadcast default consciously
   trades this law away and must warn.
   [enforced: test:tests/test_law_dice_slice.py]
6. **Lowering soundness**: every emission trick is a pure optimization —
   inlining an association-restricting filter on a proven to-one path and
   fusing pipeline phases into one SELECT
   [enforced: test:tests/test_law_lowering_soundness.py]; compiling a filter
   as a hidden measure. [review]

## 4. Notes

Database-level nondeterminism (floating-point summation order, approximate
quantiles, ties in first/last) is accepted, not closed: the laws hold up to
what the underlying engine itself keeps stable.
