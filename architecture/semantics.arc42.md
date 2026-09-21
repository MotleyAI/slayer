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
   `models/join-cardinality` › Determination through to-one chains). A derived
   column, being a function of its declaring dataset's row, is well-formed only
   when its definition crosses provably to-one hops: a definition that provably
   crosses a fanning hop is rejected at save time, an unproven hop is accepted
   with a warning and the query-time backstop (spec: `models/column-definitions`).
   [enforced: test:tests/test_dev1836_producer_execution.py]
   [enforced: test:tests/test_dev1930_save_time_arity.py]
2. **Home dataset**: every row-level expression, and every aggregation, has at most
   one home dataset — the dataset over whose rows it is evaluated with exactly one
   value per row (an aggregation is counted over it, Axiom 4). Datasets are the
   environment's root and the join paths from it; "D determines X" is Axiom 1,
   judged through dependency closures (engine P10). The home is resolved by:
   - **2.1 Leaf.** A column or star reference is homed on the dataset it lives on —
     the terminal of its join path. A derived column is homed there provided that
     dataset determines every column its definition reads; a dependency reached only
     across a fanning or unproven hop, or a definition that cannot be analysed, leaves
     it without a home. A literal is determined by every dataset and constrains nothing.
   - **2.2 Combination.** A combination of row-level expressions — arithmetic,
     comparison, scalar function, conditional — is homed on the deepest dataset that
     determines every operand's home over provably to-one hops. When no dataset
     determines them all, the expression is illegal.
   - **2.3 Attached constituent.** An aggregate or transform appearing as an operand
     is a typed dataset (Axiom 6) and is opaque: only its type is consulted (Axiom 9)
     — its grain: the explicit `partition_by=`, else the query's dimensions; for a
     transform, its result grain (Axiom 11), where a windowed inner's
     grain always includes the query's time bucket whether or not its `partition_by=`
     names it. Its value is broadcast onto the home's rows (Axiom 10), which is
     well-defined only when the home determines every grain member; so in 2.2 the
     constituent stands for its grain members. It contributes no leaf, and its
     interior is never inspected.
   - **2.4 Aggregation.** An aggregation over a row-level source is homed on the
     deepest dataset that determines the source's home (2.2) and every column-valued
     parameter and non-overridden definition default — each a row-level expression
     under 2.1, defaults resolved as references from the owning model [target: DEV-1931]. It is counted over that
     dataset's rows. The ordering key of a ranked aggregation must be determined by the
     home and never widens it; the aggregation's own `partition_by=` is not an input.
     A source with no row-level leaf is a second-order aggregation: its home is the
     operand dataset — the constituents' union-grain cells (Axiom 6) — not a model.
   - **2.5 Recursion.** Nesting resolves bottom-up: each aggregation resolves its home
     from its own source, parameters and constituent types; whatever encloses it sees
     only a typed dataset (2.3). No sibling term and no spelling of the enclosing query
     affects a node's home; the query's dimensions enter only as the type of an
     ungrained constituent.
   - **2.6 Anchor and candidates.** The source anchor is the longest common prefix of
     the source leaves' paths — where the source lives: the aggregation's definition
     (a custom aggregation and its parameter defaults) is resolved there. The home,
     when it exists, is among the inputs' paths and their longest common prefix;
     candidates are tried deepest-first, ties preferring the anchor.
   - **2.7 Spelling-invariance.** The home depends only on the inputs' paths and
     types: `customers.spend:sum`, `sum(customers.spend)` and `sum(customers.spend + 0)`
     share one home. A single-column source is the one-leaf case of 2.2 — the model
     its source names.
   - **2.8 Fail closed.** When an input has no home, or no dataset determines every
     input, the aggregation is illegal and the query fails with the input-safety error
     naming the leaf and the hop — never a multiplied or silently re-rooted value.
   - **2.9 Home is not population.** The home is per aggregation node (where it is
     counted); the population (Axiom 12) is per query (which cells exist). A
     population dimension the home does not determine is broadcast, associated or
     refused (Axioms 7–8); it never moves the home.
   [enforced: test:tests/test_dev1892_parameter_typing.py]
   [enforced: test:tests/test_dev1832_home.py]
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
   grain values is a cell. [enforced: test:tests/test_dev1871_grain_retype.py]
6. **An aggregate is a dataset**
   [enforced: test:tests/test_dev1871_terms.py]: it can be filtered, queried,
   and aggregated again; a strictly coarser grain is reached only through an
   explicit second-order aggregation — the implicit collapse is a typed
   error
   [enforced: test:tests/test_dev1824_partitioned_execution.py]; second-order
   aggregation over attached values
   [enforced: test:tests/test_dev1847_reaggregation_exec.py]; aggregation sources
   mixing row-level columns with attached values
   [enforced: test:tests/test_dev1859_row_mixed_exec.py]; a transform is itself
   such an attached value, its cells aggregated at its result grain (Axiom 11)
   [enforced: test:tests/test_dev1832_transform_source.py].
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
   [enforced: test:tests/test_dev1841_association_exec.py]
   [enforced: test:tests/test_dev1910_home_rooted_association.py]; and error (refuse)
   [enforced: test:tests/test_dev1841_error_mode.py]; an explicit partition_by
   naming a dimension unattributable only from a further (cross-model) root is an
   error outside associate mode
   [enforced: test:tests/test_dev1841_association_errors.py], but one whose own
   dependency closure (engine P10) crosses a fanning hop from its host is a
   mode-invariant input-safety error — raised in every mode, associate included,
   since it can never be counted without multiplying rows (Axiom 2.8).
   [enforced: test:tests/test_dev1911_fanning_partition_key.py]
9. **Closure**: every operator consumes and produces aggregates and may
   inspect only its operands' types (grain, home dataset), never how they were
   constructed — any "not supported inside" refusal of a well-typed term is a
   closure violation. [enforced: test:tests/test_law_guard_ratchet.py] [enforced: test:tests/test_dev1919_home_rooted_attached_inputs.py] Typed
   residue, by design and NOT a closure violation: an aggregate in dimension
   position must declare its grain explicitly — the ungrained default (the
   query's dimensions) would include the dimension being defined, a
   self-referential grain, so the explicit `partition_by=` requirement is a
   type rule [enforced: test:tests/test_dev1824_computed_dim_execution.py];
   a transform in dimension position must likewise wrap aggregates, each
   declaring `partition_by=` — two distinct typed errors for the same
   self-referential-grain reason.
   [enforced: test:tests/test_dev1868_residue_split.py]
10. **Grain-union broadcast**: combining aggregates unions their grains, each
    operand broadcast from its own grain to the union — coarser to finer only;
    the population supplies the row set, a cell an operand lacks contributes
    NULL, and combining never removes rows (spec: `queries/semantics` ›
    Grain-union broadcasting). [enforced: test:tests/test_dev1739_execution.py]
11. **Transforms are typed**: a transform consumes an aggregate-valued dataset and
    produces one, consulting only its operand's type (Axiom 9). Its grain is resolved by:
    - **11.1 Operand grain.** The union of the inner aggregates' grains (Axiom 10):
      each explicit `partition_by=`, else the query grain; a windowed inner always
      includes the query's time bucket. An operand naming no explicit aggregation
      (`rank(region)`) is the degenerate query-grain identity — grain = the query
      dimensions, warned like `sum(sum(x))`.
    - **11.2 Timeless.** The rank family preserves the operand grain; its own
      `partition_by=` partitions the operand's cells and must name operand-grain members.
    - **11.3 Time-ordered.** The axis is the query's active time bucket and must be in
      the operand grain; otherwise the transform fails with the `partition_by=` remedy.
      **11.3a Preserving** (`cumsum`, `lag`, `lead`, `time_shift`, `change`,
      `change_pct`, `consecutive_periods`): one value per operand cell, result grain =
      operand grain. **11.3b Collapsing** (`first`, `last`): one value per partition,
      result grain = operand grain minus the axis, realised as the preserving
      evaluation followed by an exact per-partition pick (Axiom 6).
    - **11.4 Position.** As a measure, filter or order key the result is broadcast onto
      the query grain (Axiom 10; a finer result is Axiom 6's implicit collapse); as an
      aggregation-source constituent it is an opaque dataset at its result grain (2.3);
      in dimension position every inner must be explicitly grained (Axiom 9 residue).
    - **11.5 Recursion.** A nested transform is an inner of the enclosing one; its
      result grain joins the enclosing operand's union like any grained inner.
    [enforced: test:tests/test_dev1871_terms.py]
    [enforced: test:tests/test_dev1832_transform_source.py]
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
    [enforced: test:tests/test_dev1909_population_pushdown.py]

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
