## MODIFIED Requirements

### Requirement: Unsafe aggregate inputs fail closed
An aggregate whose inputs — positional args, keyword args (including aggregation-parameter fragments and the aggregation definition's non-overridden defaults), or measure-level column filter references — cross a join hop that is not provably many-to-one from the aggregate's root SHALL fail with a clear error in all three modes, whatever the aggregate's root: target-rooted, host-rooted, and local aggregates alike. An input's dependencies are its *dependency closure*: its own join path plus every join path the definition of any derived column it names crosses, recursively through chains of derived columns, whether the reference is bare or path-bearing — a derived column whose definition crosses a fanning hop is an unsafe input exactly as a structural reference across that hop is. An input whose derived-column definition cannot be analyzed for join dependencies SHALL fail closed with a typed error naming the aggregate and the column — never be treated as crossing nothing. Multiplying a host-side operand through a fanning join is ambiguous and MUST never silently compute over multiplied rows. The rule applies per input role: a *filter reference* or *argument* crossing an unproven hop fails closed, whatever the aggregate's root. A crossing *source* stays legal only where the aggregate is evaluated over the join result at host grain — a host-grain wrap (an ORDER BY sort key over an unprojected joined column) consumes the target's values per matched row and keeps its established values; a definition default's references local to its owner ride with the source. A target-rooted cross-model producer re-roots its source to the target; a source that then reads through an unproven hop fans the aggregate and fails closed like any other crossing input. An attached (aggregate- or transform-valued) input is opaque to this rule: its own inputs are judged by its own producer at its own home, never by the enclosing aggregate. When an explicit column argument is the violation, the error SHALL name that argument and the hop it crosses, taking precedence over the closure's hop-only message — a derived argument included, judged by its closure. The *ranking key* of a `first`/`last` aggregation is an input under this rule whichever way it is chosen: the explicit positional argument, else the producer's first temporal row dimension, else the time dimension's raw column, else the model's `default_time_dimension` — a key whose dependency closure crosses a hop that is not provably many-to-one from the producer's root SHALL fail closed in every mode, naming the column and the hop, for host-rooted, target-rooted and `window=` producers alike, and a key whose derived definition cannot be analyzed SHALL fail closed naming the column; a ranked producer's grain may still fan (each cell ranks the rows that reached it) — only the ordering key is refused.

#### Scenario: Aggregate reading through an unproven join errors
- WHEN an aggregate's column filter references a column across a join with unproven arity from the aggregate's root
- THEN the query fails with an error naming the input and the join hop, and the remedy

#### Scenario: Local measure with a filter over a provably safe hop keeps exact values
- WHEN a local measure's column filter references a column reached over a provably many-to-one join
- THEN the query executes with values identical to the pre-unification behavior

#### Scenario: Local measure with a filter over an unproven hop errors instead of fanning
- WHEN a local measure's column filter references a column across an unproven or one-to-many hop
- THEN the query fails with an error naming the hop and the remedy — never the silently multiplied aggregate this shape previously produced

#### Scenario: Host-grain wrap over a to-many source stays legal
- WHEN a query orders by an aggregate of a joined column evaluated at host grain across a to-many join
- THEN the aggregate evaluates over the joined rows as before, with unchanged executed values

#### Scenario: Path-bearing derived argument crossing a fanning hop fails closed
- WHEN a query rooted at `orders` selects `customers.spend:weighted_avg(weight=customers.regions.bad_pop)`, where `regions.bad_pop` is defined as `pop + region_events.value` over the one-to-many `regions → region_events` hop, in any `to_many_handling` mode
- THEN the query fails with the unproven-join-hop error naming the argument `bad_pop` and the hop `region_events`, never emitting the multiplying join; the same holds when the argument names a derived column defined over that derived column (`bad_pop * 2`)

#### Scenario: Local aggregate with a derived crossing argument fails closed
- WHEN a query rooted at `orders` selects `amount:weighted_avg(weight=customers.regions.bad_pop)`
- THEN the query fails with the unproven-join-hop error naming `region_events`

#### Scenario: Definition default naming a fanning derived column fails closed
- WHEN a custom aggregation on `customers` defaults a parameter to `regions.bad_pop`, or to the expression `regions.bad_pop * 1`, and a query selects `customers.spend:<that aggregation>`
- THEN the query fails with the unproven-join-hop error naming `region_events`

#### Scenario: Measure-local filter naming a fanning derived column fails closed
- WHEN a column's measure-level filter references a derived column on a joined model whose definition crosses a fanning hop (`regions.bad_pop > 0` on a `customers` column) and a query aggregates that column
- THEN the query fails with the unproven-join-hop error naming `region_events`

#### Scenario: Derived dependencies over provably to-one hops stay legal
- WHEN an argument or definition default names a derived column reached over a provably to-one path whose own definition is local to its model (`regions.derived_pop` = `pop * 2`)
- THEN the query executes with the derived expansion applied, by executed values, unchanged from today

#### Scenario: Unanalyzable derived definition fails closed
- WHEN an aggregate input names a derived column whose definition no supported dialect can parse
- THEN the query fails at plan time with a typed error naming the aggregate and the column, containing no issue reference — never a plan that treats the column as crossing nothing

#### Scenario: Host column as a target ranking key names the column and the hop
- WHEN a query rooted at `orders` selects `customers.spend:last(ordered_at)` — a host column ranking a `customers`-rooted pick
- THEN the query fails in every mode with the input-safety error naming `ordered_at`, that it is not attributable from `customers`, and the fanning hop to `orders`

#### Scenario: Argument violation is reported ahead of a source violation
- WHEN a target-rooted aggregate both reads its source through a fanning derived definition and ranks or weights by a host column, e.g. `customers.regions.bad_pop:last(ordered_at)` rooted at `orders`
- THEN the error names the argument `ordered_at` and its hop, not the source's hop — the more specific violation wins

#### Scenario: Implicit model-default ranking key across an unproven hop fails closed
- WHEN `orders.default_time_dimension` names a derived column `li_ts` defined as `line_items.created_at` over the undeclared, reverse-PK-only `orders → line_items` hop, and a query selects `amount:last` (or `amount:first`) with no time dimension and no temporal dimension
- THEN the query fails in every mode with the input-safety error naming `li_ts`, the hop `line_items` and the host `orders` — never the ranked CTE joining `line_items` and ranking over multiplied rows

#### Scenario: Target model default ranking key across an unproven hop fails closed
- WHEN `line_items.default_time_dimension` names a derived column `sh_ts` defined as `shipments.shipped_at` over an unproven `line_items → shipments` hop, and a query rooted at `orders` selects `line_items.qty:last` with no time dimension
- THEN the query fails in every mode with the cross-model input-safety error naming `sh_ts`, that it is not attributable from `line_items`, and the hop `shipments`

#### Scenario: Fanning temporal dimension as the implicit ranking key fails closed in every mode
- WHEN a query rooted at `orders` groups by `line_items.created_at` (a TIMESTAMP column across the unproven `orders → line_items` hop) and selects `amount:last`, under `broadcast`, `error` or `associate`, with or without `window=` on the measure alongside a safe time dimension
- THEN the query fails with the input-safety error naming `created_at` and the hop `line_items`, exactly as `amount:last(line_items.created_at)` does — never a ranked CTE partitioning and ranking by the fanning column

#### Scenario: Time dimension across an unproven hop as the implicit ranking key fails closed
- WHEN a query rooted at `orders` declares `line_items.created_at` as its only time dimension and selects `amount:last`
- THEN the query fails with the input-safety error naming `created_at` and the hop `line_items`

#### Scenario: Explicit derived ranking argument names the argument and the hop
- WHEN a query rooted at `orders` selects `amount:last(li_ts)`, `li_ts` being the derived column across the unproven hop
- THEN the error names the argument `li_ts` and the hop `line_items` (the "ranks/reads by" form), not the closure's hop-only message; `amount:last(line_items.created_at)` keeps naming `created_at` and `line_items`

#### Scenario: Default ranking key whose column filter crosses an unproven hop fails closed
- WHEN `orders.default_time_dimension` names a column whose `Column.filter` references `line_items.qty` across the unproven hop, and a query selects `amount:last` with no time dimension
- THEN the query fails with the input-safety error naming that column and the hop `line_items`

#### Scenario: Unanalyzable default ranking key fails closed
- WHEN `orders.default_time_dimension` names a derived column whose definition no supported dialect can parse, and a query selects `amount:last` with no time dimension
- THEN the query fails at plan time with the typed unanalyzable-dependency error naming the aggregate and the column — never a plan that ranks by it

#### Scenario: Safe ranking keys keep executing
- WHEN the resolved ranking key is a local column (the model default, or a filter-only temporal column with no default), a derived column over a provably to-one hop (`cust_signup` = `customers.signup_at` with `customers.id` a primary key), or — for `amount:last(window='30d')` on a `created_at` time dimension — the bucket's raw column while the model default crosses the unproven hop
- THEN each query executes with its established value (the proven-hop case joins `customers` and picks the row with the latest signup; the windowed case never joins `line_items`), and the derived key renders as its plain expansion with no added CAST in both the plain and the windowed ranked CTE
