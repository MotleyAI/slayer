## MODIFIED Requirements

### Requirement: Unsafe aggregate inputs fail closed
An aggregate whose inputs — positional args, keyword args (including aggregation-parameter fragments and the aggregation definition's non-overridden defaults), or measure-level column filter references — cross a join hop that is not provably many-to-one from the aggregate's root SHALL fail with a clear error in all three modes, whatever the aggregate's root: target-rooted, host-rooted, and local aggregates alike. An input's dependencies are its *dependency closure*: its own join path plus every join path the definition of any derived column it names crosses, recursively through chains of derived columns, whether the reference is bare or path-bearing — a derived column whose definition crosses a fanning hop is an unsafe input exactly as a structural reference across that hop is. An input whose derived-column definition cannot be analyzed for join dependencies SHALL fail closed with a typed error naming the aggregate and the column — never be treated as crossing nothing. Multiplying a host-side operand through a fanning join is ambiguous and MUST never silently compute over multiplied rows. The rule applies per input role: a *filter reference* or *argument* crossing an unproven hop fails closed, whatever the aggregate's root. A crossing *source* stays legal only where the aggregate is evaluated over the join result at host grain — a host-grain wrap (an ORDER BY sort key over an unprojected joined column) consumes the target's values per matched row and keeps its established values; a definition default's references local to its owner ride with the source. A target-rooted cross-model producer re-roots its source to the target; a source that then reads through an unproven hop fans the aggregate and fails closed like any other crossing input.

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
- THEN the query fails with the unproven-join-hop error naming `region_events`, never emitting the multiplying join; the same holds when the argument names a derived column defined over that derived column (`bad_pop * 2`)

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
