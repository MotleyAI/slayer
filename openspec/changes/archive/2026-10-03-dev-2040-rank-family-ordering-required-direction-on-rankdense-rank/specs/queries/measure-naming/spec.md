## MODIFIED Requirements

### Requirement: Unnamed formula measures derive sanitized identifier keys

The result-column key of an unnamed measure whose formula is not a plain
aggregate reference (an arithmetic composite, a transform, or a mix with
literals) SHALL be derived by sanitizing the canonical formula text into a bare
identifier under the product-wide expression-name convention: lowercase, every
run of non-alphanumeric characters collapsed to one `_`, leading/trailing `_`
stripped, a leading digit guarded, names over 48 characters folded to
`<head>_<hash8>_<tail>`, and no `__` in the result. The SQL projection alias
SHALL use the same derived name. In the canonical formula text a rank-family
`direction` SHALL appear as its bare normalised value (`asc` / `desc`), never as
`direction=...`.

#### Scenario: Arithmetic composite

- **WHEN** an unnamed measure `logo_churn:sum / logo_bop:sum` is queried on model `mart`
- **THEN** its result key is `mart.logo_churn_sum_logo_bop_sum`

#### Scenario: Transform formula

- **WHEN** an unnamed measure `time_shift(cmrr_eop:sum, -1, 'year')` is queried on model `mart`
- **THEN** its result key is `mart.time_shift_cmrr_eop_sum_1_year`

#### Scenario: Rank direction spelled as its bare value

- **WHEN** the unnamed measures `rank(sum(a), direction='desc')`,
  `rank(sum(a), direction='Ascending')` and
  `rank(sum(a), partition_by=r, direction='asc')` are queried on model `o`
- **THEN** their result keys are `o.rank_a_sum_desc`, `o.rank_a_sum_asc` and
  `o.rank_a_sum_partition_by_r_asc`, while `ntile(sum(a), n=4)` keeps
  `o.ntile_a_sum_n_4`

#### Scenario: Formatting-insensitive derivation

- **WHEN** the same formula is written with different spacing (`logo_churn:sum/logo_bop:sum`)
- **THEN** it derives the identical result key

#### Scenario: Long formulas hash-fold

- **WHEN** an unnamed formula's sanitized name exceeds 48 characters
- **THEN** the key folds to the `<head>_<hash8>_<tail>` form deterministically
