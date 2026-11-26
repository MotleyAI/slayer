## MODIFIED Requirements

### Requirement: Keyword arguments versus comparison

Inside an aggregation call (built-in or model-defined) or a transform call, an unparenthesised argument `name = value` whose left side is an unquoted bare name SHALL be a keyword argument exactly when `name` case-insensitively names a declared parameter of that callee: `window` and `partition_by` for every aggregation, `weight` for `weighted_avg`, `other` for `corr` / `covar_samp` / `covar_pop`, `p` for `percentile`, `percentile_disc` and `percentile_approx`, a model aggregation's declared parameters and formula placeholders, and a transform's parameters (`periods`, `granularity`, `n`, `direction`, `partition_by`). Otherwise it SHALL be a comparison, in any argument position. A parenthesised `(name = value)` or one with a quoted left side SHALL always be a comparison, so `sum((weight = 1))` compares a column `weight`. Duplicate keyword arguments and a positional argument after a keyword argument SHALL be judged after this classification. Inside a scalar function call and everywhere else, `=` SHALL be a comparison.

#### Scenario: Keyword argument

- **WHEN** a measure is `percentile(amount, p=0.9)`, `percentile_disc(amount, p=0.9)` or `percentile_approx(amount, P=0.9)`
- **THEN** `p` is a keyword argument in each

#### Scenario: Comparison inside a scalar call

- **WHEN** a measure is `sum(iif(status = 'paid', amount, 0))`
- **THEN** `status = 'paid'` is the condition of `iif`

#### Scenario: Boolean aggregate

- **WHEN** measures are `sum(status = 'paid')` and `avg(status = 'paid')`, and a model aggregation `share(x)` with no parameter `status` is called as `share(status = 'paid')`
- **THEN** each aggregates the comparison as 0/1 (the paid count, the paid share, and `share` over the comparison)

#### Scenario: A parameter name wins

- **WHEN** a model has a column `weight` and measures are `weighted_avg(amount, weight = qty)`, `sum((weight = 1))` and `sum("weight" = 1)`
- **THEN** `weight` is a keyword argument in the first, and the other two sum the comparison of the column `weight` with 1

#### Scenario: Model aggregation parameters in any case

- **WHEN** a model aggregation `trimmed(x)` declares the parameter `cutoff` and a measure is `trimmed(amount, CUTOFF = 0.1)`
- **THEN** `CUTOFF` binds as the keyword argument `cutoff`

#### Scenario: Classification before argument-order checks

- **WHEN** a measure is `weighted_avg(status = 'paid', weight = qty)`
- **THEN** the first argument is the comparison (the positional source) and `weight` is the keyword argument, so it binds without a positional-after-keyword error
