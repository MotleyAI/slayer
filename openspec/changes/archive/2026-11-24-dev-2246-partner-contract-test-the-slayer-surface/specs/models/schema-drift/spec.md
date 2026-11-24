## ADDED Requirements

### Requirement: Drift checks every base field's source column

Schema drift SHALL check the source column of every base field of a table-backed dataset against the live table. A base field reads one physical column, with or without a `filter`. The missing-column check, the re-type check and the cascade to the fields, metrics, relationships and filters that read a dropped field all take part. A filtered base field whose source column the live table lacks SHALL be dropped like an unfiltered one, and every metric reading it SHALL be dropped with it. Relationship keys and live typing SHALL keep treating a filtered field as no physical reader.

#### Scenario: A filtered field whose column vanished is dropped
- **WHEN** a dataset declares `amount` (`expression: amount`), `paid_amount` (`expression: amount`, `filter: status = 'paid'`) and the metric `paid_revenue` (`sum(paid_amount)`), and the live table no longer has `amount`
- **THEN** the drift removes `amount`, `paid_amount` and `paid_revenue`, each with a reason naming the missing column

#### Scenario: A filtered field over a re-typed column is a re-type finding
- **WHEN** the live type of `amount` becomes text and `paid_amount` reads it under a filter
- **THEN** a re-type finding is reported for `paid_amount` as for `amount`

#### Scenario: Two fields on one column, metrics naming either
- **WHEN** two fields, one filtered, read the quoted physical column `"Amount"`, and metrics sum each of them, and the live table no longer has `"Amount"`
- **THEN** both fields and both metrics are removed

#### Scenario: A filtered field is still not a relationship key
- **WHEN** a relationship names a filtered base field as its key
- **THEN** it is refused as before
