## MODIFIED Requirements

### Requirement: Legacy spellings keep their meaning

`==`, `!=`, `is None`, `is not None`, `True`, `False`, `None`, `[…]` lists after `in` and the method-call aggregation form (`customers.sum(revenue)`) SHALL keep their meaning, result keys and SQL. Every expression that parsed before this change and contains none of the forms this change removes, and no `--` (which starts a SQL comment), SHALL bind to the same value with byte-identical generated SQL.

#### Scenario: Python spellings

- **WHEN** a filter is `status == 'paid' and region is not None` and another is `status = 'paid' AND region IS NOT NULL`
- **THEN** both render identical SQL

#### Scenario: Pre-existing expressions are unchanged

- **WHEN** the SQL golden baselines committed before this change are regenerated
- **THEN** they are byte-identical
