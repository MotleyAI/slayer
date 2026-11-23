## ADDED Requirements

### Requirement: Division and rounding follow Ossie's numeric semantics

On every Tier-1 dialect, `/` SHALL be true division: the quotient keeps its fractional part whatever the operand types, two INT operands included. Two-argument `round(x, d)` over an exact-numeric (DECIMAL) `x` SHALL break an exact tie away from zero. For a floating-point `x`, ties SHALL be the database's own, since a binary value is rarely an exact tie; this is documented.

#### Scenario: Integer operands divide exactly

- **WHEN** a dimension is `i / 2` over INT rows holding `5`, `-5` and `4`
- **THEN** it yields `2.5`, `-2.5` and `2` on every Tier-1 dialect

#### Scenario: Decimal ties round away from zero

- **WHEN** dimensions are `round(d, 0)` and `round(e, 2)` over DECIMAL columns, `d` holding `2.5` and `-2.5` and `e` holding `0.125` and `-0.125`
- **THEN** they yield `3`, `-3`, `0.13` and `-0.13` on every Tier-1 dialect
