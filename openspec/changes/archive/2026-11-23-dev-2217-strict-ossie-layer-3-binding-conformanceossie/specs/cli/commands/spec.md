## ADDED Requirements

### Requirement: slayer query takes a conformance
`slayer query` SHALL accept `--conformance` with the values `slayer` (the default) and `ossie-l3`, and SHALL run the query under that conformance exactly as the engine does (`queries/ossie-l3-binding`). A strict refusal SHALL print as the command's one error line, starting with `OssieQueryError: <code>`; any other value SHALL be refused by the argument parser.

#### Scenario: A strict refusal through the CLI
- **WHEN** `slayer query '{"dimensions": ["customers.region"], "metrics": ["orders.total_revenue", "tickets.ticket_count"]}' --conformance ossie-l3` runs on the Appendix B retail model
- **THEN** it exits non-zero with one stderr line starting `OssieQueryError: E_PARTIAL_DIMENSION_REACHABILITY`, naming `customers.region` and `tickets.ticket_count`

#### Scenario: Output names through the CLI
- **WHEN** `slayer query '{"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]}' --conformance ossie-l3` runs on the same model
- **THEN** the output's columns are `region` and `total_revenue`
