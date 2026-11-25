## Why

The canonical text SLayer derives from a parsed Mode-B expression drops the double quotes of every segment whose bare
spelling is an identifier, so `lower("REGIONS".lbl)` becomes `lower(REGIONS.lbl)`. A quoted segment matches only its
exact spelling (`queries/identifier-case`), and quoting also marks a word as a column rather than a unit, date part,
keyword argument or literal, so any text that passes through canonical rendering and is read again changes meaning:
a pre-2.0 stored extension column reading `lower("REGIONS".lbl)` where only `Regions` exists is inlined
case-insensitively instead of being refused.

## What Changes

- Canonical expression text quotes a reference segment when it was written quoted, or when its bare spelling would not
  read back as that unquoted name in its position (not an identifier, or a reserved word such as `NULL`, `TRUE`,
  `FALSE`, `NOT` where the grammar reads it as a keyword); every other segment stays bare. The text parses back with the
  same segments and the same quoting.
- Telling expressions apart before binding stays quote-blind: expressions that differ only in quoting remain one
  expression for time-dimension and refinement de-duplication, as today.
- Result keys and generated result names are unchanged by quoting.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/expression-grammar`: adds the requirement that canonical expression text keeps identifier quoting.

## Impact

- `slayer/core/syntax.py` — the canonical renderer gains a quoting policy independent of its legacy spelling flag.
- `slayer/core/query.py` — the pre-binding identity helpers use the quote-blind form; stored extension-field
  conversion reads the faithful text.
- `slayer/sql/naming.py` — the `<source>_<agg>` alias base uses the quote-blind form.
- Every consumer that re-reads canonical text (stored-field inlining, query-time extension inlining, model-scoped metric
  expansion, formula lifting, `metric_text_at_path` in the catalog and PG facade) now keeps quoted meaning.
