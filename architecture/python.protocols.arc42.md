# protocols — BI wire protocols

## 1. Purpose & context

The BI wire-protocols bucket serves SLayer models to BI tools over Arrow Flight
SQL and the Postgres wire protocol, translating their SQL into SLayer queries.

## 2. Building blocks

No view of its own; the bucket appears in the `landscape` view
([system.arc42.md](system.arc42.md) §2).

## 3. Principles

1. **Honest wire catalogs**: every column a BI wire protocol advertises is
   selectable as advertised. [enforced: test:tests/test_dev2172_honest_catalogs.py]

## 4. Rationale

A BI tool builds its queries from the catalog it is shown, so a column it
cannot select turns into a failed query in front of a user (archived change
`2026-10-30-dev-2172-facades-advertise-only-selectable-columns-and-serialise`).
