# importers — importers

## 1. Purpose & context

The importers bucket turns dbt, Cube and OSI semantic-layer definitions into
SLayer models and reports what it drops.

## 2. Building blocks

No view of its own; the bucket appears in the `landscape` view
([system.arc42.md](system.arc42.md) §2).

## 3. Principles

1. **Importers preserve meaning**: an importer emits a construct only when
   SLayer reads it with the source's meaning, up to SLayer's documented NULL
   conventions; anything else is dropped with a report.
   [enforced: test:tests/test_dev2171_importers_preserve_meaning.py]

## 4. Rationale

An import that changes what a construct means yields models that answer wrong
without warning; dropping with a report keeps every imported answer
trustworthy (archived change
`2026-10-30-dev-2171-importers-preserve-meaning-cube-raw-sql-measures-self`).
