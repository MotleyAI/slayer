# search/event-loop Specification

## Purpose
Keeps a server's event loop responsive while search work that depends on optional extras (availability checks, library imports, embedding token truncation) runs.

## Requirements

### Requirement: Search's optional-extra work never blocks the event loop

Search work reached from asynchronous code — each check that an optional extra is available, including importing that extra's library, and embedding token truncation — SHALL run without blocking the event loop.

#### Scenario: A model refresh with a blocking embedding availability check

- **WHEN** a model's search entries are refreshed through ingestion or a model edit while the embedding availability check blocks
- **THEN** the event loop keeps running other tasks until the check returns, and the refresh then completes

#### Scenario: A search with a blocking embedding availability check

- **WHEN** a search with a non-blank question reaches the embedding channel while the embedding availability check blocks
- **THEN** the event loop keeps running other tasks until the check returns, and the search then completes

#### Scenario: Memory and datasource refreshes with a blocking embedding availability check

- **WHEN** a memory is saved, or a datasource's search entry is refreshed, while the embedding availability check blocks
- **THEN** the event loop keeps running other tasks until the check returns

#### Scenario: A cypher-filtered search with a blocking graph availability check

- **WHEN** a search with a cypher filter runs while the graph availability check blocks
- **THEN** the event loop keeps running other tasks until the check returns, and the search then completes

#### Scenario: An embedding batch with blocking token truncation

- **WHEN** a batch of texts is embedded while the embedding channel is available and token truncation blocks
- **THEN** the event loop keeps running other tasks until truncation returns

#### Scenario: A stubbed or reset availability check is honoured

- **WHEN** the availability check is replaced with a stub, or its cached result is cleared, before asynchronous search work runs
- **THEN** the asynchronous work sees the stub's result, or a freshly computed result, and still runs the check off the event loop
