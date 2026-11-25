## MODIFIED Requirements

### Requirement: Library growth never invalidates a stored artefact

Every name the Mode-B library reserves — scalar and list functions, function spellings, date-part aliases, transforms, built-in granularities, built-in aggregations, and names reserved with no meaning (`agg`) — SHALL be pinned, so a newly reserved name is only accepted when it is a function or function spelling (which a model's own definition shadows) or when a stored-upgrade migration covers stored documents already using it. A stored model whose custom aggregation takes any reserved function or spelling name SHALL load and answer its aggregation, and a stored datasource whose custom granularity takes any reserved name SHALL load after migration and answer through the renamed granularity.

Every name a custom granularity may not take SHALL have a rename gate: the dataset, query and memory versions current when the library reserved it. Every name reserved when the gates were introduced SHALL have the gate dataset 30, query 19, memory 14. A gate added later SHALL be strictly higher, for each of the three, than every earlier gate, so the release that reserves a name also steps the versions its dependents are written at.

#### Scenario: Every reserved function name as a stored aggregation

- **WHEN** for each reserved function and spelling name a stored model defines a custom aggregation of that name with formula `SUM({value})`
- **THEN** each model loads and the aggregation returns the sum of its column

#### Scenario: Every reserved name as a stored granularity

- **WHEN** for each reserved name other than a built-in granularity (never storable as a custom one) a stored datasource defines a custom granularity of that name used by a stored model's time dimension written below its gate
- **THEN** the datasource and model load and the time dimension buckets by the renamed granularity

#### Scenario: An unpinned reservation is caught

- **WHEN** the library reserves a name outside the pinned set that is neither a function nor a function spelling and no migration covers it
- **THEN** the library-growth check fails, naming the unpinned name

#### Scenario: A reservation without a rename gate is caught

- **WHEN** the library reserves a name, a function included, that has no rename gate
- **THEN** the library-growth check fails, naming the name

#### Scenario: A later gate that does not step the versions is caught

- **WHEN** a newly reserved name's gate is not higher than every earlier gate for the dataset, query and memory versions
- **THEN** the library-growth check fails, naming the name

### Requirement: Stored granularities colliding with a library name are renamed

On every load, whatever the datasource's version, a stored datasource custom granularity whose name case-insensitively matches any reserved library name SHALL be renamed to `<name>_granularity`, or `<name>_granularity_<n>` with the smallest `n` ≥ 2 that is free, and the renamed granularity SHALL record its former name as `renamed_from`. A stored granularity carrying `renamed_from` SHALL keep that record. The datasource's renames SHALL be its pending renames together with its recorded ones; a record SHALL decide the new name of its former name, and when two granularities record one former name, the first in list order SHALL decide it.

A reference to a former name SHALL be rewritten to the new name, in every position a granularity name is accepted, only in a document of that datasource written below that former name's rename gate: a dataset, a saved query, a query-backed dataset's stages, or a memory. A nested stored query SHALL be judged by its own version, and a document without a version SHALL count as old. A document at or above the gate SHALL be read as written.

Each pending rename SHALL be logged as a warning naming the datasource and both names; a recorded rename SHALL log nothing. Every rename SHALL apply in memory on every load, which writes nothing. This SHALL hold on both the YAML and the SQLite store. A client saving a custom granularity with a reserved name SHALL still be refused.

#### Scenario: Granularity named left

- **WHEN** a stored datasource defines the granularity `left` and a stored model's query-backed source and a memory's query, both written below the gate, bucket `created_at` by it
- **THEN** after load the granularity is `left_granularity` with `renamed_from: left`, both documents reference `left_granularity`, a warning names the rename, both queries return the rows they returned before, and no stored document changes

#### Scenario: Granularity named collect

- **WHEN** a stored datasource defines the granularity `collect`
- **THEN** it loads as `collect_granularity`, recording `renamed_from: collect`

#### Scenario: The new name is taken

- **WHEN** a stored datasource defines the granularities `left` and `left_granularity`
- **THEN** `left` is renamed to `left_granularity_2` and references to `left_granularity` are untouched

#### Scenario: A recorded rename renames an old document that arrives later

- **WHEN** a store whose datasource was upgraded to `left_granularity` with `renamed_from: left` receives a dataset written below the gate that buckets by `left(created_at)`
- **THEN** the dataset loads reading `left_granularity(created_at)`, returns the rows it returned before, and no rename warning is logged

#### Scenario: A current document is read as written

- **WHEN** a stored datasource defines `left_granularity` with `renamed_from: left`, or still defines the pending `left`, and a dataset at version 30 calls the library function `left(name, 2)`
- **THEN** the dataset loads with `left(name, 2)` unchanged

#### Scenario: A nested stage is judged by its own version

- **WHEN** a dataset at version 30 holds a stored stage at query version 18 that buckets by `left(created_at)`, and its datasource records `renamed_from: left`
- **THEN** the stage loads reading `left_granularity(created_at)`

#### Scenario: An interrupted load completes

- **WHEN** `slayer storage upgrade` is interrupted after storing the datasource with its record but before storing some dependents, or after storing some dependents but before the datasource
- **THEN** a load in between reads every document with the new name, and the next run stores the rest, leaving one consistent store

#### Scenario: A rename converges

- **WHEN** a store whose granularity `contains` was renamed by `slayer storage upgrade` is loaded and upgraded again, on the YAML and on the SQLite store
- **THEN** no document changes and no rename warning is logged

#### Scenario: A granularity colliding with a later library addition

- **WHEN** a later library release reserves `left` with a gate above the current versions, a datasource stored at the current version defines the granularity `left`, and a stored query written at the current query version buckets by `left(created_at)`
- **THEN** on load the granularity is `left_granularity`, the query reads `left_granularity(created_at)`, and it returns the rows it returned before

#### Scenario: A new reserved-name granularity is refused

- **WHEN** a client saves a datasource defining the granularity `left`
- **THEN** the save fails naming the reserved name

### Requirement: A load never writes

Loading a stored dataset, semantic-model document, saved query, memory or datasource SHALL NOT write to the store, on the YAML and the SQLite store alike. Every migration, repair and conversion a stored document needs SHALL apply in memory on each load. This includes schema migrations, join-key and join-target repairs, legacy alias rewrites, field `sql` conversion, query upgrades and the library-granularity settle. The upgraded form SHALL be persisted only by a save of that document or by `slayer storage upgrade`. Datasource priority and embeddings SHALL be neither read as documents to upgrade nor rewritten. Opening a store SHALL keep its one-time layout and schema moves (the YAML pre-v4 flat-layout move, the legacy memory-file split, the SQLite table rebuilds, the embedding sidecar's kind rename), which change no document's content, except that the flat-layout move fills an empty `data_source` when exactly one datasource is registered; they are the only writes opening a store performs.

#### Scenario: Loading an old store leaves it unchanged
- **WHEN** the committed 0.10.2 and 1.8.2 stores, as YAML and as SQLite, are opened and every dataset, saved query, memory and datasource is loaded twice
- **THEN** every document loads with its upgraded content both times, the logical content of every stored document is unchanged, and no storage write hook is called

#### Scenario: A save persists the upgraded form
- **WHEN** a dataset stored below the current version is loaded and then saved
- **THEN** the stored document is at the current version with the upgraded content

#### Scenario: Opening a pre-v4 flat-layout YAML store
- **WHEN** a YAML store in the pre-v4 flat layout is opened
- **THEN** its files move into the per-datasource layout, an empty `data_source` is filled when exactly one datasource is registered, and no other content changes

### Requirement: A load warns when a document needs upgrading

Loading a stored document that the upgrade would change SHALL emit one structured warning per document per storage instance. The warning SHALL name the entity kind, the document's identity and `slayer storage upgrade`. It is emitted whether the change comes from the document's own version, from a nested stage, removed form or repair, or from a rewrite of stored input that the document's model applies while validating (at any nesting depth). Filling a default for an omitted key SHALL NOT count as a change. A document the upgrade would not change SHALL load without that warning.

#### Scenario: An old dataset warns once
- **WHEN** a dataset stored at version 15 is loaded three times through one storage instance
- **THEN** exactly one warning names the dataset, its datasource and `slayer storage upgrade`

#### Scenario: A current document with an old nested stage warns
- **WHEN** a query-backed dataset stored at the current version holds a stage written in a form the upgrade converts
- **THEN** loading it warns naming the dataset

#### Scenario: A validator-level rewrite warns and is persisted
- **WHEN** a datasource stored at the current version spells its user as `user`
- **THEN** loading it warns naming the datasource, and `slayer storage upgrade` stores it with `username`

#### Scenario: A defaulted key does not warn
- **WHEN** a datasource stored at the current version defines a custom granularity without an `origin`
- **THEN** loading it emits no upgrade warning

#### Scenario: An upgraded store is quiet
- **WHEN** a store that `slayer storage upgrade` has upgraded is loaded
- **THEN** no upgrade warning is emitted

### Requirement: slayer storage upgrade persists the current format

`slayer storage upgrade` SHALL bring every stored document that needs it to the current format, through the same upgrade a load applies. The documents are datasets, semantic-model documents, saved queries, memories and datasources, optionally limited by `--data-source`. Each changed document SHALL be written with its upgraded content and without the semantic checks of a client save, so a legacy document that loads (for instance one holding a cycle or a fanning reference) is upgraded rather than refused. A document SHALL be written only if its stored form is unchanged since the upgrade read it. Otherwise it SHALL be reported as a conflict and left as it is. Documents SHALL be written in no particular order, and no document SHALL be held back because another was refused, conflicted or unreadable. `--dry-run` SHALL print, per document, the difference the upgrade would make and SHALL write no document; opening the store still performs its one-time layout moves.

A document the upgrade cannot complete SHALL be reported with its identity and the reason, and SHALL be left untouched, while every other document is upgraded. The command SHALL then exit 1. Such documents include:
- one that fails to load, including a memory file that is not UTF-8 text;
- one with field `sql` waiting for a datasource dialect that neither the store nor `--datasource-type NAME=TYPE` supplies;
- one below version 8 whose datasource cannot be reached for type refinement.

`--datasource-type NAME=TYPE`, repeatable, SHALL supply the dialect of a datasource the store holds no config for.

#### Scenario: An old store upgrades
- **WHEN** `slayer storage upgrade` runs on a store holding v15 datasets, a v15 memory and a datasource
- **THEN** every document is stored at the current version, loading the store afterwards emits no upgrade warning, every query returns what it returned before, and the command exits 0

#### Scenario: A dry run shows the change and writes nothing
- **WHEN** `slayer storage upgrade --dry-run` runs on the same store
- **THEN** it prints each document's difference, every stored document's content is unchanged, and the command exits 0

#### Scenario: A legacy cycle is upgraded, not refused
- **WHEN** the store holds a dataset written before the current version whose derived fields form a cycle that a client save would refuse
- **THEN** the upgrade stores it at the current version

#### Scenario: A dataset waiting for its dialect
- **WHEN** the store holds a v15 dataset with field `sql` `UPPER(status)` and no config for its datasource
- **THEN** the upgrade reports the dataset and leaves it untouched, upgrades every other document and exits 1; run again with `--datasource-type warehouse=duckdb`, it upgrades the dataset and exits 0

#### Scenario: A corrupt document does not stop the others
- **WHEN** one stored dataset is corrupt and the others are valid
- **THEN** the valid ones are upgraded, the corrupt one is reported and untouched, embeddings are preserved, and the command exits 1

#### Scenario: An undecodable memory file does not stop the others
- **WHEN** a YAML store holds one memory file that is not UTF-8 text beside a valid v15 memory and a v15 dataset
- **THEN** a whole-store run reports that memory by its id and leaves it untouched, upgrades the others and exits 1; a `--data-source` run upgrades the dataset without failing on the memory

#### Scenario: A concurrent edit is not overwritten
- **WHEN** a document is changed in the store after the upgrade read it and before it would write it
- **THEN** the upgrade reports a conflict for it, leaves the newer content in place and exits 1

#### Scenario: An interrupted settle converges
- **WHEN** an upgrade that renames a datasource's library-colliding granularity is interrupted after writing some documents, whichever they are
- **THEN** a second run completes the rename and leaves one consistent store

#### Scenario: A refused dependent holds its datasource's rename
- **WHEN** a datasource defines the granularity `left`, one dependent dataset upgrades and another conflicts
- **THEN** the datasource is not held back: it is stored with `left_granularity` and `renamed_from: left`, which keeps the rename for the conflicted dataset; the command exits 1, the conflicted dataset still loads with `left_granularity`, and a later run that can complete it leaves one consistent store

### Requirement: Documents SLayer does not own upgrade without a store

A public entry point SHALL take one datasource's stored dataset documents, an optional datasource type and an optional datasource config, and return each document in the current format. It SHALL apply the same upgrade a load applies, open no store and write nothing. The result of each document SHALL be the same as `slayer storage upgrade` would store. A document the entry point cannot complete SHALL be reported with its identity and the reason that `slayer storage upgrade` gives for it, and SHALL not be returned upgraded, while every other document is returned. Such documents include one that fails to validate, one waiting for a dialect, and one below version 8 that needs live type refinement.

#### Scenario: A v15 model set upgrades in memory
- **WHEN** two v15 dataset documents of one datasource, one with field `sql` `UPPER(status)`, are passed with the datasource type `duckdb`
- **THEN** both come back at the current version, every key is one the dataset model declares at its level, they validate to the same datasets a store would load, and nothing is written

#### Scenario: A pre-v8 document is refused
- **WHEN** a v5 dataset document with a refineable DOUBLE field is passed
- **THEN** it is reported as needing live type refinement through `slayer storage upgrade`

#### Scenario: An invalid document is refused on its own
- **WHEN** a valid document and one whose field has an unknown data type are passed together
- **THEN** the valid one comes back upgraded and the invalid one is reported with its name and the validation reason

#### Scenario: The same refusal as the store upgrade
- **WHEN** the same pre-v8 or dialect-waiting document is upgraded through the entry point and through `slayer storage upgrade`
- **THEN** both report it with the same reason

## ADDED Requirements

### Requirement: A datasource save keeps its granularity rename records

Saving a datasource SHALL keep every rename the stored datasource of that name holds, pending or recorded. When no granularity of the saved config records that former name and the saved config defines its new name, the stored result SHALL carry `renamed_from` with the former name if that granularity has none; if it records another former name, the save SHALL be refused with an error naming the granularity, and nothing SHALL be written. A save SHALL NOT be refused because a rename is pending. A saved `renamed_from` SHALL name a reserved library name and SHALL be unique, case-insensitively, among the datasource's granularities; otherwise the save SHALL be refused with an error naming the granularity, and nothing SHALL be written. This SHALL hold on both the YAML and the SQLite store, through every save path.

#### Scenario: Saving a loaded datasource whose rename is pending

- **WHEN** a stored datasource defines the granularity `left`, a stored dataset written below the gate buckets by it, and the datasource as loaded (with `left_granularity`) is saved
- **THEN** the save succeeds, the stored datasource defines `left_granularity` with `renamed_from: left`, and after reopening the store the dataset still loads and buckets by `left_granularity`

#### Scenario: A config built without the record keeps it

- **WHEN** the stored datasource defines `left_granularity` with `renamed_from: left`, and a config defining `left_granularity` without `renamed_from` is saved under its name
- **THEN** the stored datasource defines `left_granularity` with `renamed_from: left`

#### Scenario: A record replaced by another former name is refused

- **WHEN** the stored datasource defines `left_granularity` with `renamed_from: left`, and a config defining `left_granularity` with `renamed_from: collect` is saved under its name
- **THEN** the save fails naming `left_granularity`, and nothing is written

#### Scenario: A record moved to another granularity is kept

- **WHEN** the stored datasource defines `left_granularity` with `renamed_from: left`, and a config defining `left_granularity` without `renamed_from` and `left_g` with `renamed_from: left` is saved under its name
- **THEN** the save succeeds and the stored datasource records `renamed_from: left` on `left_g` only

#### Scenario: A record naming no reserved name is refused

- **WHEN** a client saves a datasource whose granularity `fw` records `renamed_from: fiscal_week`
- **THEN** the save fails naming `fw`, and nothing is written

#### Scenario: Two records of one former name are refused

- **WHEN** a client saves a datasource whose granularities `left_granularity` and `left_g` both record `renamed_from: left`
- **THEN** the save fails naming the second, and nothing is written

## REMOVED Requirements

### Requirement: A datasource save waits for its pending granularity rename

**Reason**: The renamed granularity now records its former name, so saving a datasource no longer drops a pending rename. The save keeps the records instead of being refused.

**Migration**: Nothing to do. A datasource save that was refused while a rename was pending now succeeds and stores the rename record. `slayer storage upgrade` still stores every dependent document under the new name.
