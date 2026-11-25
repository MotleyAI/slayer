## MODIFIED Requirements

### Requirement: storage upgrade is a storage command

`slayer storage upgrade` SHALL run the store upgrade of `models/stored-upgrade` › slayer storage upgrade persists the current format on the store `--storage` names (the default store otherwise), and SHALL take:
- `--data-source NAME`, which limits the upgrade to one datasource's documents;
- `--dry-run`, which prints each document's difference and writes no document (opening the store still performs its one-time layout moves);
- `--datasource-type NAME=TYPE`, repeatable, which supplies the dialect of a datasource the store holds no config for.

It SHALL print one line per upgraded, unchanged-but-checked, refused or conflicting document. It SHALL exit 0 when every document is current or was upgraded, and 1 when any was refused or conflicted.

#### Scenario: Upgrading one datasource
- **WHEN** `slayer storage upgrade --data-source warehouse` runs on a store with two datasources holding v15 datasets
- **THEN** only `warehouse`'s documents are upgraded and the command exits 0

#### Scenario: A malformed type option
- **WHEN** `slayer storage upgrade --datasource-type warehouse` runs
- **THEN** the command fails with a one-line error naming the expected `NAME=TYPE` form, and nothing is written

#### Scenario: A dry run on a pre-v4 store
- **WHEN** `slayer storage upgrade --dry-run` runs on a YAML store in the pre-v4 flat layout
- **THEN** the store's files move into the per-datasource layout, no document's content changes beyond that move, and the command prints each document's difference
