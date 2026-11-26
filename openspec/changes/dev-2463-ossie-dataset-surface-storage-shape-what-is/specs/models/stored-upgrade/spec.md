## ADDED Requirements

### Requirement: Standalone datasets stored by 2.0 upgrade into the stored format
A load SHALL read each 2.0 standalone dataset (YAML `models/<data_source>/<name>.yaml`, SQLite `models` rows) through the existing upgrade, and present it as a dataset of its datasource model (`models/stored-format`), with the upgrade warning of "A load warns when a document needs upgrading" and without writing. `slayer storage upgrade` SHALL, per datasource, write each such dataset as a dataset document and write the datasource model listing them, then delete the legacy files or rows. A 2.0 document already named like its datasource SHALL become the datasource model and gain `dataset_refs`. On the way, a dataset's `meta` entries `osi_ai_context`, `osi_nonportable_extensions`, `osi_is_time` and `osi_unique_keys` SHALL become its core `ai_context`, `nonportable_extensions`, `dimension.is_time` and `unique_keys`, and its `sql_table` SHALL become a `source` spelled per `models/table-names`. The upgrade of a datasource SHALL run under the datasource's write serialisation and SHALL compare every participating legacy file or row and model document with what it read; on any difference it SHALL leave the datasource's legacy form intact and report a conflict. An interrupted upgrade SHALL converge when run again. A dataset the upgrade refuses SHALL stay in its legacy form while the other datasets of its datasource are upgraded. A save of a legacy standalone dataset SHALL store it in the final form and remove its legacy file or row. A store holding both forms SHALL read correctly. Backends that do not implement the semantic-model hooks, and the public entry point of "Documents SLayer does not own upgrade without a store", SHALL keep the 2.0 dataset form.

#### Scenario: A 2.0 store upgrades
- **WHEN** `slayer storage upgrade` runs on a 2.0 YAML store whose datasource `shop` holds standalone datasets `orders` (`sql_table: public.Orders` on Postgres, a composite primary key, `meta.osi_ai_context`) and `customers`, with a relationship between them
- **THEN** `semantic_models/shop/` holds `orders.dataset.yaml`, `customers.dataset.yaml` and the model document `shop` listing both, with the relationship in its `relationships`; `orders` has `source: public."Orders"`, a dataset-level `primary_key` and its `ai_context` in core; `models/shop/` is gone; and every query returns what it returned before

#### Scenario: Loading before the upgrade writes nothing
- **WHEN** the same 2.0 store is loaded before the upgrade
- **THEN** `orders` and `customers` load as datasets of the datasource model `shop` with the upgrade warning, and no file changes

#### Scenario: A same-named document is adopted
- **WHEN** a 2.0 store holds the document `shop` in `semantic_models/shop/` and standalone dataset `returns` of datasource `shop`
- **THEN** the upgrade adds `returns` to `dataset_refs` of document `shop` and changes nothing else of its text

#### Scenario: A concurrent edit is not overwritten
- **WHEN** a legacy standalone dataset is changed after the upgrade read it and before the upgrade wrote its datasource
- **THEN** the datasource's legacy form is intact, the upgrade reports a conflict and exits 1, and a later run upgrades it
