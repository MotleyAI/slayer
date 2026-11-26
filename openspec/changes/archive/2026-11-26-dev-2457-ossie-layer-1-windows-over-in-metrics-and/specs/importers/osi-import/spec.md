## MODIFIED Requirements

### Requirement: Import stores the document natively, verbatim
`slayer import-osi <path> --datasource X` SHALL store each valid current-shape document it reads as a native semantic-model document of datasource `X` (`models/semantic-model-documents`), named by the document's `name`, with its text unchanged. It SHALL NOT introspect the datasource and SHALL NOT need it to be reachable, except to upgrade a legacy document ("Legacy wrapper documents keep importing"). Importing a document over an existing document of the same name SHALL replace it. Whether SLayer can evaluate each entity SHALL NOT be checked at import: an entity SLayer cannot evaluate yet SHALL be stored and listed in the import report with the reason.

#### Scenario: Import keeps comments and needs no database
- **WHEN** a valid document with comments is imported into a datasource whose database is unreachable
- **THEN** the import succeeds and the stored text equals the file's text

#### Scenario: A windowed metric is reported, not refused
- **WHEN** upstream's `tpcds_semantic_model.yaml` is imported
- **THEN** the document is stored whole with its text unchanged, and the report lists none of `cumulative_sales`, `brand_rank_in_store` and `monthly_sales_change` as not evaluable

#### Scenario: The TPC-DS window metrics evaluate
- **WHEN** the imported TPC-DS document is queried over a small fixture of `store_sales`, `date_dim`, `store` and `item` for `cumulative_sales` by `date_dim.d_date`, `brand_rank_in_store` by `store.s_store_sk` and `item.i_brand`, and `monthly_sales_change` by `date_dim.d_year` and `date_dim.d_moy`
- **THEN** each returns the values the metric's SQL gives when run by hand over the grouped fixture, and each queried without the grouping its window names fails with a typed error naming the missing key
