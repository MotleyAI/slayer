## MODIFIED Requirements

### Requirement: Deleting document entities runs the dataset cascades
Deleting a document dataset SHALL run the same cascade as deleting a standalone dataset (its embeddings, its memory entities, relationships of other datasets that target it) and SHALL remove its relationships from the document; a delete that would break a model-scoped metric SHALL be refused as "Explicit changes never break a model-scoped metric" specifies. Deleting a document SHALL delete each of its datasets through that cascade, plus the embeddings of the document and of its model-scoped metrics.

#### Scenario: Deleting a document dataset strips memory entities
- **WHEN** a memory links the entity `shop.orders`, no model-scoped metric reads `orders`, and the document dataset `orders` is deleted
- **THEN** the memory no longer lists `shop.orders` and the document no longer declares `orders` or its relationships

## ADDED Requirements

### Requirement: Explicit changes never break a model-scoped metric
A model-scoped metric's references are the datasets it reads, each field `d.f` it reads, each dataset metric it evaluates as `AGG(d.m)`, and each model-scoped metric it reads by name. A change breaks a stored model-scoped metric when, after it, the metric reaches, directly or through other model-scoped metrics, an unresolved reference it did not reach before. An explicit change SHALL be refused, writing nothing, when it breaks a stored model-scoped metric: saving, editing, deleting or moving a dataset to another datasource, saving a document dataset's SLayer extensions, and saving, editing or deleting a semantic-model document. The refusal SHALL list every model-scoped metric the change breaks, with its semantic model and the reference or dependency chain that breaks it, together with every stored ontology mapping the change breaks (`ontologies/ontology-documents`), in one error. A reference already unresolved before the change SHALL NOT cause a refusal. When a broken metric is hidden from the caller by access tags, the refusal SHALL be a hidden-content conflict that names no hidden content. Database-driven changes (ingest, drift cleanup, live-schema refinement) SHALL apply, warn naming each model-scoped metric they break and why, and move each broken metric written in the document's Ossie core into the document's `SLAYER` entry, so the stored document stays Ossie-valid. A model-scoped metric with an unresolved reference, or reading such a metric through other model-scoped metrics, SHALL be unloadable, with a diagnostic naming the reference or the dependency chain; every other entity SHALL load. Every surface SHALL report a refusal with every broken metric; REST SHALL answer it with 400.

#### Scenario: Deleting a dataset a metric reads is refused
- **WHEN** document `sales` declares the model-scoped metric `revenue_per_customer` reading `customers.customer_id`, and dataset `customers` is deleted
- **THEN** the delete is refused naming `revenue_per_customer` of `sales` and `customers`, and the document is unchanged

#### Scenario: Removing a field a metric reads is refused
- **WHEN** the model-scoped metric `amount_total` (`SUM(orders.amount)`) reads `orders.amount`, and a save of dataset `orders` drops field `amount`
- **THEN** the save is refused naming `amount_total` and `orders.amount`, and nothing is stored

#### Scenario: Renaming a field a metric reads is refused
- **WHEN** a save of dataset `orders` renames field `amount`, which `amount_total` reads, to `amount_usd`
- **THEN** the save is refused naming `amount_total` and `orders.amount`, and nothing is stored

#### Scenario: Removing a dataset metric a metric evaluates is refused
- **WHEN** `revenue_per_customer` reads `AGG(orders.total_revenue)`, and a save of dataset `orders` removes or renames the dataset metric `total_revenue`
- **THEN** the save is refused naming `revenue_per_customer` and `orders.total_revenue`, and nothing is stored

#### Scenario: Removing a metric another metric reads is refused
- **WHEN** the model-scoped metric `doubled` (`revenue_per_customer * 2`) reads `revenue_per_customer`, and `edit_semantic_model` removes `revenue_per_customer`
- **THEN** the edit is refused naming `doubled` and `revenue_per_customer`, and the document is unchanged

#### Scenario: A dependency chain is named
- **WHEN** `doubled` reads `revenue_per_customer`, and dataset `customers` is deleted
- **THEN** the delete is refused naming `revenue_per_customer`, and `doubled` with the chain through `revenue_per_customer` to `customers`

#### Scenario: Replacing a document's text without a read dataset is refused
- **WHEN** the text of document `sales` is replaced by one that keeps `revenue_per_customer` and drops dataset `customers`
- **THEN** the save is refused naming `revenue_per_customer`, and the stored text is unchanged

#### Scenario: Removing a SLayer-only field a metric reads is refused
- **WHEN** a model-scoped metric reads the field `orders.net`, held only in the dataset's `SLAYER` entry, and saving the dataset's SLayer extensions drops `net`
- **THEN** the save is refused naming the metric and `orders.net`, and nothing is stored

#### Scenario: One refusal lists a mapping and a metric
- **WHEN** a stored mapping maps `customers.region` and `revenue_per_customer` reads `customers`, and dataset `customers` is deleted
- **THEN** the delete is refused in one error naming both the mapping and `revenue_per_customer`

#### Scenario: A metric already unresolved blocks nothing new
- **WHEN** a stored model-scoped metric reads `orders.discount`, which `orders` does not declare, and a save of `orders` changes the description of field `status`
- **THEN** the save succeeds

#### Scenario: A further break of a partly unresolved metric is refused
- **WHEN** a stored model-scoped metric reads `orders.discount`, which `orders` does not declare, and `orders.amount`, and a save of `orders` drops field `amount`
- **THEN** the save is refused naming the metric and `orders.amount`

#### Scenario: Making a metric read an unresolved metric is refused
- **WHEN** a stored model-scoped metric `a` reads `orders.discount`, which `orders` does not declare, and `edit_semantic_model` changes the metric `b` to read `a`
- **THEN** the edit is refused naming `b` and the chain through `a` to `orders.discount`, and the document is unchanged

#### Scenario: A refused move writes nothing
- **WHEN** dataset `customers`, which `revenue_per_customer` reads, is moved to another datasource
- **THEN** the move is refused naming `revenue_per_customer`, the other datasource holds no `customers`, and `shop` still holds it

#### Scenario: A break of a hidden metric is a hidden-content conflict
- **WHEN** a caller cannot access dataset `payroll`, the model-scoped metric `cost_per_head` reads `payroll.salary` and `staff.head_count`, and the caller deletes the visible dataset `staff`
- **THEN** the delete is refused with a hidden-content conflict naming neither `cost_per_head` nor `payroll`, and nothing is deleted

#### Scenario: A drift cleanup that drops a field applies with a warning
- **WHEN** column `amount` is dropped from the database, `amount_total` (`SUM(orders.amount)`) is written in the document's Ossie core, `twice` (`amount_total * 2`) reads it, and the drift cleanup removes field `orders.amount`
- **THEN** the cleanup applies with a warning naming `amount_total` and `twice`, both metrics are held in the document's `SLAYER` entry, the stored document is Ossie-valid, and on load both are unloadable with diagnostics naming `orders.amount` and, for `twice`, the chain through `amount_total`

#### Scenario: A drift cleanup that drops a dataset applies with a warning
- **WHEN** table `customers` is dropped from the database and the drift cleanup deletes dataset `customers`
- **THEN** the cleanup applies with a warning naming `revenue_per_customer`, and on load `revenue_per_customer` is unloadable with a diagnostic naming `customers`, while `orders` loads

#### Scenario: REST reports a refused delete
- **WHEN** `DELETE /datasets/customers` would break `revenue_per_customer`
- **THEN** the response is a 400 naming `revenue_per_customer`, and `customers` is kept
