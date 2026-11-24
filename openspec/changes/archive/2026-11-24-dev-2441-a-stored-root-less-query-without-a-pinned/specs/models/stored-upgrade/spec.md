## ADDED Requirements

### Requirement: A stored memory whose population names no stored dataset reads one datasource

When a stored memory's query is pinned to no datasource and either names no population or names a population spelling the time spine that no stored dataset spells exactly, its datasource SHALL be the one datasource its dataset-level entities (`<datasource>.<dataset>` or `<datasource>.<dataset>.<leaf>`, naming a stored dataset) name, else the only datasource holding a stored dataset. Every load-time upgrade of it — its qualified-reference upgrade, the typing of its literals, the rewrite of its custom-granularity buckets, and the rename of a library-colliding granularity it references — SHALL read that datasource; a root-less memory SHALL infer its population there as a pinned query does and SHALL fail to load when no single dataset there reaches what it reads; no other datasource is tried. With no such datasource, its custom-granularity buckets SHALL NOT be rewritten, no granularity it references SHALL be renamed, and, when its qualified-reference upgrade is pending, it SHALL fail to load with a warning naming it and saying that its datasource cannot be determined. A stored memory whose explicit population names no stored dataset in any other spelling SHALL NOT use this datasource and SHALL fail to load naming the population. A memory's conversion SHALL run on each load and never be written back; only the rename of a library-colliding granularity SHALL be persisted, as for every stored document.

#### Scenario: A root-less memory loads on a single-datasource store

- **WHEN** the one datasource holds `orders` and `customers`, and a stored v4 memory query with no entities has the dimension `status`, the measure `sum(amount)` named `rev` and the filter `status = 'ok'`, with no population
- **THEN** it loads as `orders.status`, `sum(orders.amount)` and `orders.status = 'ok'`, returns its earlier rows (`ok`: 80), and the stored memory is unchanged

#### Scenario: Its entities pin a root-less memory's datasource over the priority

- **WHEN** datasources `shop` and `shop2` both hold `orders` and `customers`, `status` and `amount` are fields of `orders` in `shop` and of `customers` in `shop2`, `shop` is first in the priority, and the root-less memory above is stored with the entities `shop2.customers`, `shop2.customers.status` and `shop2.customers.amount`
- **THEN** it loads as `customers.status` and `sum(customers.amount)` and returns its rows when run in `shop2`

#### Scenario: A root-less memory's legacy literal is repaired

- **WHEN** the root-less memory filters `ordered_at >= '2024/01/01'` on a single-datasource store
- **THEN** it loads as `orders.ordered_at >= '2024-01-01'` and returns its earlier total (200)

#### Scenario: A root-less memory's literal is typed in the datasource it is read in

- **WHEN** datasources `shop` and `shop2` both hold `orders`, `ordered_at` is a TIMESTAMP in `shop` and TEXT in `shop2`, and the root-less memory filters `ordered_at >= '2024/01/01'`
- **THEN** with the entity `shop.orders` the literal is repaired to `'2024-01-01'`; with the entity `shop2.orders` it is left unchanged

#### Scenario: An entity-pinned memory is read only in its datasource

- **WHEN** the root-less memory reads `status`, its entity `shop.customers` names `shop`, which holds no dataset reaching `status`, and `shop2` holds one
- **THEN** the memory fails to load with a warning naming it and `status`

#### Scenario: An entity-less memory on a multi-datasource store fails to load

- **WHEN** datasources `shop` and `shop2` both hold `orders` and `customers`, and the root-less memory is stored with no entities, or with entities naming no stored dataset or naming both datasources
- **THEN** the memory fails to load with a warning naming it and saying that its datasource cannot be determined

#### Scenario: A root-less memory's custom-granularity bucket is rewritten

- **WHEN** the datasource defines the granularity `mid_month`, and a stored pre-v9 root-less memory buckets `created_at` by `mid_month` and filters `created_at:mid_month >= '2025-02-01'`
- **THEN** it loads with the filter `date_trunc('mid_month', events.created_at) >= '2025-02-01'` and returns its earlier rows

#### Scenario: A root-less memory's library-colliding granularity is renamed

- **WHEN** the datasource defines the granularity `left` and a stored root-less memory buckets `created_at` by `left`
- **THEN** it loads bucketing by `left_granularity` and returns its earlier rows

#### Scenario: An entity-pinned memory's custom granularities follow its datasource

- **WHEN** datasources `g1` and `g2` both hold `events`, only `g2` defines the granularities `left` and `mid_month`, and a stored pre-v9 root-less memory with the entity `g2.events` buckets `created_at` by `left` and filters `created_at:mid_month >= '2025-02-01'`
- **THEN** it loads bucketing by `left_granularity` with the filter `date_trunc('mid_month', events.created_at) >= '2025-02-01'`, and returns its earlier rows when run in `g2`

#### Scenario: A time-spine memory's custom granularities are upgraded

- **WHEN** a stored memory with population `time_spine` buckets `time_spine.timestamp` by the datasource's granularity `left`, or filters `events.created_at:mid_month >= '2025-02-01'` with `mid_month` defined
- **THEN** it loads bucketing by `left_granularity`, or with the filter `date_trunc('mid_month', events.created_at) >= '2025-02-01'`, and returns its earlier rows

#### Scenario: A missing explicit population gets no fallback

- **WHEN** a stored memory names the population `ordrs`, which no datasource stores, and buckets by the datasource's granularity `left`
- **THEN** no datasource is read for it, its granularity is not renamed, and it fails to load naming `ordrs`
