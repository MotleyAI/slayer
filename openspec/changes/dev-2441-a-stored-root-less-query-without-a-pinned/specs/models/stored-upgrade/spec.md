## ADDED Requirements

### Requirement: A stored query whose population names no stored dataset reads one datasource

When a stored query is pinned to no datasource and either names no population or names a population spelling the time spine that no stored dataset spells exactly, every load-time upgrade of it — its qualified-reference upgrade, the typing of its literals, the rewrite of its custom-granularity buckets, and the rename of a library-colliding granularity it references — SHALL read one datasource: among the datasources holding a dataset that its references or its extensions name (an extended dataset or a join target), else among every datasource, the first in the datasource priority, else the first by name. A root-less query SHALL infer its population in that datasource as a pinned one does, and SHALL fail to load only when no single dataset there reaches what it reads; no other datasource is tried. A stored query whose explicit population names no stored dataset in any other spelling SHALL NOT use this datasource and SHALL fail to load naming the population. Memories SHALL convert on each load, never rewritten in the store.

#### Scenario: A root-less memory loads on a single-datasource store

- **WHEN** the one datasource holds `orders` and `customers`, and a stored v4 memory query has the dimension `status`, the measure `sum(amount)` named `rev` and the filter `status = 'ok'`, with no population
- **THEN** it loads as `orders.status`, `sum(orders.amount)` and `orders.status = 'ok'`, returns its earlier rows (`ok`: 80), and the stored memory is unchanged

#### Scenario: The datasource priority picks where a root-less memory is read

- **WHEN** datasources `shop` and `shop2` both hold `orders` and `customers`, `status` and `amount` are fields of `orders` in `shop` and of `customers` in `shop2`, and the root-less memory above is stored
- **THEN** with `shop` first in the priority it loads as `orders.status` and `sum(orders.amount)`, with `shop2` first as `customers.status` and `sum(customers.amount)`, and each returns its rows when run in that datasource

#### Scenario: A root-less memory's legacy literal is repaired

- **WHEN** the root-less memory filters `ordered_at >= '2024/01/01'` on a single-datasource store
- **THEN** it loads as `orders.ordered_at >= '2024-01-01'` and returns its earlier total (200)

#### Scenario: A root-less memory's literal is typed in the datasource it is read in

- **WHEN** datasources `shop` and `shop2` both hold `orders`, `ordered_at` is a TIMESTAMP in `shop` and TEXT in `shop2`, and the root-less memory filters `ordered_at >= '2024/01/01'`
- **THEN** with `shop` first the literal is repaired to `'2024-01-01'`; with `shop2` first it is left unchanged

#### Scenario: A named dataset narrows the datasources

- **WHEN** datasources `a_shop` and `b_shop` both hold `orders`, only `b_shop` holds `customers`, no priority is set, and a root-less memory filters `customers.region = 'North'`
- **THEN** it is read in `b_shop`

#### Scenario: An extension's join target narrows the datasources

- **WHEN** datasources `a_shop` and `b_shop` both hold `orders`, only `b_shop` holds `customers`, no priority is set, and a root-less memory extends `orders` with a join to `customers` and reads only bare names
- **THEN** it is read in `b_shop`

#### Scenario: With no priority entry the first datasource by name is read

- **WHEN** datasources `a_shop` and `b_shop` both hold `orders`, neither is in the priority, and the root-less memory names no dataset
- **THEN** it is read in `a_shop`

#### Scenario: No other datasource is tried

- **WHEN** the first datasource in the priority holds no dataset reaching `status`, another datasource does, and the root-less memory reads `status`
- **THEN** the memory fails to load with a warning naming it and `status`

#### Scenario: A root-less memory's custom-granularity bucket is rewritten

- **WHEN** the datasource defines the granularity `mid_month`, and a stored pre-v9 root-less memory buckets `created_at` by `mid_month` and filters `created_at:mid_month >= '2025-02-01'`
- **THEN** it loads with the filter `date_trunc('mid_month', events.created_at) >= '2025-02-01'` and returns its earlier rows

#### Scenario: A root-less memory's library-colliding granularity is renamed

- **WHEN** the datasource defines the granularity `left` and a stored root-less memory buckets `created_at` by `left`
- **THEN** it loads bucketing by `left_granularity` and returns its earlier rows

#### Scenario: A time-spine memory's custom granularities are upgraded

- **WHEN** a stored memory with population `time_spine` buckets `time_spine.timestamp` by the datasource's granularity `left`, or filters `events.created_at:mid_month >= '2025-02-01'` with `mid_month` defined
- **THEN** it loads bucketing by `left_granularity`, or with the filter `date_trunc('mid_month', events.created_at) >= '2025-02-01'`, and returns its earlier rows

#### Scenario: A missing explicit population gets no fallback

- **WHEN** a stored memory names the population `ordrs`, which no datasource stores, and buckets by the datasource's granularity `left`
- **THEN** no datasource is read for it, its granularity is not renamed, and it fails to load naming `ordrs`
