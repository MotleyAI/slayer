## ADDED Requirements

### Requirement: Stored extension columns are inlined

On load, every stored query written before this change — in a stored query-backed model's `source_queries`, a saved query or a memory's query, recursively through nested stages — whose extensions carry `columns` SHALL be converted so that no extension carries `columns`, with the result keys it returned before, except that a column reached through a join keeps its name as a stage output column while its top-level result key takes the flattened spelling. The conversion SHALL bind the query against the models of its datasource, as the engine resolves them, with the extension columns present, and SHALL replace every reference that resolves to an extension column by that column's expression, qualified by the canonical path the reference resolved through:
- a dimension SHALL become a computed dimension named after the column, and a time dimension an expression time dimension named after it, the column's join path flattened with `__` when it is reached through a join (`customers__tier_label`);
- a measure reading the column SHALL carry an explicit name equal to the name it was generated before, its join path flattened with `__`;
- filters, order items, partition and ranking keys, `main_time_dimension`, extension measures and other extension columns SHALL take the expression in place, extension columns expanding in dependency order;
- an expression is the column's `sql` read as Mode B, wrapped as `iif(<filter>, <sql>, NULL)` when the column has a `filter`, and as `CAST(<expression> AS <type>)` when it declares a type other than `TEXT` or `UNKNOWN`;
- a column without `sql` SHALL become a reference to the extended model's column of its name;
- a column's `label` and `format` SHALL carry over where the converted position holds them.

Each converted column SHALL emit one structured warning naming the document and the column, stating that its text is now read as Mode B. The converted query SHALL return the rows it returned before, except where a construct's Mode-B meaning differs from the dialect's native one (`queries/string-matching`, `queries/regex`, `queries/mixed-type-coercion`).

A stored query SHALL be unconvertible, and the document holding it SHALL fail to load with a typed error naming the document and the construct, through the stored-document load boundary, and stay unchanged in storage, when it holds:
- an inline `source_model`;
- an extension column whose text is not valid Mode B;
- an extension column of type `ARRAY`;
- an extension column without `sql` whose name is not a visible column of the extended model;
- an extension column without `sql` that has a `filter`;
- an extension column with `sql` named like a column of the extended model;
- a dependency cycle among extension columns;
- an extension join whose `join_pairs` names an extension column on either side;
- a reference to an extension column in a position that holds no expression, or one the binder cannot attribute.

A converted query-backed model or saved query SHALL be written back at the current version; a memory SHALL be converted on every load and SHALL NOT be written back.

#### Scenario: A dimension keeps its result key
- **WHEN** a stored query-backed model's stage rooted at `orders` extends `customers` with the column `tier_label` (`sql` `upper(tier)`) and selects the dimension `customers.tier_label` and `sum(amount)`
- **THEN** it loads with the computed dimension `upper(customers.tier)` named `customers__tier_label`, no extension columns, one warning naming the model and `tier_label`, and returns the rows recorded before under the stage column `customers__tier_label`

#### Scenario: A measure keeps its generated name
- **WHEN** a stored stage extends `orders` with `double_amount` (`sql` `amount * 2`, type `DOUBLE`) and selects `sum(double_amount)`
- **THEN** it loads as `sum(CAST(amount * 2 AS DOUBLE))` named `double_amount_sum` and returns the recorded value

#### Scenario: A reference through a named edge is qualified by the edge
- **WHEN** a stored stage rooted at `orders` extends `addresses` with `city_label` and selects `billing_address.city_label`
- **THEN** the expression is qualified as `billing_address.<…>` and returns the recorded rows

#### Scenario: Filtered, chained and sql-less columns
- **WHEN** a stored stage's extension has `q_amount` (`sql` `amount`, `filter` `status = 'ok'`), `q_double` (`sql` `q_amount * 2`) and a sql-less `status`, each used in a filter, an order item and a measure
- **THEN** each use takes `iif(status = 'ok', amount, NULL)`, `iif(status = 'ok', amount, NULL) * 2` and `status` respectively, and returns the recorded rows

#### Scenario: Unconvertible documents fail to load and stay untouched
- **WHEN** stored documents hold, in turn, an inline `source_model`, a column `sql` `strftime('%Y', created_at)`, a sql-less column naming a column the model does not declare, a join keyed on an extension column, and two columns referencing each other
- **THEN** each fails to load with the typed error naming the document and the construct, every other document of the store loads, and the stored bytes are unchanged

#### Scenario: A memory converts on every load
- **WHEN** a stored memory's query carries an extension column and the memory is loaded twice
- **THEN** both loads return the converted query with the warning, and the stored memory is unchanged

#### Scenario: Released stores still open with their results
- **WHEN** the committed 0.10.2 and 1.8.2 stores are opened
- **THEN** every model and memory loads and every recorded query returns the rows recorded from its release

## MODIFIED Requirements

### Requirement: Stored root extensions move to query extensions

On load, every stored query written before this change whose `source_model` is an extension object SHALL become a query with `source_model` naming the extension's base model and that extension as its one `extensions` entry. This SHALL apply recursively through stored `source_queries`, saved queries and memories' queries, including a query nested in a document whose own version is current.
- The upgrade SHALL emit no deprecation warning and SHALL lose no measure or join; the extension's columns SHALL then be inlined as "Stored extension columns are inlined" specifies.
- Repeated loads SHALL change nothing further.
- The upgraded query SHALL return the rows it returned before.
- A save SHALL write the upgraded form.

#### Scenario: A stored query-backed model's stage
- **WHEN** a stored query-backed model has a stage with `source_model` = `{"source_name": "orders", "joins": [...]}`
- **THEN** it loads with `source_model` `orders` and one extension of `orders` carrying those joins, without a deprecation warning, and returns the rows recorded before the upgrade

#### Scenario: A memory's query in a current-version memory
- **WHEN** a memory at the current memory version holds a query marked with a version before this change whose `source_model` is an extension object
- **THEN** the query loads in the upgraded form, without a deprecation warning, with its extension intact

#### Scenario: An unversioned stored memory query
- **WHEN** a stored memory without a version holds a query whose `source_model` is an extension object
- **THEN** it loads in the upgraded form, without a deprecation warning

#### Scenario: Upgrade is idempotent
- **WHEN** an upgraded document is saved and loaded again
- **THEN** it is unchanged

### Requirement: Stored Mode-B text is upgraded to the SQL string and identifier rules

On load, every Mode-B surface of a stored model, saved query or memory written before this change — `ModelMeasure.formula`, and a stored query's measures, dimensions, computed-dimension expressions, time-dimension columns, order columns and expressions, `main_time_dimension` and filters, recursively through stored `source_queries`, saved queries and extension measures — SHALL be rewritten so that it means under the current grammar what it meant when stored: a double-quoted string literal becomes the equivalent single-quoted literal, a string literal using backslash escapes is re-encoded with its decoded value, a `|` concatenation becomes `||`, two minus signs written with nothing between them are separated by a space, so a double negation never becomes a comment, and a Python `#` comment becomes a `--` comment. The rewrite SHALL be token-exact: text inside string literals is never otherwise touched, and stored text that cannot be tokenised is left unchanged. Stored text that the grammar of the time it claims to be from could not have held SHALL be left unchanged, with a warning naming the document and the field. Mode-A surfaces SHALL NOT be rewritten, and a client query without a version SHALL NOT be rewritten.

#### Scenario: Double-quoted strings in a stored query

- **WHEN** a stored query written before this change filters `status == "paid"` and has measure `sum(iif(region == "EU", amount, 0))`
- **THEN** it loads as `status == 'paid'` and `sum(iif(region == 'EU', amount, 0))` and returns the rows it returned before

#### Scenario: Escapes and embedded quotes

- **WHEN** a stored filter is `name == "O'Brien"`, another `name == 'O\'Brien'`, and a stored measure formula compares with `"tab\there"`
- **THEN** they load as `name == 'O''Brien'` (twice) and a single-quoted literal containing a real tab character, and match the same rows as before

#### Scenario: Pipe concatenation

- **WHEN** a stored computed dimension is `status | region`
- **THEN** it loads as `status || region` and returns the same values

#### Scenario: Adjacent minus signs

- **WHEN** a stored measure formula is `sum(amount) --sum(refund)`, another `sum(a--b)`, and a third `sum(a- -b)`
- **THEN** they load as `sum(amount) - -sum(refund)`, `sum(a- -b)` and `sum(a- -b)` unchanged, and return the values they returned before

#### Scenario: Mode-A text untouched

- **WHEN** a stored model has `Column.sql` `"Order Total" * 2` and a model filter `status = "x"`
- **THEN** both load byte-for-byte unchanged

#### Scenario: Client queries are not rewritten

- **WHEN** a client sends a query without a version that filters `status = "paid"`
- **THEN** it is not rewritten and fails with the double-quoted-name hint unless a column named `paid` exists

#### Scenario: Released stores still open with their results

- **WHEN** the committed 0.10.2 store is opened
- **THEN** every model and memory loads and every recorded query returns the rows recorded from 0.10.2

#### Scenario: Text already in the current grammar is left alone

- **WHEN** a stored model still marked with a version before this rewrite has the measures `sum(x::int)`, `sum(a || b)`, `sum(CAST(x AS INT))` and `sum(x) FILTER (WHERE y > 1)`, and a stored query of the same era filters `x BETWEEN 1 AND 2`
- **THEN** each loads byte-for-byte unchanged and a warning names the document and each such field

### Requirement: Stored colon spellings are upgraded to the functional spelling

On load, every Mode-B surface of a stored model, saved query or memory written before this change — `ModelMeasure.formula`, and a stored query's measures, dimensions, computed-dimension expressions, time-dimension columns, order columns and expressions, `main_time_dimension` and filters, recursively through stored `source_queries`, saved queries and extension measures — SHALL have every colon span rewritten to its twin: `date_trunc('<name>', <source>)` when `<name>` is a built-in granularity, and `<name>(<source>[, <args>])` otherwise, with the source and arguments as written and colon spans inside the arguments rewritten too. Everything outside a colon span SHALL stay byte-identical, and a colon inside a string literal, a quoted identifier or a `::` cast SHALL be untouched. A stored order item whose expression is rewritten SHALL load exactly as the same order key written fresh in the functional spelling. The document SHALL return the rows and result keys it returned before; a persisted computed-dimension name SHALL be kept and keep its meaning. A datasource granularity rename SHALL apply before the rewrite. Stored text that cannot otherwise be parsed SHALL be left unchanged, with a warning naming the document and the field. Mode-A surfaces SHALL NOT be rewritten, and a client query without a version SHALL NOT be rewritten.

#### Scenario: Aggregations, stars and parameters

- **WHEN** a stored model has the measures `amount:sum`, `*:count`, `customers.*:count`, `customers.regions.population:sum`, `price:percentile(p=0.9)` and `balance:last(updated_at)`
- **THEN** they load as `sum(amount)`, `count(*)`, `count(customers.*)`, `sum(customers.regions.population)`, `percentile(price, p=0.9)` and `last(balance, updated_at)` and return the values they returned before

#### Scenario: Colon spans inside larger text

- **WHEN** a stored query has the measure `rank(rev:sum)`, the filter `amount:sum > 100 AND note = 'a:sum'` and the measure `sum(amount::INT) / *:count`
- **THEN** they load as `rank(sum(rev))`, `sum(amount) > 100 AND note = 'a:sum'` and `sum(amount::INT) / count(*)`, every other byte unchanged

#### Scenario: Order items

- **WHEN** a stored query orders by the item persisted as `{column: {name: amount_sum}, raw_formula: "amount:sum"}` descending, and by `*:count`
- **THEN** it loads exactly as a fresh query ordering by `sum(amount)` descending and by `count(*)`, and returns the rows in the order it returned them before

#### Scenario: Granularity spans

- **WHEN** a stored query orders by `created_at:month` and a stored computed dimension buckets `created_at:fiscal_quarter` on a datasource defining `fiscal_quarter`
- **THEN** they load as `date_trunc('month', created_at)` and `date_trunc('fiscal_quarter', created_at)` and return the rows they returned before

#### Scenario: A granularity renamed on load

- **WHEN** a stored datasource defines the granularity `left`, and a stored model's source query and a memory's query bucket by `created_at:left`
- **THEN** after load they read `date_trunc('left_granularity', created_at)` and return the rows they returned before

#### Scenario: Result keys and computed-dimension names are kept

- **WHEN** a stored query has the unnamed measures `amount:sum / *:count` and a measure whose derived key is long enough to carry a hash, and a computed dimension persisted with its auto-name
- **THEN** after load the result keys are the ones returned before, and the computed dimension keeps its name and is still treated as auto-named

#### Scenario: Memories

- **WHEN** a memory stored as YAML frontmatter or as a SQLite row bundles a query measuring `amount:sum`, and it is loaded twice
- **THEN** both loads give `sum(amount)` and the query's rows

#### Scenario: The released corpus returns its results

- **WHEN** the committed store written by `motley-slayer==0.10.2` is opened
- **THEN** every colon formula in it loads in the functional spelling and every recorded query returns the rows and result keys recorded from 0.10.2

#### Scenario: Client queries are refused, not rewritten

- **WHEN** a client sends a query without a version measuring `amount:sum`
- **THEN** it is not rewritten and fails with the colon-spelling error

### Requirement: Stored bucket calls are upgraded to date_trunc
On load, a stored model, saved query or memory written before this change SHALL have every Mode-B call `<g>(x)` whose callee names a built-in granularity, or a custom granularity of the datasource it belongs to, rewritten to `date_trunc('<g>', x)` — in query measures, filters, dimensions, time dimensions, order keys and `main_time_dimension`, in saved-measure formulas, and in every nested saved query, model extension and query-backed model — with the rest of the text byte-identical. Mode-A text (`Column.sql`, `sql_table`, model `filters`, custom-aggregation formulas and parameter defaults), structured time dimensions, transform units and `Column.granularity` SHALL be untouched. A call whose callee is a custom aggregation of the owning model and not a granularity SHALL be untouched. Every upgraded query SHALL return the rows and result keys it returned before. The custom-granularity rewrite SHALL use the datasource's settled granularity names (after any library-collision rename) and SHALL complete on the next load when a load is interrupted.

#### Scenario: Saved query bucketing by month
- **WHEN** a stored model's saved query has the dimension `month(created_at)`, the filter `year(created_at) >= '2024'` and the order key `month(created_at)`
- **THEN** after load they read `date_trunc('month', created_at)`, `date_trunc('year', created_at) >= '2024'` and `date_trunc('month', created_at)`, and the query returns the rows and result keys it returned before

#### Scenario: Custom granularity in a memory
- **WHEN** a stored datasource defines `fiscal_year` and a stored memory's query buckets `fiscal_year(order_date)`
- **THEN** after load the memory's query reads `date_trunc('fiscal_year', order_date)` and returns its earlier rows

#### Scenario: Custom aggregation is not a bucket
- **WHEN** a stored model defines the custom aggregation `fiscal_total` and a saved measure `fiscal_total(amount)`
- **THEN** the measure loads unchanged

#### Scenario: Mode-A text is untouched
- **WHEN** a stored column's `sql` is `month(created_at)` and a model filter is `year(created_at) > 2020`
- **THEN** both load unchanged
