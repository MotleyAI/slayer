## ADDED Requirements

### Requirement: Mode-A surfaces bind variables as literals

A `{var}` placeholder in a model's Mode-A text — `Column.sql`, `Column.filter`, model `sql` and model `filters` — SHALL be bound as a literal node of the parsed SQL, never spliced as text, by the rules Mode-B surfaces follow:
- inside a string literal, wholly or partly, it contributes its value's text to that literal;
- in a bare literal position it takes a number or a boolean, a numeric string reading as its number;
- forming the whole parenthesised list of `IN` it takes a list value.

A placeholder in any other position — an identifier, a quoted identifier, a function or type name, a comment, part of an `IN` list — and a string value bound into a bare literal position SHALL be rejected with the error stating that variables supply literal values. No variable value, whether supplied by a query, a stage or a model default, SHALL change the structure of the Mode-A text it is bound into. A `{? … ?}` optional block SHALL keep its template meaning: it is kept or dropped as a whole by the presence of its variables, and the placeholders it keeps then bind as literals. The position check needs the datasource's dialect: it SHALL run when a model is saved, so a model whose Mode-A text holds a placeholder in a non-literal position is refused at save, and again when a query binds the model.

#### Scenario: A hostile value is refused
- **WHEN** a stored model has the column `scaled` with `sql` `amount * {k}` and the filter `amount >= {floor}`, and a query passes `k = "(SELECT count(*) FROM users)"`, or `floor = "0 AND EXISTS (SELECT 1 FROM users WHERE id = 1)"`
- **THEN** each query fails with the literal-values error and no SQL reading `users` executes

#### Scenario: Literal values bind
- **WHEN** the same model is queried with `k = 2` and `floor = "10"`, and a model filter `status IN ({statuses})` with `statuses = ["ok", "new"]`, and a column `sql` `name || '{suffix}'` with `suffix = "O'Brien"`
- **THEN** each executes with the hand-computed values, the string literal's value being exactly `O'Brien`

#### Scenario: A placeholder in a non-literal position is refused at save
- **WHEN** a model is saved whose `sql` is `SELECT * FROM {table}`, or whose column `sql` is `{col} * 2`, `{f}(amount)` or `"{c}"`
- **THEN** the save fails with the literal-values error naming the field

#### Scenario: Optional blocks keep their meaning
- **WHEN** a model filter is `{? created_at >= '{from}' ?}`, and one query supplies `from` and another does not
- **THEN** the first filters on the literal date and the second drops the block

#### Scenario: Cube imports still execute
- **WHEN** the committed Cube import fixtures with `FILTER_PARAMS` pushdown blocks are imported and queried with and without their variables
- **THEN** they return the values they returned before
