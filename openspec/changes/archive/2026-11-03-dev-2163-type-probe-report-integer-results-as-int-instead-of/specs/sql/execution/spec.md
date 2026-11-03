## ADDED Requirements

### Requirement: An async-only driver URL still gets a synchronous engine

For a datasource type with a native asynchronous driver (postgres/postgresql: asyncpg; mysql/mariadb: aiomysql), a connection URL naming that asynchronous-only driver SHALL get its synchronous connection on the type's default synchronous driver (postgres: psycopg2; mysql/mariadb: pymysql), preserving the backend and every other URL part, so metadata reads and column-type probes work. Statements SHALL still run on the asynchronous driver the URL names.

#### Scenario: asyncpg URL
- **WHEN** a postgres datasource's URL is `postgresql+asyncpg://u:p@h:5432/d?sslmode=require` and its client pings, reflects a table and probes a statement's column types
- **THEN** each succeeds over `postgresql+psycopg2://u:p@h:5432/d?sslmode=require`, and query statements run on asyncpg

#### Scenario: aiomysql URL
- **WHEN** a mysql datasource's URL is `mysql+aiomysql://u@h/d`
- **THEN** its synchronous connection URL is `mysql+pymysql://u@h/d`
