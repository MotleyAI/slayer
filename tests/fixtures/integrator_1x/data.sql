CREATE TABLE customers (id INTEGER, email TEXT, country TEXT);
INSERT INTO customers VALUES
  (1, 'ann@x.com', 'DE'),
  (2, 'bob@y.com', 'FR'),
  (3, 'cyd@x.com', 'US');
CREATE TABLE orders (
  id INTEGER, customer_id INTEGER, status TEXT, amount DOUBLE PRECISION, tags TEXT[], created_at TIMESTAMP
);
INSERT INTO orders VALUES
  (1, 1, 'paid', 10.0, ARRAY['dbt', 'sql'], TIMESTAMP '2024-01-03 10:15:30'),
  (2, 1, 'open', 20.0, ARRAY['dbt'], TIMESTAMP '2024-01-20 08:00:00'),
  (3, 2, 'paid', 30.0, ARRAY['sql'], TIMESTAMP '2024-02-11 23:59:59'),
  (4, 2, 'paid', 5.0, ARRAY['dbt', 'py'], TIMESTAMP '2024-04-01 00:00:00'),
  (5, 3, 'open', 40.0, ARRAY['py'], TIMESTAMP '2024-07-14 12:30:00'),
  (6, 3, 'cancelled', 100.0, ARRAY['dbt'], TIMESTAMP '2024-07-15 09:00:00');
