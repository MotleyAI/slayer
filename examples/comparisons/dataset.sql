-- Shared probe dataset (DuckDB). Edge cases, all deliberate:
--   Frank (customer 6) has no orders; Eve (5) has a NULL region;
--   Oslo appears in North and South; East has no customers;
--   order 20 is an orphan (NULL customer_id); no orders in 2024-04..11 or 2025-04 (month gaps).
CREATE TABLE regions (id INTEGER PRIMARY KEY, name VARCHAR);
CREATE TABLE customers (
  id INTEGER PRIMARY KEY, name VARCHAR, region_id INTEGER, city VARCHAR,
  tier VARCHAR, credit DOUBLE, discount DOUBLE
);
CREATE TABLE orders (
  id INTEGER PRIMARY KEY, customer_id INTEGER, amount DOUBLE, status VARCHAR, order_date DATE
);

INSERT INTO regions VALUES (1, 'North'), (2, 'South'), (3, 'East');

INSERT INTO customers VALUES
  (1, 'Alice', 1, 'Oslo', 'gold', 100, 1),
  (2, 'Bob', 1, 'Bergen', 'silver', 200, 2),
  (3, 'Carol', 2, 'Rome', 'gold', 300, 3),
  (4, 'Dave', 2, 'Rome', 'bronze', 400, 0),
  (5, 'Eve', NULL, 'Paris', 'silver', 500, 5),
  (6, 'Frank', 1, 'Oslo', 'gold', 600, 0),
  (7, 'Gina', 2, 'Oslo', 'bronze', 700, 1);

INSERT INTO orders VALUES
  (1, 1, 100, 'ok', DATE '2024-01-05'),
  (2, 1, 50, 'bad', DATE '2024-01-20'),
  (3, 2, 200, 'ok', DATE '2024-02-10'),
  (4, 3, 300, 'ok', DATE '2024-02-15'),
  (5, 4, 50, 'ok', DATE '2024-03-01'),
  (6, 5, 70, 'ok', DATE '2024-03-15'),
  (7, 7, 100, 'bad', DATE '2024-12-01'),
  (8, 1, 120, 'ok', DATE '2024-12-20'),
  (9, 3, 80, 'ok', DATE '2025-01-10'),
  (10, 2, 60, 'bad', DATE '2025-01-25'),
  (11, 4, 90, 'ok', DATE '2025-02-05'),
  (12, 5, 110, 'ok', DATE '2025-02-28'),
  (13, 7, 40, 'ok', DATE '2025-03-03'),
  (14, 1, 100, 'ok', DATE '2025-03-31'),
  (15, 3, 150, 'bad', DATE '2025-05-05'),
  (16, 2, 75, 'ok', DATE '2025-05-20'),
  (17, 4, 25, 'ok', DATE '2025-06-01'),
  (18, 5, 100, 'ok', DATE '2025-06-30'),
  (19, 7, 55, 'ok', DATE '2025-06-15'),
  (20, NULL, 30, 'ok', DATE '2025-06-10');

CREATE VIEW orders_flat AS
SELECT o.id, o.customer_id, o.amount, o.status, o.order_date,
       r.name AS region, c.city, c.tier, c.credit, c.discount
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
LEFT JOIN regions r ON c.region_id = r.id;
