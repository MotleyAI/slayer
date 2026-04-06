-- Jaffle Shop DuckDB schema
-- Tables ordered: independent first, then dependent (respecting FK constraints)

CREATE TABLE customers (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE stores (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    opened_at DATE NOT NULL,
    tax_rate DOUBLE NOT NULL
);

CREATE TABLE products (
    sku VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    price DOUBLE NOT NULL,
    description VARCHAR NOT NULL
);

CREATE TABLE orders (
    id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL REFERENCES customers(id),
    ordered_at DATE NOT NULL,
    store_id VARCHAR NOT NULL REFERENCES stores(id),
    subtotal DOUBLE NOT NULL,
    tax_paid DOUBLE NOT NULL,
    order_total DOUBLE NOT NULL
);

CREATE TABLE order_items (
    id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL REFERENCES orders(id),
    sku VARCHAR NOT NULL REFERENCES products(sku),
    quantity INTEGER NOT NULL
);

CREATE TABLE supplies (
    id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    cost DOUBLE NOT NULL,
    perishable VARCHAR NOT NULL,
    sku VARCHAR NOT NULL REFERENCES products(sku),
    PRIMARY KEY (id, sku)
);

CREATE TABLE tweets (
    id VARCHAR PRIMARY KEY,
    user_id VARCHAR NOT NULL REFERENCES customers(id),
    tweeted_at DATE NOT NULL,
    content VARCHAR NOT NULL
);
