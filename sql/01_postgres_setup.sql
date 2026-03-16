-- POSTGRES 3NF SETUP FOR OLTP — ecommerce database
-- 1. Customers table — one row per customer
CREATE TABLE IF NOT EXISTS customers (
    customer_id   VARCHAR(20)  PRIMARY KEY,
    country       VARCHAR(100) NOT NULL
);

-- 2. Products table — one row per product
CREATE TABLE IF NOT EXISTS products (
    stock_code    VARCHAR(20)  PRIMARY KEY,
    description   VARCHAR(255),
    unit_price    NUMERIC(10,2)
);

-- 3. Orders table — one row per invoice
CREATE TABLE IF NOT EXISTS orders (
    invoice_no    VARCHAR(20)  PRIMARY KEY,
    customer_id   VARCHAR(20)  NOT NULL,
    invoice_date  TIMESTAMP    NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) --resolving 1-M relationship between customer and orders table
);

-- 4. Order items table — one row per invoice line item
CREATE TABLE IF NOT EXISTS order_items (
    id            SERIAL       PRIMARY KEY,  -- surrogate key, resolving M-N relationship between orders and products
    invoice_no    VARCHAR(20)  NOT NULL,
    stock_code    VARCHAR(20)  NOT NULL,
    quantity      INTEGER      NOT NULL,
    unit_price    NUMERIC(10,2),
    total_price   NUMERIC(10,2),
    FOREIGN KEY (invoice_no) REFERENCES orders(invoice_no),
    FOREIGN KEY (stock_code) REFERENCES products(stock_code),
    CONSTRAINT uq_order_items UNIQUE (invoice_no, stock_code) -- to prevent duplicate line items for same product in same invoice
);