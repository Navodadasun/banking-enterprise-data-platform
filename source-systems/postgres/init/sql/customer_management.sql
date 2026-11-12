CREATE TABLE IF NOT EXISTS customers (
 customer_id BIGINT PRIMARY KEY, full_name TEXT NOT NULL, email TEXT UNIQUE NOT NULL,
 phone TEXT NOT NULL, city TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS idx_customers_updated ON customers(updated_at);
