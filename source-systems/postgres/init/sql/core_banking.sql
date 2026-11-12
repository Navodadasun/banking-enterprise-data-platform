CREATE TABLE IF NOT EXISTS accounts (
 account_id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, branch_id INT NOT NULL,
 currency CHAR(3) NOT NULL DEFAULT 'LKR', account_type TEXT NOT NULL,
 balance NUMERIC(18,2) NOT NULL CHECK(balance >= 0),
 status TEXT NOT NULL DEFAULT 'ACTIVE', created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS idx_accounts_customer ON accounts(customer_id);
