CREATE TABLE IF NOT EXISTS loans (
 loan_id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL,
 principal NUMERIC(18,2) NOT NULL CHECK(principal > 0),
 outstanding NUMERIC(18,2) NOT NULL CHECK(outstanding >= 0),
 interest_rate NUMERIC(5,2) NOT NULL, status TEXT NOT NULL DEFAULT 'ACTIVE',
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
