-- Counts for each published Silver and Gold table.
SELECT 'silver.customers' AS table_name, COUNT(*) AS row_count FROM nessie.silver.customers
UNION ALL SELECT 'silver.accounts', COUNT(*) FROM nessie.silver.accounts
UNION ALL SELECT 'silver.transactions', COUNT(*) FROM nessie.silver.transactions
UNION ALL SELECT 'silver.cards', COUNT(*) FROM nessie.silver.cards
UNION ALL SELECT 'silver.loans', COUNT(*) FROM nessie.silver.loans
UNION ALL SELECT 'silver.branches', COUNT(*) FROM nessie.silver.branches
UNION ALL SELECT 'gold.daily_transactions', COUNT(*) FROM nessie.gold.daily_transactions
UNION ALL SELECT 'gold.branch_performance', COUNT(*) FROM nessie.gold.branch_performance
UNION ALL SELECT 'gold.customer_accounts', COUNT(*) FROM nessie.gold.customer_accounts
UNION ALL SELECT 'gold.loan_portfolio', COUNT(*) FROM nessie.gold.loan_portfolio
UNION ALL SELECT 'gold.monthly_kpis', COUNT(*) FROM nessie.gold.monthly_kpis;

-- Iceberg snapshot metadata proves the table has committed files and history.
SELECT committed_at, snapshot_id, operation, summary
FROM nessie.gold.daily_transactions.snapshots
ORDER BY committed_at DESC
LIMIT 1;

-- Sample banking analytics query.
SELECT transaction_date, branch_name, transaction_type,
       transaction_count, total_amount, net_amount
FROM nessie.gold.daily_transactions
ORDER BY transaction_date DESC, transaction_count DESC
LIMIT 10;

-- If this returns rows, those source records were quarantined rather than published.
SELECT source, source_table, errors, COUNT(*) AS record_count
FROM nessie.silver.quarantine_records
GROUP BY source, source_table, errors
ORDER BY record_count DESC;
