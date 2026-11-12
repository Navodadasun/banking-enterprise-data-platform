#!/usr/bin/env bash
set -euo pipefail
for db in core_banking payment_processing card_management loan_management customer_management branch_management; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -v dbname="$db" <<'SQL'
SELECT format('CREATE DATABASE %I', :'dbname') WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'dbname')
\gexec
SQL
done
for db in core_banking payment_processing card_management loan_management customer_management branch_management; do
  echo "Initializing $db"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" -f "/docker-entrypoint-initdb.d/sql/${db}.sql"
done
