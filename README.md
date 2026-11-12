# Banking Enterprise Data Platform (BEDP)

A local-first, cloud-ready **synthetic** banking source system. Initial deployment: one PostgreSQL container, six logical databases, and a Python activity simulator. **Not production banking software.**

## Requirements
- Windows 11, Docker Desktop with WSL 2, Python 3.11+
- 16 GB RAM and SSD recommended

## Quick start (PowerShell from project root)

```powershell
Copy-Item .env.example .env
notepad .env
```

Set `POSTGRES_PASSWORD` to a strong local password. The Compose file reads it from the root `.env` (Docker Compose loads the project directory .env when `--project-directory .` is provided). Run:

```powershell
docker compose --project-directory . -f infrastructure/docker/compose.yaml up -d
docker compose --project-directory . -f infrastructure/docker/compose.yaml ps
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r applications/banking-simulator/requirements.txt
.\.venv\Scripts\python.exe applications/banking-simulator/main.py seed --customers 100
.\.venv\Scripts\python.exe applications/banking-simulator/main.py run --interval 3 --count 20
.\scripts\verify.ps1
```

**Note:** Initial database scripts execute only when the PostgreSQL volume is empty. `docker compose down` retains data; `down -v` destroys it. No real banking or card information should be used.

## Inspect data

```powershell
docker compose --project-directory . -f infrastructure/docker/compose.yaml exec postgres psql -U bedp_admin -d customer_management -c "SELECT COUNT(*) FROM customers;"
docker compose --project-directory . -f infrastructure/docker/compose.yaml exec postgres psql -U bedp_admin -d payment_processing -c "SELECT * FROM transactions ORDER BY transaction_id DESC LIMIT 5;"
docker compose --project-directory . -f infrastructure/docker/compose.yaml exec postgres psql -U bedp_admin -d postgres -c "SHOW wal_level;"
```

## Batch ingestion and Bronze storage

The batch-ingestion extension is integrated at the project root. It extracts the six PostgreSQL source databases, writes compressed Parquet files and JSON manifests to the `banking-lakehouse` SeaweedFS S3 bucket, and maintains a local SQLite checkpoint for incremental transactions.

Start PostgreSQL first so its `banking-platform-net` network exists, then run these commands from the project root:

```powershell
docker compose -f infrastructure/docker/compose.storage.yaml config
docker compose -f infrastructure/docker/compose.storage.yaml up -d
docker compose -f infrastructure/docker/compose.storage.yaml ps
.\.venv\Scripts\python.exe -m pip install -r data-ingestion/batch/requirements.txt
.\.venv\Scripts\python.exe data-ingestion/batch/ingest.py --source all --chunk-size 5000
.\.venv\Scripts\python.exe data-ingestion/batch/verify_lake.py
Get-Content runtime\batch_runs.jsonl -Tail 6
```

SeaweedFS uses persistent Docker volume `banking-enterprise-data-platform_seaweedfs_data`, binds its master UI to http://127.0.0.1:9333 and its S3 API to http://127.0.0.1:8333, and is limited to 1 GiB. Keep the existing PostgreSQL volume; do not use `docker compose down -v` or remove volumes to troubleshoot.

The client and SeaweedFS S3 identity use `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, and `LAKE_BUCKET` from `.env`; local-development defaults are shown in `.env.example`. Replace the default access and secret keys in `.env` before sharing the host. Ports are localhost-only, but these settings, the synthetic data, and this deployment are not production-secure.

`runtime\ingestion_state.sqlite3` stores the transaction high-water checkpoint and `runtime\batch_runs.jsonl` stores status, batch IDs, timestamps, row counts, and manifests. Both are ignored by Git and must be preserved for incremental continuity. Snapshot sources are re-ingested on each run; transactions are incremental by `transaction_id`. Run the ingestion command twice to confirm that the second transaction manifest reports zero new rows. The verifier checks the Parquet objects against the manifests and current PostgreSQL counts and detects duplicate transaction IDs across recorded batches.

### Integration verification (November 12, 2026)

- The add-on had been extracted under `bedp-batch-ingestion-addon\`; the project-root Compose and ingestion paths did not exist. The destinations were checked for conflicts, then the Compose file, ingestion code, and batch documentation were moved to the root layout.
- The initial image pull failed once with Docker reporting a DNS lookup failure for `production.cloudfront.docker.com`. On diagnosis, Windows resolved both that host and `registry-1.docker.io`, WSL 2 resolved the Docker Registry and reached it over HTTPS, and Docker Desktop reported its built-in proxy (`http.docker.internal:3128`). A later `docker pull chrislusf/seaweedfs:3.80` succeeded without changing system DNS, WSL settings, or proxy settings. No system-level network changes were made. WSL emitted a PATH translation warning for `E:\msys64\ucrt64\bin`; it did not prevent name resolution or registry access.
- SeaweedFS `3.80` is running and healthy on the existing `banking-platform-net`, with persistent volume `banking-enterprise-data-platform_seaweedfs_data`, a 1 GiB memory limit, and localhost-only ports. The master UI returned HTTP 200; an unsigned S3 request returned HTTP 403; signed requests with the configured `.env` identity succeeded and created/checked the `banking-lakehouse` bucket.
- The existing PostgreSQL container remained healthy throughout. Its `banking-enterprise-data-platform_postgres_data` volume was retained. Source counts verified before/after ingestion: customers 100,000; accounts 100,000; cards 100,000; loans 100,000; branches 5; transactions 3,814.
- The first `--source all --chunk-size 5000` run wrote six successful manifests and 403,819 rows to Parquet: 100,000 each from customers, accounts, cards, and loans; 3,814 transactions; and 5 branches. The verifier checked each Parquet object's batch metadata, ingestion timestamp, row count, manifest reference, and source counts.
- The second run wrote another five snapshots and an incremental transaction manifest with 0 rows. The final verification passed across 12 manifests; all 3,814 transaction IDs were unique, and the run log contained 12 successes and no failures.

## Architecture and caveats
- `core_banking`: accounts and balances
- `payment_processing`: transaction event records
- `card_management`: tokenized fictional cards (no real PANs)
- `loan_management`: synthetic loan portfolio
- `customer_management`: fictional customer profiles
- `branch_management`: branch master data

The simulator writes account updates and payment events to different databases. These writes are **not atomic across databases**; failure between them can leave discrepancies. A production-grade system needs a transactional outbox, reconciliation, idempotency, and appropriate financial ledger controls. No external ports are exposed beyond localhost. Credentials are local development credentials, not production secrets. No TLS, RBAC hardening, encryption-at-rest policy, audit trail, backup automation, or CDC connector is implemented yet. Logical WAL is enabled to prepare for later CDC.

## Future modules
Add Iceberg catalog, processing, governance, analytics and observability to this **same** repository. Avoid putting phase numbers in source file names.
