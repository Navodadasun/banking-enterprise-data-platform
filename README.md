# Banking Enterprise Data Platform (BEDP)

A local-first, cloud-ready **synthetic** banking source system. Initial deployment: one PostgreSQL container, six logical databases, and a Python activity simulator. **Not production banking software.**

**Permanent project location (since November 12, 2026):** `C:\Projects\banking-enterprise-data-platform`. The earlier OneDrive copy is retained as a backup and is not used by Docker. Compose keeps the fixed project name `banking-enterprise-data-platform`, so the existing named volumes (PostgreSQL, SeaweedFS, Nessie, Spark cache/runtime) are reused from this path; changing the working directory must never create a second set of volumes.

### Recover a stale Spark mount

If Spark reports that a `/opt/project/...` file is missing, the container is still bind-mounted to an old directory. Recreate **only** Spark; every named volume is preserved:

```powershell
docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml up -d --no-deps --force-recreate spark
docker exec bedp-spark test -f /opt/project/data-processing/lakehouse/pipeline.py
.\scripts\verify-lakehouse.ps1
```

The lakehouse scripts check that the project is visible before submitting Spark jobs. Never use `docker compose down -v`, `docker system prune --volumes`, or `docker volume rm`; they destroy the banking data, Iceberg warehouse, or Nessie catalog.

PostgreSQL's `/docker-entrypoint-initdb.d` bind mount still references the retired OneDrive path. It is only read when the PostgreSQL data volume is empty, so the running database is unaffected; do not initialise a fresh PostgreSQL volume until that mount is corrected. Recreating PostgreSQL was intentionally out of scope for this migration.

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
docker compose --project-directory . -f infrastructure/docker/compose.storage.yaml config
docker compose --project-directory . -f infrastructure/docker/compose.storage.yaml up -d
docker compose --project-directory . -f infrastructure/docker/compose.storage.yaml ps
.\.venv\Scripts\python.exe -m pip install -r data-ingestion/batch/requirements.txt
.\.venv\Scripts\python.exe data-ingestion/batch/ingest.py --source all --chunk-size 5000
.\.venv\Scripts\python.exe data-ingestion/batch/verify_lake.py
Get-Content runtime\batch_runs.jsonl -Tail 6
```

SeaweedFS uses persistent Docker volume `banking-enterprise-data-platform_seaweedfs_data`, binds its master UI to http://127.0.0.1:9333 and its S3 API to http://127.0.0.1:8333, and is limited to 1 GiB. Keep the existing PostgreSQL volume; do not use `docker compose down -v` or remove volumes to troubleshoot.

The client and SeaweedFS S3 identity use `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, and `LAKE_BUCKET` from `.env`; local-development defaults are shown in `.env.example`. Replace the default access and secret keys in `.env` before sharing the host. Ports are localhost-only, but these settings, the synthetic data, and this deployment are not production-secure.

`runtime\ingestion_state.sqlite3` stores the transaction high-water checkpoint and `runtime\batch_runs.jsonl` stores status, batch IDs, timestamps, row counts, and manifests. Both are ignored by Git and must be preserved for incremental continuity. Snapshot sources are re-ingested on each run; transactions are incremental by `transaction_id`. Run the ingestion command twice to confirm that the second transaction manifest reports zero new rows. The verifier checks the Parquet objects against the manifests and current PostgreSQL counts and detects duplicate transaction IDs across recorded batches.

### Live runtime verification (November 12, 2026)

The lakehouse was started and exercised against the existing PostgreSQL and SeaweedFS data without rerunning ingestion or regenerating synthetic records. Docker Desktop's Linux engine and both WSL 2 distributions were running. PostgreSQL and SeaweedFS reported healthy; their named data volumes were left intact. The non-empty S3 access and secret values in the ignored root `.env` matched the running SeaweedFS identity; no credential values were printed.

Bronze verification passed against 12 existing manifests and Parquet objects. PostgreSQL and Bronze counts matched: customers 100,000; accounts 100,000; transactions 3,814; cards 100,000; loans 100,000; branches 5. All 3,814 transaction IDs were unique across the incremental manifests.

The pinned Spark 3.5.3 / Iceberg 1.6.0 / Nessie 0.92.1 / Hadoop S3A 3.3.4 dependencies resolved from Maven Central. Two end-to-end Spark runs completed and published to Nessie `main`. The first run took 168.8 seconds and the idempotency rerun took 359.9 seconds. Both reported zero duplicate rows dropped and zero quarantined rows; all six Silver row counts and five Gold row counts were unchanged after the rerun.

| Published table | Rows after rerun |
|---|---:|
| `nessie.silver.customers` | 100,000 |
| `nessie.silver.accounts` | 100,000 |
| `nessie.silver.transactions` | 3,814 |
| `nessie.silver.cards` | 100,000 |
| `nessie.silver.loans` | 100,000 |
| `nessie.silver.branches` | 5 |
| `nessie.silver.quarantine_records` | 0 |
| `nessie.gold.daily_transactions` | 10 |
| `nessie.gold.branch_performance` | 5 |
| `nessie.gold.customer_accounts` | 100,000 |
| `nessie.gold.loan_portfolio` | 1 |
| `nessie.gold.monthly_kpis` | 1 |

The latest `daily_transactions` Iceberg snapshot after the rerun was committed at `2026-11-12 05:23:36 UTC`, snapshot ID `8893050378360794250`, operation `append`, with 10 added/total records. The Gold sample query returned 10 rows. For example, on `2026-11-11`, Jaffna City withdrawals had 413 transactions totaling `1060767.00`, net `-1060767.00`; Kandy City withdrawals had 400 totaling `1017428.00`, net `-1017428.00`.

The Docker engine reported 7.61 GiB available to Linux containers (16 CPUs), despite the host's 16 GB RAM. The configured Spark cap is 4 GiB / 2 CPUs and Nessie's cap is 768 MiB; observed Spark usage stayed below 2.1 GiB during processing.

## Spark, Iceberg, Nessie lakehouse

The lakehouse runs Spark in local mode with two worker threads; it does not start a multi-node Spark cluster. Apache Spark `3.5.3`, Apache Iceberg `1.6.0` (`iceberg-spark-runtime-3.5_2.12`), Project Nessie `0.92.1` and its Spark 3.5/Scala 2.12 extension, Hadoop S3A `3.3.4`, and the Spark-container boto3 client `1.35.99` are pinned. The Spark/Iceberg/Nessie artifact combination follows the published Nessie Spark 3.5 compatibility matrix for Iceberg 1.6.0.

The existing PostgreSQL, batch ingestion, and SeaweedFS services are not replaced. The lakehouse Compose file joins their existing `banking-platform-net` network and adds Nessie, a small Spark driver, and one-shot volume-permission helpers. Nessie uses its own persistent RocksDB volume; Iceberg data and metadata files are stored in the existing SeaweedFS bucket under `warehouse/`. Spark code is mounted read-only, and its Maven cache and structured run log use separate persistent volumes. Do not use `docker compose down -v`; it deletes persistent data.

Before building the lakehouse, confirm the root `.env` contains non-empty `S3_ACCESS_KEY` and `S3_SECRET_KEY` values that match the existing SeaweedFS S3 identity. If either is missing, add it with `notepad .env`; do not put credentials in scripts or commit them.

Run these commands from the repository root in PowerShell. Keep the existing `.env` and ingestion checkpoint. Check that Docker Desktop's Linux engine and WSL 2 are available:

```powershell
docker info
docker context ls
wsl --status
wsl -l -v
```

Check and start the existing source/storage containers. If they already exist but are stopped, `docker start` avoids recreating them; do not remove their named volumes:

```powershell
docker ps -a
docker start bedp-postgres bedp-seaweedfs
docker inspect --format 'status={{.State.Status}} health={{.State.Health.Status}}' bedp-postgres
docker inspect --format 'status={{.State.Status}} health={{.State.Health.Status}}' bedp-seaweedfs
```

For a first-time setup where those containers do not yet exist, Compose creates them with the existing named volumes:

```powershell
docker compose --project-directory . -f infrastructure/docker/compose.yaml up -d
docker compose --project-directory . -f infrastructure/docker/compose.storage.yaml up -d
docker compose --project-directory . -f infrastructure/docker/compose.yaml ps
docker compose --project-directory . -f infrastructure/docker/compose.storage.yaml ps
```

Check the source counts and Bronze data using the existing verifier. Do not run ingestion if the existing manifests and Parquet objects verify:

```powershell
docker exec bedp-postgres pg_isready -U bedp_admin -d postgres
.\.venv\Scripts\python.exe data-ingestion\batch\verify_lake.py
```

For a new or empty Bronze lake only, use the existing ingestion flow; it does not delete source records or existing SeaweedFS data:

```powershell
.\.venv\Scripts\python.exe data-ingestion\batch\ingest.py --source all --chunk-size 5000
.\.venv\Scripts\python.exe data-ingestion\batch\verify_lake.py
```

Build and start the lakehouse services, then process Bronze:

```powershell
docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml config
docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml up -d --build
docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml ps
.\scripts\run-lakehouse.ps1 -RunId demo-001
.\scripts\verify-lakehouse.ps1
.\scripts\run-lakehouse-sql.ps1
```

The first Spark submission downloads the pinned Maven artifacts, so it requires network access to Maven Central. Omit `-RunId` to generate a unique run ID automatically. Reuse an existing source/storage data volume; these commands don't generate synthetic data. Each run selects the latest completed manifest for the five full-snapshot sources and reads all completed transaction manifests. Schema/null/domain checks and referential checks run before a Silver table is written; invalid records are saved to `nessie.silver.quarantine_records`. Duplicate primary keys are counted and deterministically reduced to the latest Bronze ingestion. Snapshot tables are atomically replaced on a temporary Nessie branch, and transactions are merged by `transaction_id`, making transaction replays idempotent.

The pipeline initializes the empty `silver` and `gold` namespaces on Nessie `main` before creating its run branch; this gives a brand-new Nessie catalog a merge base. All six Silver tables and five Gold analytics tables are staged on the run-specific branch. The branch is merged into `main` only after every input, quality check, Silver write, and Gold aggregate succeeds; a failed run leaves published tables unchanged. Nessie checks the original `main` hash during publish to reject a concurrent conflicting run. Structured JSON events include run IDs, counts, durations, and failure types without record values or credentials. Detailed counts, quarantine results, Iceberg snapshot metadata, and a sample Gold query are available from the Python and SQL verification scripts.

The Spark service is limited to 4 GiB and 2 CPUs and uses `local[2]` with a 3 GiB driver. Nessie is capped at 768 MiB; existing PostgreSQL and SeaweedFS limits are unchanged. Expected resource use is suitable for the 16 GiB development host.

### Lakehouse troubleshooting

- **Docker commands cannot connect:** start Docker Desktop and wait for its Linux engine before running any Compose command. Do not remove or recreate volumes to fix a stopped daemon.
- **Compose reports that `banking-platform-net` is missing:** start the existing PostgreSQL and SeaweedFS Compose files above. They create/use the network without deleting their volumes.
- **Spark reports it cannot resolve Nessie or SeaweedFS:** check that `bedp-nessie` is healthy and the existing `bedp-seaweedfs` container is running on `banking-platform-net`; keep the service endpoints in Compose rather than changing them to `localhost`.
- **Nessie does not become healthy:** inspect `docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml logs nessie`. The local catalog uses a persistent RocksDB volume; do not delete that volume when troubleshooting.
- **Spark package resolution fails:** confirm outbound Maven Central connectivity, then rerun the submission. The dependency coordinates and versions are pinned in the scripts; do not substitute versions independently.
- **Bronze decimal fields fail validation:** keep the existing ingestion output unchanged. Decimal values are serialized as strings in Bronze Parquet and cast to decimal by the Silver job.
- **No completed Bronze manifests or missing Parquet objects:** run `data-ingestion\batch\verify_lake.py` after confirming PostgreSQL and SeaweedFS are healthy. A transformation intentionally fails before publish if a manifest is absent, malformed, or references missing objects.
- **A run fails:** inspect the JSON failure event emitted by `run-lakehouse.ps1`, then check `docker compose ... logs nessie` and the Spark command output. Failed run branches are not merged; a failed branch cleanup is explicitly reported as `CLEANUP_WARNING`.
- **Credentials:** set the S3 access/secret keys only in the ignored root `.env`; do not pass them as script arguments or add them to source control. The local sample defaults are for development only.

### Safe shutdown and recovery

Stop only the lakehouse containers when you want to free resources; this retains Nessie, Maven-cache, and runtime-log volumes:

```powershell
docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml stop spark nessie
```

Resume the services and rerun the verification without rebuilding:

```powershell
docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml up -d
docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml ps
.\scripts\verify-lakehouse.ps1
```

If PostgreSQL or SeaweedFS is stopped, start the existing containers with `docker start bedp-postgres bedp-seaweedfs` before Spark. For diagnosis, use `docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml logs --tail 100 nessie spark` and inspect `runtime\lakehouse_runs.jsonl`. Never use `docker compose down -v`, `docker volume rm`, or Docker prune commands for routine recovery; they can delete the source data or Iceberg/Nessie state.

## Architecture and caveats
- `core_banking`: accounts and balances
- `payment_processing`: transaction event records
- `card_management`: tokenized fictional cards (no real PANs)
- `loan_management`: synthetic loan portfolio
- `customer_management`: fictional customer profiles
- `branch_management`: branch master data

The simulator writes account updates and payment events to different databases. These writes are **not atomic across databases**; failure between them can leave discrepancies. A production-grade system needs a transactional outbox, reconciliation, idempotency, and appropriate financial ledger controls. No external ports are exposed beyond localhost. Credentials are local development credentials, not production secrets. No TLS, RBAC hardening, encryption-at-rest policy, audit trail, backup automation, or CDC connector is implemented yet. Logical WAL is enabled to prepare for later CDC.

## Future modules
The Spark/Iceberg/Nessie catalog, Silver/Gold processing, and baseline runtime verification are implemented in this **same** repository. Governance, automated backup/retention, CDC, Kafka, and Airflow remain future work; avoid adding them until the lakehouse operational requirements are defined and tested. Avoid putting phase numbers in source file names.
