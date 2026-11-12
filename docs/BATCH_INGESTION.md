# Batch ingestion to Bronze storage

Run from project root on Windows PowerShell. Existing PostgreSQL data remains untouched.

1. `docker compose -f infrastructure/docker/compose.storage.yaml up -d`
2. `docker compose -f infrastructure/docker/compose.storage.yaml ps` — confirm `bedp-seaweedfs` is healthy. The local SeaweedFS master UI is at http://127.0.0.1:9333 ; S3 API is http://127.0.0.1:8333 .
3. `.\.venv\Scripts\python.exe -m pip install -r data-ingestion/batch/requirements.txt`
4. `.\.venv\Scripts\python.exe data-ingestion/batch/ingest.py --source all --chunk-size 5000`
5. `.\.venv\Scripts\python.exe data-ingestion/batch/verify_lake.py`
6. `Get-Content runtime\batch_runs.jsonl -Tail 6`

This is a local, synthetic-data prototype. Compose creates a SeaweedFS S3 identity using `S3_ACCESS_KEY` and `S3_SECRET_KEY` from `.env` (defaults are `localdev` for development), and the ingestion client signs requests with the same values. Change these defaults before sharing the host. Do not expose port 8333 publicly or store real banking information. TLS, encryption, hardened access controls, and backups are not configured.

## Ingestion semantics

- Transactions: append-only incremental by `transaction_id` with a SQLite high-water checkpoint. Keep the checkpoint file under `runtime/`; a crash after uploading but before checkpoint commit can create duplicate rows across batches. The verifier detects duplicate transaction IDs across recorded batches; downstream Silver should still deduplicate on `transaction_id`/`event_id`.
- Customers, accounts, cards, loans, branches: full snapshot on every run, since balances and customer attributes can change. Each snapshot is a new batch; analytics must select the latest complete manifest, not union snapshots blindly.
- Data is written as Parquet under `s3://banking-lakehouse/bronze/`. JSON manifests provide per-batch row counts and object lists.
- If a run fails, partial objects may remain. Only batches with a completed manifest are considered valid.
- The local SQLite checkpoint file under `runtime/` must be preserved for incremental continuity. Future work will migrate this metadata into a dedicated pipeline control database.
- `verify_lake.py` checks each manifest against its Parquet objects, validates batch metadata, compares latest snapshots and cumulative transactions with PostgreSQL row counts, and confirms transaction IDs are unique.
- This stage does not implement CDC, streaming, Airflow, Iceberg, or encryption.
