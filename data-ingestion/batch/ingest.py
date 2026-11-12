"""Local Bronze batch ingestion. Synthetic source data only; not production-secure."""
import argparse
import io
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import boto3
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / '.env')
STATE = ROOT / 'runtime' / 'ingestion_state.sqlite3'
LOG = ROOT / 'runtime' / 'batch_runs.jsonl'
BUCKET = os.getenv('LAKE_BUCKET', 'banking-lakehouse')
SOURCES = {
    'customer_management': ('customers', 'customer_id', 'snapshot'),
    'core_banking': ('accounts', 'account_id', 'snapshot'),
    'payment_processing': ('transactions', 'transaction_id', 'incremental'),
    'card_management': ('cards', 'card_id', 'snapshot'),
    'loan_management': ('loans', 'loan_id', 'snapshot'),
    'branch_management': ('branches', 'branch_id', 'snapshot'),
}

def emit(event):
    # Keep a lightweight audit trail for each batch run so we can trace failures and success metrics.
    LOG.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(event, default=str)
    with LOG.open('a', encoding='utf-8') as f:
        f.write(line + '\n')
    print(line, flush=True)

def s3_client():
    # Point the batch job at the local S3-compatible endpoint used by the lakehouse environment.
    return boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT', 'http://127.0.0.1:8333'),
                        aws_access_key_id=os.getenv('S3_ACCESS_KEY', 'localdev'),
                        aws_secret_access_key=os.getenv('S3_SECRET_KEY', 'localdev'),
                        region_name='us-east-1', config=Config(s3={'addressing_style': 'path'}, retries={'max_attempts': 3}))

def ensure_bucket(s3):
    # Create the landing bucket lazily so local and CI runs can bootstrap without manual setup.
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError as exc:
        if exc.response.get('Error', {}).get('Code') not in ('404', 'NoSuchBucket'):
            raise
        s3.create_bucket(Bucket=BUCKET)

def db(dbname):
    # Each source database gets its own connection, which keeps source-specific ingestion isolated and easy to debug.
    return psycopg.connect(host=os.getenv('POSTGRES_HOST', '127.0.0.1'),
                           port=int(os.getenv('POSTGRES_PORT', '5432')),
                           user=os.getenv('POSTGRES_USER', 'bedp_admin'),
                           password=os.environ['POSTGRES_PASSWORD'], dbname=dbname)

def normalize(row):
    # Convert IDs and numeric values to strings so Arrow and JSON can serialize them consistently.
    return {k: str(v) if isinstance(v, (Decimal, uuid.UUID)) else v for k, v in row.items()}

def run_source(s3, state, source, chunk_size):
    table, pk, mode = SOURCES[source]
    started = datetime.now(timezone.utc)
    batch_id = str(uuid.uuid4())
    checkpoint_row = state.execute(
        'SELECT last_id FROM checkpoints WHERE source=?', (source,)
    ).fetchone() if mode == 'incremental' else None
    checkpoint = int(checkpoint_row[0]) if checkpoint_row else 0
    # Fixed upper ID bound prevents ingestion from chasing new inserts indefinitely.
    with db(source) as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        max_id = conn.execute(f'SELECT COALESCE(MAX({pk}),0) FROM {table}').fetchone()[0]
        cols = [description.name for description in conn.execute(
            f'SELECT * FROM {table} LIMIT 0'
        ).description]
        offset_id = checkpoint if mode == 'incremental' else 0
        total = 0
        parts = []
        part = 0
        while True:
            rows = conn.execute(f'SELECT * FROM {table} WHERE {pk} > %s AND {pk} <= %s ORDER BY {pk} LIMIT %s', (offset_id, max_id, chunk_size)).fetchall()
            if not rows:
                break
            records = [normalize(dict(zip(cols, row))) for row in rows]
            for record in records:
                record['_batch_id'] = batch_id
                record['_ingested_at'] = started
                record['_source_database'] = source
            data = pa.Table.from_pylist(records)
            buf = io.BytesIO()
            pq.write_table(data, buf, compression='snappy')
            key = f'bronze/{source}/{table}/mode={mode}/batch_id={batch_id}/part-{part:05d}.parquet'
            s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
            parts.append(key)
            offset_id = int(rows[-1][cols.index(pk)])
            total += len(rows)
            part += 1
        manifest = {'batch_id': batch_id, 'source': source, 'table': table, 'mode': mode,
                    'started_at': started.isoformat(), 'completed_at': datetime.now(timezone.utc).isoformat(),
                    'row_count': total, 'previous_checkpoint': checkpoint, 'upper_id': int(max_id), 'objects': parts}
        manifest_key = f'bronze/_manifests/{source}/{batch_id}.json'
        s3.put_object(Bucket=BUCKET, Key=manifest_key, Body=json.dumps(manifest).encode('utf-8'), ContentType='application/json')
    if mode == 'incremental':
        state.execute('INSERT INTO checkpoints(source,last_id) VALUES (?,?) ON CONFLICT(source) DO UPDATE SET last_id=excluded.last_id', (source, int(max_id)))
        state.commit()
    emit({'status':'SUCCESS', **manifest})
    return total

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL to S3 Bronze Parquet ingestion')
    parser.add_argument('--source', choices=['all', *SOURCES], default='all')
    parser.add_argument('--chunk-size', type=int, default=5000)
    args = parser.parse_args()
    if not 1 <= args.chunk_size <= 20000:
        parser.error('chunk-size must be between 1 and 20000')
    STATE.parent.mkdir(parents=True, exist_ok=True)
    s3 = s3_client()
    ensure_bucket(s3)
    with sqlite3.connect(STATE) as state:
        state.execute('CREATE TABLE IF NOT EXISTS checkpoints(source TEXT PRIMARY KEY, last_id INTEGER NOT NULL)')
        for source in SOURCES if args.source == 'all' else [args.source]:
            try:
                run_source(s3, state, source, args.chunk_size)
            except (BotoCoreError, OSError, pa.ArrowException, psycopg.Error,
                    sqlite3.Error, ValueError) as exc:
                emit({'status':'FAILED', 'source':source, 'error_type':type(exc).__name__,
                      'at':datetime.now(timezone.utc).isoformat()})
                raise

if __name__ == '__main__':
    main()
