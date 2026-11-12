"""Verify Bronze manifests, Parquet objects, source counts, and transaction uniqueness."""
import io
import json
from datetime import datetime

import pyarrow.parquet as pq

from ingest import BUCKET, SOURCES, db, s3_client


def source_row_count(source, table):
    with db(source) as conn:
        return conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]


def main():
    s3 = s3_client()
    s3.head_bucket(Bucket=BUCKET)

    manifests = []
    for page in s3.get_paginator('list_objects_v2').paginate(
        Bucket=BUCKET, Prefix='bronze/_manifests/'
    ):
        for item in page.get('Contents', []):
            response = s3.get_object(Bucket=BUCKET, Key=item['Key'])
            manifests.append(json.loads(response['Body'].read()))

    if not manifests:
        raise RuntimeError(f'No Bronze manifests found in bucket {BUCKET!r}. Run ingest.py first.')

    latest_snapshots = {}
    transaction_ids = set()
    transaction_rows = 0
    batch_ids = set()
    for manifest in manifests:
        batch_id = manifest['batch_id']
        source = manifest['source']
        table, primary_key, expected_mode = SOURCES[source]
        if manifest['table'] != table or manifest['mode'] != expected_mode:
            raise RuntimeError(f'Manifest {batch_id} has unexpected source metadata.')
        if batch_id in batch_ids:
            raise RuntimeError(f'Duplicate batch ID found: {batch_id}')
        batch_ids.add(batch_id)

        measured_rows = 0
        for key in manifest['objects']:
            if f'batch_id={batch_id}/' not in key:
                raise RuntimeError(f'Object {key} is not scoped to batch {batch_id}.')
            response = s3.get_object(Bucket=BUCKET, Key=key)
            parquet = pq.read_table(io.BytesIO(response['Body'].read()))
            if not {'_batch_id', '_ingested_at', '_source_database', primary_key}.issubset(
                parquet.column_names
            ):
                raise RuntimeError(f'Parquet object {key} is missing required columns.')
            if parquet.column('_batch_id').unique().to_pylist() != [batch_id]:
                raise RuntimeError(f'Parquet object {key} contains an unexpected batch ID.')
            if parquet.column('_source_database').unique().to_pylist() != [source]:
                raise RuntimeError(f'Parquet object {key} contains an unexpected source name.')
            timestamps = parquet.column('_ingested_at')
            expected_timestamp = datetime.fromisoformat(manifest['started_at'])
            if timestamps.null_count or timestamps.unique().to_pylist() != [expected_timestamp]:
                raise RuntimeError(f'Parquet object {key} has an unexpected ingestion timestamp.')
            measured_rows += parquet.num_rows
            if source == 'payment_processing':
                ids = parquet.column('transaction_id').to_pylist()
                if len(ids) != len(set(ids)) or transaction_ids.intersection(ids):
                    raise RuntimeError('Duplicate transaction IDs found in Bronze incremental data.')
                transaction_ids.update(ids)

        if measured_rows != manifest['row_count']:
            raise RuntimeError(
                f'Batch {batch_id} row count mismatch: manifest={manifest["row_count"]}, '
                f'Parquet={measured_rows}.'
            )
        print(
            f'VALID batch_id={batch_id} source={source} rows={measured_rows} '
            f'objects={len(manifest["objects"])} started_at={manifest["started_at"]}'
        )

        if source == 'payment_processing':
            transaction_rows += measured_rows
        elif source not in latest_snapshots or manifest['started_at'] > latest_snapshots[source]['started_at']:
            latest_snapshots[source] = manifest

    for source, (table, _, mode) in SOURCES.items():
        expected_rows = source_row_count(source, table)
        if mode == 'snapshot':
            manifest = latest_snapshots.get(source)
            if manifest is None or manifest['row_count'] != expected_rows:
                actual_rows = manifest['row_count'] if manifest else 'missing'
                raise RuntimeError(
                    f'{source}.{table} count mismatch: PostgreSQL={expected_rows}, '
                    f'latest Bronze snapshot={actual_rows}.'
                )
        elif transaction_rows != expected_rows:
            raise RuntimeError(
                f'{source}.{table} count mismatch: PostgreSQL={expected_rows}, '
                f'Bronze incremental total={transaction_rows}.'
            )
        print(f'VERIFIED source={source} mode={mode} postgres_rows={expected_rows}')

    print(
        f'Bronze verification passed: bucket={BUCKET} manifests={len(manifests)} '
        f'batches={len(batch_ids)} transaction_rows={transaction_rows} '
        f'unique_transaction_ids={len(transaction_ids)}'
    )


if __name__ == '__main__':
    main()
