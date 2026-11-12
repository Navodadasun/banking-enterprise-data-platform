"""Verify Iceberg table counts, quality results, metadata, and sample Gold output."""

import json

from pipeline import GOLD_NAMESPACE, SILVER_NAMESPACE, build_spark


TABLES = {
    SILVER_NAMESPACE: (
        "customers",
        "accounts",
        "transactions",
        "cards",
        "loans",
        "branches",
    ),
    GOLD_NAMESPACE: (
        "daily_transactions",
        "branch_performance",
        "customer_accounts",
        "loan_portfolio",
        "monthly_kpis",
    ),
}


def main() -> None:
    spark = build_spark("main")
    try:
        counts = {}
        for namespace, names in TABLES.items():
            for name in names:
                table = f"{namespace}.{name}"
                count = spark.table(table).count()
                counts[table] = count
                print(json.dumps({"check": "table_count", "table": table, "rows": count}))

        quarantine_table = f"{SILVER_NAMESPACE}.quarantine_records"
        if spark.catalog.tableExists(quarantine_table):
            print(
                json.dumps(
                    {
                        "check": "quarantine_count",
                        "rows": spark.table(quarantine_table).count(),
                    }
                )
            )
        else:
            print(json.dumps({"check": "quarantine_count", "rows": 0}))

        snapshots = spark.sql(
            f"""
            SELECT committed_at, snapshot_id, operation, summary
            FROM {GOLD_NAMESPACE}.daily_transactions.snapshots
            ORDER BY committed_at DESC
            LIMIT 1
            """
        )
        print(
            json.dumps(
                {
                    "check": "iceberg_metadata",
                    "latest_snapshot": [row.asDict(recursive=True) for row in snapshots.collect()],
                },
                default=str,
            )
        )
        sample = spark.sql(
            f"""
            SELECT transaction_date, branch_name, transaction_type,
                   transaction_count, total_amount, net_amount
            FROM {GOLD_NAMESPACE}.daily_transactions
            ORDER BY transaction_date DESC, transaction_count DESC
            LIMIT 10
            """
        )
        print(
            json.dumps(
                {
                    "check": "gold_sample",
                    "rows": [row.asDict(recursive=True) for row in sample.collect()],
                },
                default=str,
            )
        )
        print(json.dumps({"check": "verification", "status": "SUCCESS", "tables": counts}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
