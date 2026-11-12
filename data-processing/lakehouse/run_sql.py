"""Execute the checked-in lakehouse SQL verification queries."""

import json
import re
from pathlib import Path

from pipeline import build_spark


def main() -> None:
    sql_path = Path(__file__).with_name("verification.sql")
    statements = [
        statement.strip()
        for statement in re.split(r";\s*(?:\r?\n|$)", sql_path.read_text(encoding="utf-8"))
        if statement.strip()
    ]
    spark = build_spark("main")
    try:
        for index, statement in enumerate(statements, start=1):
            rows = spark.sql(statement).collect()
            print(
                json.dumps(
                    {
                        "statement": index,
                        "rows": [row.asDict(recursive=True) for row in rows],
                    },
                    default=str,
                )
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
