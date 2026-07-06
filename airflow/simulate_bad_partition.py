"""
One-off manual demonstration that GX validation actually catches bad data (Milestone 3
acceptance criterion: "Test with bad data -> DAG fails as expected"). Not a pytest suite
(that's Milestone 4's tests/ directory) and not a DAG (kept out of airflow/dags/ so the
scheduler doesn't try to parse it).

Loads the real latest bronze partition, corrupts a couple of rows in memory only, and
runs it through the exact same run_bronze_validation() the DAG calls. Never writes
anything back to bronze/. Exits non-zero if validation unexpectedly passes.
"""

import sys

sys.path.insert(0, "/opt/airflow/dags")

from bronze_validation import load_latest_partition_df, run_bronze_validation  # noqa: E402


def main() -> int:
    df = load_latest_partition_df().copy()
    if len(df) < 2:
        print(f"Latest partition only has {len(df)} rows — need at least 2 to corrupt safely.")
        return 1

    df.loc[df.index[0], "unit_price"] = -50.0
    df.loc[df.index[1], "region"] = None

    result = run_bronze_validation(df)

    if result.success:
        print("UNEXPECTED: validation passed on deliberately corrupted data.")
        return 1

    print("Validation correctly FAILED on corrupted data. Failed expectations:")
    for validation_result in result.run_results.values():
        for r in validation_result.results:
            if not r.success:
                print(f"  - {r.expectation_config.type}: {r.expectation_config.kwargs}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
