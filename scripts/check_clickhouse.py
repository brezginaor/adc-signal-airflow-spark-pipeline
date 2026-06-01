import argparse
from clickhouse_connect import get_client


CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
DATABASE = "adc_signals"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--min-rows", type=int, default=1)
    args = parser.parse_args()

    client = get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username="default",
        password="clickhouse"
    )

    query = f"SELECT count() FROM {DATABASE}.{args.table}"
    count = client.query(query).result_rows[0][0]

    print(f"Table {DATABASE}.{args.table} contains {count} rows")

    if count < args.min_rows:
        raise RuntimeError(
            f"Table {DATABASE}.{args.table} contains only {count} rows, expected at least {args.min_rows}"
        )

    print("ClickHouse check passed")


if __name__ == "__main__":
    main()