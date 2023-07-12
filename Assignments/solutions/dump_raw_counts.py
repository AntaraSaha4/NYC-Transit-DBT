#!/usr/bin/env python
import duckdb
import sys

raw_tables = [
    "central_park_weather",
    "daily_citi_bike_trip_counts_and_weather",
    "fhv_bases",
    "fhv_tripdata",
    "fhvhv_tripdata",
    "green_tripdata",
    "yellow_tripdata",
    "bike_data",
]


def main(conn):
    for t in sorted(raw_tables):
        rows = conn.sql(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(t, rows)


if __name__ == "__main__":
    with duckdb.connect(sys.argv[1]) as conn:
        main(conn)
