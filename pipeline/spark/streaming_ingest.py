"""Simulated streaming ingest of 1-minute bars with PySpark.

Every PULL_INTERVAL minutes, pull the trailing WINDOW_HOURS of 1-minute bars
from the Massive (formerly Polygon.io) aggregates API, merge them into a Spark
DataFrame (a left anti-join drops rows a newer pull replaces), and recompute
a 30-minute moving average of c, l, h, o and v.

The free API tier rejects same-day bars, so the "stream" replays a completed
trading session: the window advances PULL_INTERVAL minutes per pull from
--base-time.

Usage:
    export MASSIVE_API_KEY=...            # never hard-code the key
    python pipeline/spark/streaming_ingest.py --ticker AAPL \
        --base-time 2025-09-25T10:00 --fast

--fast skips the real 5-minute sleeps between pulls (useful for demos).
"""

import argparse
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType, TimestampType)

BASE_URL = "https://api.massive.com"  # api.polygon.io also still resolves

RAW_SCHEMA = StructType([
    StructField("Stock Name", StringType(), True),
    StructField("UTC Timestamp", TimestampType(), True),
    StructField("c", DoubleType(), True),
    StructField("l", DoubleType(), True),
    StructField("h", DoubleType(), True),
    StructField("o", DoubleType(), True),
    StructField("v", DoubleType(), True),
])

MA_SCHEMA = StructType([
    StructField("Datetime", TimestampType(), True),
    StructField("c_MA", DoubleType(), True),
    StructField("l_MA", DoubleType(), True),
    StructField("h_MA", DoubleType(), True),
    StructField("o_MA", DoubleType(), True),
    StructField("v_MA", DoubleType(), True),
])


def get_bars(api_key, ticker, start_dt, end_dt, timespan="minute", multiplier=1):
    """Return one pull of bars as a pandas DataFrame in RAW_SCHEMA column order."""
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_ms}/{end_ms}"

    # Send the key in a header so it never appears in a logged URL.
    response = requests.get(url, headers={"Authorization": f"Bearer {api_key}"}, timeout=20)

    if response.status_code == 403:
        print(f"Error 403 for {ticker}: check the API key or plan (same-day bars need a paid plan).")
        return pd.DataFrame()
    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text[:200]}")
        return pd.DataFrame()

    results = response.json().get("results") or []
    if not results:
        print(f"No results for {ticker} between {start_dt} and {end_dt}.")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df["UTC Timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True)
    df["Stock Name"] = ticker
    df["v"] = df["v"].astype("float64")
    return df[["Stock Name", "UTC Timestamp", "c", "l", "h", "o", "v"]]


def merge_pull(spark, stock_df, new_pd_df):
    """Incremental load: newer pulls replace overlapping rows, new rows are appended."""
    new_df = spark.createDataFrame(new_pd_df, schema=RAW_SCHEMA)
    if stock_df.isEmpty():
        return new_df
    keys = ["Stock Name", "UTC Timestamp"]
    kept = stock_df.join(new_df.select(keys), on=keys, how="left_anti")
    return kept.unionByName(new_df).dropDuplicates(keys)


def moving_averages(stock_df, minutes=30):
    """Trailing time-based moving average, labelled by the window's end timestamp."""
    window = (Window.partitionBy("Stock Name")
              .orderBy(F.col("UTC Timestamp").cast(LongType()))
              .rangeBetween(-minutes * 60, 0))
    return (stock_df
            .select(col("UTC Timestamp").alias("Datetime"),
                    *[F.avg(c).over(window).alias(f"{c}_MA") for c in ["c", "l", "h", "o", "v"]])
            .filter(col("Datetime").isNotNull()))


def main():
    parser = argparse.ArgumentParser(description="Simulated streaming ingest with PySpark.")
    parser.add_argument("--ticker", default="AAPL")
    parser.add_argument("--base-time", default="2025-09-25T10:00",
                        help="first pull's end time (a past trading session, UTC)")
    parser.add_argument("--duration-min", type=int, default=30)
    parser.add_argument("--interval-min", type=int, default=5)
    parser.add_argument("--window-hours", type=int, default=1)
    parser.add_argument("--ma-minutes", type=int, default=30)
    parser.add_argument("--fast", action="store_true", help="do not sleep between pulls")
    parser.add_argument("--out", default=None, help="optional folder for raw + MA CSV output")
    args = parser.parse_args()

    api_key = os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        raise SystemExit("Set MASSIVE_API_KEY first (see .env.example).")

    spark = SparkSession.builder.appName("StockStreamingIngest").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    stock_df = spark.createDataFrame([], schema=RAW_SCHEMA)
    ma_df = spark.createDataFrame([], schema=MA_SCHEMA)

    base = datetime.fromisoformat(args.base_time)
    num_pulls = args.duration_min // args.interval_min + 1  # 7 pulls for 30 min / 5 min

    for i in range(num_pulls):
        end = base + timedelta(minutes=i * args.interval_min)
        start = end - timedelta(hours=args.window_hours)
        print(f"[pull {i + 1}/{num_pulls}] {args.ticker} {start} -> {end}")

        pull = get_bars(api_key, args.ticker, start, end)
        if not pull.empty:
            stock_df = merge_pull(spark, stock_df, pull)
            print(f"  unique raw rows: {stock_df.count()}")

            current = moving_averages(stock_df, args.ma_minutes)
            ma_df = (ma_df.unionByName(current)
                     .groupBy("Datetime")
                     .agg(*[F.max(f"{c}_MA").alias(f"{c}_MA") for c in ["c", "l", "h", "o", "v"]])
                     .orderBy("Datetime"))
            print(f"  moving-average rows: {ma_df.count()}")

        if i < num_pulls - 1 and not args.fast:
            time.sleep(args.interval_min * 60)

    print("\nRaw bars:")
    stock_df.orderBy("UTC Timestamp").show(5, truncate=False)
    print("Moving averages:")
    ma_df.show(5, truncate=False)

    lo, hi = stock_df.select(F.min("UTC Timestamp"), F.max("UTC Timestamp")).first()
    print(f"Collected {stock_df.count()} bars from {lo} to {hi}")

    if args.out:
        stock_df.orderBy("UTC Timestamp").toPandas().to_csv(f"{args.out}/stream_raw.csv", index=False)
        ma_df.toPandas().to_csv(f"{args.out}/stream_moving_averages.csv", index=False)
        print(f"Saved CSVs to {args.out}")

    spark.stop()


if __name__ == "__main__":
    main()
