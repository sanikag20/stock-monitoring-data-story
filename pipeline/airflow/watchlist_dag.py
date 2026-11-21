"""Airflow DAG: fetch the watchlist, compute metrics, raise volatility alerts.

    fetch_data  ->  analytics_and_summary  ->  alert_notification

The analytics step writes the same CSVs the story page reads, so a finished
run can be viewed at http://127.0.0.1:8001/story.html?data=latest

Setup: symlink or copy this file into your Airflow dags folder and keep the
repo checked out. Optional environment variables:
    WATCHLIST_REPO_DIR     repo root (default: two folders above this file)
    WATCHLIST_OUTPUT_DIR   where to write results (default: <repo>/data/watchlist_latest)
    WATCHLIST_TICKERS      comma-separated tickers (default: NVDA,TSLA,SPY)
    WATCHLIST_DAYS         trading days per run (default: 30)
    ALERT_THRESHOLD_PCT    latest-day swing that triggers an alert (default: 3)
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

REPO_DIR = Path(os.environ.get("WATCHLIST_REPO_DIR", Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(REPO_DIR))

OUTPUT_DIR = Path(os.environ.get("WATCHLIST_OUTPUT_DIR", REPO_DIR / "data" / "watchlist_latest"))
TICKERS = os.environ.get("WATCHLIST_TICKERS", "NVDA,TSLA,SPY").split(",")
TRADING_DAYS = int(os.environ.get("WATCHLIST_DAYS", "30"))
ALERT_THRESHOLD = float(os.environ.get("ALERT_THRESHOLD_PCT", "3"))


# Task 1: Fetch Data
def fetch_data():
    from pipeline.fetch_watchlist import fetch

    df = fetch(OUTPUT_DIR, TICKERS, TRADING_DAYS)
    print(f"Rows downloaded: {len(df)} -> {OUTPUT_DIR / 'stock_daily.csv'}")


# Task 2: Analytics & Summary
def calculate_summary():
    from pipeline.watchlist_metrics import run

    tables = run(OUTPUT_DIR / "stock_daily.csv", OUTPUT_DIR)
    print("\nWatchlist summary:")
    print(tables["stock_summary"].round(2).to_string(index=False))
    print("\nReview workload:")
    print(tables["workload_summary"].to_string(index=False))


# Task 3: Alert / Notification
def check_alerts():
    import pandas as pd

    metrics = pd.read_csv(OUTPUT_DIR / "stock_metrics.csv", dtype={"date": str})
    latest_date = metrics["date"].max()
    latest = metrics[metrics["date"] == latest_date]

    alerts = []
    for _, row in latest.iterrows():
        swing = row["daily_swing_pct"]
        if swing >= ALERT_THRESHOLD:
            print(f"ALERT: {row['ticker']} experienced high volatility "
                  f"({swing:.2f}% swing) on {latest_date}")
            alerts.append(row["ticker"])
        else:
            print(f"{row['ticker']}: Normal volatility ({swing:.2f}% swing) on {latest_date}")

    if not alerts:
        print("\nAll assets traded within normal volatility ranges.")
    return alerts  # available to downstream tasks via XCom


default_args = {
    "owner": "sanika",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="watchlist_volatility_pipeline",
    default_args=default_args,
    description="Stock watchlist volatility monitoring pipeline",
    start_date=datetime(2026, 10, 5),
    schedule=None,  # trigger manually, or set e.g. "30 21 * * 1-5" for after the close
    catchup=False,
    tags=["stocks", "volatility"],
) as dag:

    fetch_task = PythonOperator(task_id="fetch_data", python_callable=fetch_data)
    analytics_task = PythonOperator(task_id="analytics_and_summary", python_callable=calculate_summary)
    alert_task = PythonOperator(task_id="alert_notification", python_callable=check_alerts)

    fetch_task >> analytics_task >> alert_task
