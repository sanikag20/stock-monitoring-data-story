"""Watchlist metrics shared by the notebook, the Airflow DAG and the story page.

Input: a daily OHLCV table with columns
    date, ticker, Open, High, Low, Close, Volume

Outputs (written to one folder, read by story.html):
    stock_metrics.csv     daily normalized index, swing and alert flags
    stock_summary.csv     per-ticker return, average swing, alert counts
    daily_workload.csv    one row per date and threshold, with the asset chosen for review
    workload_summary.csv  workload totals per threshold
    alert_details.csv     every alert, marked selected or not

Run on the saved snapshot:
    python -m pipeline.watchlist_metrics data/watchlist/stock_daily.csv data/watchlist
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

THRESHOLDS = (3, 5)
REQUIRED = ["Open", "High", "Low", "Close", "Volume"]


def validate(df: pd.DataFrame) -> None:
    """Raise ValueError if the daily table is incomplete or inconsistent."""
    missing_cols = {"date", "ticker", *REQUIRED} - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns: {sorted(missing_cols)}")
    if df.empty:
        raise ValueError("No rows to analyze.")
    if df.duplicated(["ticker", "date"]).any():
        raise ValueError("Duplicate ticker/date records found.")

    # Every ticker must cover the same trading dates.
    date_sets = df.groupby("ticker")["date"].apply(frozenset)
    if date_sets.nunique() != 1:
        raise ValueError("Tickers do not cover the same trading dates.")

    values = df[REQUIRED].to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise ValueError("Missing or non-finite OHLCV values.")
    if not (df[["Open", "High", "Low", "Close"]] > 0).all().all():
        raise ValueError("Prices must be positive.")
    if not (df["Volume"] >= 0).all():
        raise ValueError("Volume must be nonnegative.")
    if not (df["High"] >= df[["Open", "Low", "Close"]].max(axis=1)).all():
        raise ValueError("High is below Open, Low or Close on some rows.")
    if not (df["Low"] <= df[["Open", "High", "Close"]].min(axis=1)).all():
        raise ValueError("Low is above Open, High or Close on some rows.")


def compute(df: pd.DataFrame, thresholds=THRESHOLDS) -> dict[str, pd.DataFrame]:
    """Compute every table the story page reads. Same logic as the notebook."""
    df = df.sort_values(["ticker", "date"]).copy()

    first_close = df.groupby("ticker")["Close"].transform("first")
    df["normalized_index"] = 100 * df["Close"] / first_close
    df["daily_swing_pct"] = 100 * (df["High"] - df["Low"]) / df["Open"]
    for t in thresholds:
        df[f"alert_{t}pct"] = df["daily_swing_pct"] >= t

    agg = {
        "trading_days": ("date", "count"),
        "first_close": ("Close", "first"),
        "last_close": ("Close", "last"),
        "avg_daily_swing_pct": ("daily_swing_pct", "mean"),
    }
    agg.update({f"alerts_{t}pct": (f"alert_{t}pct", "sum") for t in thresholds})
    summary = df.groupby("ticker").agg(**agg).reset_index()
    summary["window_return_pct"] = (
        100 * (summary["last_close"] - summary["first_close"]) / summary["first_close"]
    )

    # Review rule: at most one asset per date, the largest swing, ties alphabetical.
    workload_rows, alert_rows = [], []
    for t in thresholds:
        alerts = df[df["daily_swing_pct"] >= t]
        for date in sorted(df["date"].unique()):
            daily = alerts[alerts["date"] == date].sort_values(
                ["daily_swing_pct", "ticker"], ascending=[False, True]
            )
            count = len(daily)
            selected = daily.iloc[0]["ticker"] if count else None
            workload_rows.append({
                "date": date,
                "threshold_pct": t,
                "asset_alerts": count,
                "selected_asset": selected,
                "unreviewed_alerts": max(count - 1, 0),
            })
            for _, row in daily.iterrows():
                alert_rows.append({
                    "date": date,
                    "threshold_pct": t,
                    "ticker": row["ticker"],
                    "daily_swing_pct": row["daily_swing_pct"],
                    "selected_for_review": row["ticker"] == selected,
                })

    workload = pd.DataFrame(workload_rows)
    alert_details = pd.DataFrame(
        alert_rows,
        columns=["date", "threshold_pct", "ticker", "daily_swing_pct", "selected_for_review"],
    )
    workload_summary = workload.groupby("threshold_pct").agg(
        total_asset_alerts=("asset_alerts", "sum"),
        dates_with_alerts=("asset_alerts", lambda x: (x > 0).sum()),
        simultaneous_alert_dates=("asset_alerts", lambda x: (x > 1).sum()),
        unreviewed_alerts=("unreviewed_alerts", "sum"),
    ).reset_index()

    return {
        "stock_metrics": df,
        "stock_summary": summary,
        "daily_workload": workload,
        "workload_summary": workload_summary,
        "alert_details": alert_details,
    }


def write(tables: dict[str, pd.DataFrame], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, table in tables.items():
        table.to_csv(out_dir / f"{name}.csv", index=False)


def run(input_csv: Path, out_dir: Path) -> dict[str, pd.DataFrame]:
    df = pd.read_csv(
        input_csv, dtype={"date": str, "ticker": str}, float_precision="round_trip"
    )
    validate(df)
    tables = compute(df)
    write(tables, out_dir)
    return tables


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input_csv", type=Path, help="daily OHLCV table (stock_daily.csv)")
    parser.add_argument("out_dir", type=Path, help="folder for the output CSVs")
    args = parser.parse_args()

    tables = run(args.input_csv, args.out_dir)
    print(tables["stock_summary"].round(2).to_string(index=False))
    print()
    print(tables["workload_summary"].to_string(index=False))


if __name__ == "__main__":
    main()
