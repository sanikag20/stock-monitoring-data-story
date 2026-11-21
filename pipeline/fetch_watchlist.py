"""Download daily OHLCV bars for the watchlist from Yahoo Finance (yfinance).

Writes to out_dir:
    <TICKER>_raw.csv   untouched provider output per ticker
    stock_daily.csv    tidy table: date, ticker, Open, High, Low, Close, Volume
    provenance.json    what was requested and when

Run standalone:
    python -m pipeline.fetch_watchlist data/watchlist_latest --days 30
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

TICKERS = ["NVDA", "TSLA", "SPY"]
TRADING_TZ = "America/New_York"

# Same download settings as the saved snapshot in data/watchlist/.
YF_SETTINGS = dict(
    interval="1d",
    auto_adjust=False,
    back_adjust=False,
    repair=False,
    rounding=False,
    prepost=False,
    actions=False,
    threads=False,
    progress=False,
    multi_level_index=False,
    ignore_tz=False,
)


def tidy(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Turn one ticker's yfinance frame into rows keyed by New York trading date."""
    dates = pd.DatetimeIndex(pd.to_datetime(raw.index, utc=True))
    dates = dates.tz_convert(TRADING_TZ)
    stock = raw[["Open", "High", "Low", "Close", "Volume"]].copy()
    stock.insert(0, "ticker", ticker)
    stock.insert(0, "date", dates.strftime("%Y-%m-%d"))
    return stock.reset_index(drop=True)


def fetch(out_dir: Path, tickers=TICKERS, trading_days: int = 30,
          end: date | None = None) -> pd.DataFrame:
    """Fetch the last `trading_days` completed sessions shared by every ticker."""
    import yfinance as yf  # imported here so the metrics code runs without it

    out_dir.mkdir(parents=True, exist_ok=True)
    end_exclusive = end or datetime.now(timezone.utc).date()  # excludes today's open bar
    # Two calendar days per trading day leaves room for weekends and holidays.
    start = end_exclusive - timedelta(days=trading_days * 2 + 10)

    frames = []
    for ticker in tickers:
        raw = yf.download(tickers=ticker, start=start.isoformat(),
                          end=end_exclusive.isoformat(), **YF_SETTINGS)
        if raw is None or raw.empty:
            raise RuntimeError(f"No data returned for {ticker}.")
        raw.to_csv(out_dir / f"{ticker}_raw.csv")
        frames.append(tidy(raw, ticker))

    df = pd.concat(frames, ignore_index=True)

    # Keep only dates every ticker traded, then the most recent N of them.
    common = set.intersection(*(set(f["date"]) for f in frames))
    keep = sorted(common)[-trading_days:]
    if len(keep) < trading_days:
        raise RuntimeError(f"Only {len(keep)} common trading dates; wanted {trading_days}.")
    df = df[df["date"].isin(keep)].sort_values(["ticker", "date"]).reset_index(drop=True)
    df.to_csv(out_dir / "stock_daily.csv", index=False)

    (out_dir / "provenance.json").write_text(json.dumps({
        "provider": "Yahoo Finance via yfinance",
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "yfinance_version": yf.__version__,
        "start": keep[0],
        "end_inclusive": keep[-1],
        "tickers": list(tickers),
        "trading_timezone": TRADING_TZ,
        **{k: v for k, v in YF_SETTINGS.items() if k not in ("progress", "threads")},
        "rows": len(df),
    }, indent=2))
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out_dir", type=Path)
    parser.add_argument("--days", type=int, default=30, help="trading days to keep")
    parser.add_argument("--tickers", default=",".join(TICKERS))
    args = parser.parse_args()
    df = fetch(args.out_dir, args.tickers.split(","), args.days)
    print(f"Saved {len(df)} rows to {args.out_dir / 'stock_daily.csv'}")


if __name__ == "__main__":
    main()
