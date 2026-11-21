# Methodology

## Watchlist data (`data/watchlist/`)

| Setting | Value |
|---|---|
| Provider | Yahoo Finance via `yfinance` (version recorded in `provenance.json`) |
| Tickers | NVDA, TSLA, SPY |
| Window | 2026-08-21 to 2026-10-02 inclusive: 30 trading dates, 90 rows (Sept 7 market holiday excluded) |
| Interval | `1d` |
| Adjustments | `auto_adjust=False`, `back_adjust=False`, `repair=False`, `rounding=False`, `prepost=False`, `actions=False` |
| Dates | Converted to `America/New_York` trading dates |
| Units | USD for prices, shares for volume, percent for returns and swings |

The untouched provider output is kept in `<TICKER>_raw.csv`. The analysis table `stock_daily.csv`
keeps only `date, ticker, Open, High, Low, Close, Volume`. It drops `Adj Close`, so the normalized index
tracks closing prices only and is not a total-return series.

## Validation (`pipeline/watchlist_metrics.validate`)

- No duplicate ticker/date rows, and every ticker covers the same trading dates.
- OHLCV values are present and finite. Prices are positive and volume is nonnegative.
- `High` is at least Open, Low and Close; `Low` is at most Open, High and Close.

Incomplete downloads are rejected, never interpolated or forward-filled.

## Calculations

```text
Normalized index_t   = 100 × Close_t / Close_first_date
Window return (%)    = 100 × (Close_last − Close_first) / Close_first
Daily swing (%)      = 100 × (High − Low) / Open
Average daily swing  = mean of the daily swings
Alert                = daily swing ≥ threshold   (thresholds: 3% and 5%)
```

Daily swing is the intraday range relative to the open. It is not the daily return, a standard
deviation or an annualized volatility. Values are stored at full precision and rounded only for
display.

**Review rule:** on each date with alerts, review the stock with the largest swing; break exact
ties alphabetically. At most one stock is reviewed per date. Any other alerts that day are logged as
unreviewed.

| Output | Contents |
|---|---|
| `stock_metrics.csv` | daily index, swing, alert flags |
| `stock_summary.csv` | per-ticker first/last close, return, average swing, alert counts |
| `daily_workload.csv` | one row per date and threshold, with the selected stock |
| `workload_summary.csv` | alert totals, dates with alerts, competing-alert dates, unreviewed alerts |
| `alert_details.csv` | every alert and whether it was selected |

## Live tracker

- `stock_server.py` calls the Massive daily aggregates endpoint
  `/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}` with `adjusted=true`, `sort=asc`, `limit=5000`.
- The window ends yesterday in New York time, which excludes today's unfinished bar, and starts 30
  calendar days earlier.
- The two most recent distinct trading dates give the latest close and the previous close:
  `change = latest − previous`, and `% change = 100 × change / previous`.
- The API key is sent in a header, never in the URL, and is redacted from error messages.
- Requests are spaced at least 13 seconds apart. The page waits 90 seconds after each cycle
  finishes before starting the next, so cycles never overlap.
- History is kept in browser memory. Use **Download evidence JSON** to save a session.

## Streaming ingest (Spark)

- Each pull asks for the trailing hour of 1-minute bars. The window moves forward 5 minutes per pull,
  for 7 pulls in total.
- Merge step: `existing LEFT ANTI JOIN new ON (Stock Name, UTC Timestamp)`, then union with the new
  pull. Newer data replaces overlapping rows, and new minutes are appended.
- Moving averages use a time-range window covering the previous 30 minutes (`rangeBetween(-1800, 0)`)
  over `c, l, h, o, v`. Each average is labelled with the end timestamp of its window.
- The free API tier blocks same-day bars, so the stream replays a completed past session.

## Limitations

- One 30-day window and three tickers is a small sample.
- Daily high and low don't show when a threshold was crossed, so this is an after-the-close screen.
- News, liquidity, positions and exposure are not modelled.
- Neither 3% nor 5% has been tuned against outcomes.
- Provider corrections can change historical values, so reproduce results from the saved files
  rather than from a new download.
