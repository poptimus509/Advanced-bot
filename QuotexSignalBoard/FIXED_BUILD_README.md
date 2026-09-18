# Advanced Bot — correctness/diagnostic build

This package is a full copy of the application, prepared for replacing the existing GitHub project files.

## What changed

- Fixed DMI/ADX directional-movement math and Wilder smoothing.
- Unified RSI/ATR calculations around Wilder-style smoothing and corrected RSI zero-loss/zero-gain edge cases.
- Deriv clock now uses Deriv server timestamps when available instead of always returning the machine clock.
- Historical fallback requests always normalize forex symbols to the `frx...` form.
- Historical candle seeding is idempotent: REST history and live candles cannot create duplicate epochs.
- 5M/15M history is rebuilt from complete, aligned 1M blocks at cold start, so partial higher-timeframe candles are not created.
- A historical REST candle is no longer inserted into `live_state` and treated as a fresh quote.
- Telegram dispatch is blocked if a genuinely fresh live tick is unavailable.
- The scanner now distinguishes:
  - `NO_1M_HISTORY`
  - `INSUFFICIENT_RAW_1M_HISTORY`
  - `INSUFFICIENT_CONTIGUOUS_1M_HISTORY`
  - `LATEST_CLOSED_CANDLE_MISSING`
- Added `HISTORY_DIAG ...` log lines with raw/prepared counts and timestamps.
- Added `/api/history-diagnostics` for browser-visible per-pair candle diagnostics.
- Tick-count windows start at 1 and are not marked complete unless the next minute proves the window was observed from its beginning.
- Added data-pipeline regression tests.
- Added `.gitignore`; runtime SQLite DB is intentionally not included.

## What was deliberately NOT changed

`SIGNAL_THRESHOLD_CALL_PUT` remains `6`. This build fixes data/correctness problems; it does not claim to improve profitability or win rate.

The repository's noise-rejection guard still fails with the current strategy. That is retained as a visible warning rather than hidden by weakening the test.

## Render

Recommended build command:

```bash
pip install -r requirements.txt
```

Recommended start command:

```bash
gunicorn --workers 1 --threads 8 --timeout 120 --bind 0.0.0.0:$PORT bot:app
```

Use one Gunicorn worker because each worker would otherwise start its own scanner threads.

## After deployment

Open:

`/api/history-diagnostics`

For every pair, check these fields:

- `manager_1m.count`
- `raw_count_before_prepare`
- `prepared_count`
- `prepared_last_epoch`
- `latest_candle_exact`

The Render logs will also show `History EURUSD: ...` and, when a pair is rejected, `HISTORY_DIAG EURUSD ...`.
