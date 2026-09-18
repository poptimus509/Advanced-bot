# Corrections (v2)

The previous version of this file described a strategy the code did not
actually implement (the shipped `strategy.py` never read 15M data, never
used ADX, and `tests/test_strategy.py` imported functions that did not
exist in `strategy.py` at all). This file only describes what the code
in this repository actually does right now. If you change `strategy.py`,
update this file in the same commit, or it will rot again.

## What this bot is, honestly

A rule-based scanner over 16 forex pairs' 1-minute Deriv candles that
posts a Telegram message when its scoring rule fires. It does not
learn, does not backtest, and does not know anything about win
probability beyond what `/api/performance` measures after the fact
from its own hypothetical-outcome tracking. Treat every signal as a
rule match, not a prediction with a known accuracy.

## Strategy scoring (`strategy.py`)

Four independent components, each worth up to 2 points (max 8):

1. **Market structure** - confirmed swing pivots (`confirmed_swings`),
   a non-repainting fractal detector. A pivot at bar `i` only depends on
   the 2 bars on either side of it, so it can never be silently
   rewritten once later bars arrive.
2. **1M EMA stack** - `close > ema9 > ema21 > ema50` (or the mirrored
   bearish case).
3. **RSI momentum** - trend-following only. The old version also
   awarded points to the *opposite* side on RSI ≤25 / ≥75 ("oversold
   reversal"), which fought directly against the structure and EMA
   components above. That branch has been removed.
4. **Candle reaction** - the latest closed candle must actually confirm
   the direction (`candle_reaction`): a bearish candle can never confirm
   a bullish setup, full stop.

`SIGNAL_THRESHOLD_CALL_PUT = 8` means all four must agree. If CALL and
PUT evidence tie, the result is `NO_TRADE` - there is no forced
tie-break in either direction (the previous version defaulted every tie
to CALL).

### Optional gates (no score contribution)

- **5M regime** (`_safe_5m_bias`): only used when a real 5M OHLC
  dataframe with at least `CONTEXT_5M_MIN_CANDLES` rows is supplied by
  `bot.py`. If the candidate direction conflicts with the 5M EMA21/50
  regime, the signal is blocked. Genuinely missing/short 5M history
  skips this gate rather than faking a result - the dashboard field
  `trend_5m` says `UNAVAILABLE` in that case instead of quietly
  reporting a number derived from 1M data (which is what the previous
  `detect_m5_trend` did, despite its name).
- **ADX(5M) floor** (`MIN_ADX_5M`, default 20): if `bot.py` was able to
  compute a 5M ADX(14) reading and it's below this, the signal is
  blocked as too choppy to trust.

Both gates are defensive: malformed or missing inputs are ignored and
produce the exact same result as if they were never passed
(`tests/test_strategy.py::test_higher_timeframes_have_no_effect`).

## Calibration, not vibes

`tests/test_noise_rejection.py` runs the strategy against pure
zero-drift geometric random walks and asserts the signal rate stays
under 15%. Before this fix, the same test on the previous scoring logic
returned a ~50% signal rate on pure noise - i.e. it was close to a coin
flip. Re-run this test after any change to `strategy.py`'s scoring; if
the rate creeps back up, something reintroduced double-counting or
dropped a real filter.

This test does **not** prove the strategy is profitable. It only proves
it isn't trivially over-triggering on data with no real trend in it.

## Data-feed limitation (unchanged, still real)

This project analyzes **Deriv** candles but the Telegram message is
explicit that it's a Deriv-feed signal. Deriv and Quotex candles can and
do differ, especially for OTC instruments. For broker-level accuracy,
the analysis feed must match the market and symbol actually used for
execution. This bot cannot guarantee that, and says so in every message.

## Data pipeline fixes

- `CandleManager.seed_historical_candles` no longer clears
  `_closed_candles` / the forming candle on every REST resync. It
  previously ran once a minute and wiped every live tick-built candle
  each time, meaning the WebSocket tick pipeline was only ever "in
  charge" for a few seconds after each resync. It now merges in only
  candles it doesn't already have and never touches the forming candle.
- `live_quote()` now rejects quotes older than `STALE_TICK_THRESHOLD_SEC`
  instead of silently falling back to a >120-second-old closed-candle
  price and calling it "live".
- The scan window that decides how late in a minute a signal can still
  be dispatched is now bounded by `MAX_ENTRY_DELAY_SECONDS` (was
  effectively 45 seconds into the minute, which meant "entry price"
  could be captured most of the way through the candle it was
  supposedly entering at the open of).

## Reporting fixes

- `monitor.calculate_win_rate` now counts ties in the denominator
  (`wins / (wins + losses + ties)`). The previous version excluded
  ties, which inflates the displayed win rate.
- `database.get_db_connection` opens with a busy timeout and WAL mode.
  16 pairs re-opening short-lived SQLite connections every scan cycle
  produced real `database is locked` failures under the old default
  connection settings.
- `dispatch_ledger_v2` is now pruned on a rolling 24h window instead of
  growing forever.

## Config

`config.py` now only contains values that are actually read somewhere
in the code. The previous file had roughly 15 settings (ATR zone
widths, cooldown minutes, alignment-requirement flags, etc.) that
looked configurable but were never referenced by any module - changing
them did nothing. If you add a new setting, grep for it after wiring it
in, or it will silently become another one of these.

## Deploying on Render

**Build Command:**
```bash
pip install -r requirements.txt
```

**Start Command:**
```bash
gunicorn --workers 1 --threads 8 --timeout 120 --bind 0.0.0.0:$PORT bot:app
```

Two details that matter and are easy to get wrong:

- **`--workers 1` is mandatory, not a performance suggestion.** The scan
  loop, Deriv WebSocket connection, and in-memory candle history all
  live inside the same Python process as background threads
  (`start_background_threads_once()`, called once at module import).
  Each gunicorn *worker* is a separate process with its own copy of all
  of that state. Run more than one worker and you get that many
  independent scanners, each capable of sending its own Telegram
  message for the same signal - the SQLite dispatch ledger's primary
  key will block some duplicate DB rows, but it cannot stop duplicate
  Deriv connections or duplicate Telegram sends from separate
  processes. `--threads 8` (not more workers) is how this app should
  scale request handling for the dashboard/API routes.
- Background threads now start at **module import time**, not on the
  first incoming HTTP request. Render's own health checks hit the app
  almost immediately after boot, which made the old
  first-request-triggered version look correct in testing while still
  being fragile - a deploy that received zero traffic would never
  start scanning at all.

**Environment variables to set in the Render dashboard** (Settings →
Environment): `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, and if Pusher is
used, `PUSHER_APP_ID`, `PUSHER_KEY`, `PUSHER_SECRET`, `PUSHER_CLUSTER`.
Do not commit any of these to the repo. `DB_PATH` defaults to
`signal_board.db` in the working directory; on Render's ephemeral
filesystem this resets on every deploy/restart unless you attach a
persistent disk and point `DB_PATH` at a file on it.

## Required environment variables

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. If Pusher is needed,
also set `PUSHER_APP_ID`, `PUSHER_KEY`, `PUSHER_SECRET`, and optionally
`PUSHER_CLUSTER`. Any credentials that were ever committed to this
repository in the past should be rotated before deploying.

## Validation

From the `QuotexSignalBoard` directory:

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

All tests (including the noise-rejection guard-rail) must pass before
merging any change to `strategy.py`.

Before enabling Telegram delivery, run with `SIGNALS_ENABLED = False`
for a while and compare `/api/evaluations` against the same broker/feed
you intend to actually trade on.
