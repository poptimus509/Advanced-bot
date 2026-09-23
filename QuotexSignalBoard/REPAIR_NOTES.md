# M1 structure and signal reliability update

New signals expire at the next M1 boundary (60 seconds from the target candle open), not 60 seconds after the user receives Telegram. Historical signals retain their saved expiry.

## Entry rules

- Threshold: 8/8. Four components contribute two points each: aligned M1 EMA trend and confirmed structure; recent candle pressure; RSI momentum; support/resistance clearance.
- CALL requires HH/HL and bullish M1 EMA21/EMA50 alignment. PUT requires LH/LL and bearish alignment. Missing, mixed or conflicting structure means no trade.
- Structure uses the latest 60 M1 bars with two bars on each side confirming a pivot. Pressure uses the latest five closed M1 bars (at least three directional bodies, directional net price/high/low movement and a confirming close).
- Support/resistance uses confirmed pivots and prior extrema over 30 M1 bars. An opposing level within 0.25 ATR blocks entry; a closed breakout beyond that level can pass.
- Normally M5 must align and ADX must be at least 20. A strong M1 override also permits missing, neutral or opposing M5 context, or unavailable/low ADX. It requires all four score components plus at least two strong bars in the last three, the latest strong, the last two directional, and at least 0.5 ATR directional movement over the three bars. Strong bars have a directional body at least 60% of range and close in the outer 25%.
- Candle pressure is an OHLC proxy, not measured buyer/seller order flow. Tick counts remain informational, not trade volume. Telegram and evaluations disclose the M5 override.
- The previous RSI pullback-zone score has been replaced by directional RSI recovery/momentum. RSI can leave the pullback zone; zero-loss/zero-gain edge cases are handled correctly.

## Reliability

- Cooldown uses tuple-compatible access and expires after ten minutes.
- ADX downward movement and warm-up are corrected; incomplete warm-up is unavailable, not an artificial zero.
- Signal history is written before Telegram. Delivery status is recorded separately; failed/unknown delivery does not count as a settled trade or trigger the pair cooldown. Ambiguous sends are not automatically resent.
- M1 quotes must be live and entry must remain inside the configured five-second window. Historical refresh cannot replace a live quote.
- The outcome worker uses each record's expiry, retaining old 5M outcomes. History includes delivery state.
- The pair performance filter uses a rolling 24-hour window, avoiding permanent bans based on old losses. A recent underperforming pair can still be paused.
- Partial M5 candles and duplicate REST/live M5 epochs are excluded. Late ticks cannot rewrite the current M1 candle.
- Dashboard history loads independently of performance; scan reasons and delivery errors are visible. Scores are displayed out of eight.

## Validation

Run from QuotexSignalBoard after installing requirements:

```sh
python -m unittest discover -s tests -v
```

Tests cover both trade directions, mixed structure, M5 override conditions, support/resistance blocking, indicators, cooldown expiry, history/HTTP routes, delivery failure, deduplication, old/new expiry settlement and candle integrity. These are correctness tests, not a profitability backtest.

## Hosting requirement

This patch does not change the hosting plan. A sleeping service cannot scan prices. Local SQLite is not durable on an ephemeral deployment filesystem. For continuous operation use an always-on instance and set DB_PATH to a mounted persistent disk, or migrate storage to an external database. Use a single Gunicorn worker for this in-process scanner. Do not clear the database to reset filters; preserve the history and adjust the relevant settings instead.

No broker trades are placed by this application. Prices and WIN/LOSS records are hypothetical Deriv references and may differ from Quotex.
