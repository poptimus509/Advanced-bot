# Signal Accuracy Corrections

This version intentionally prioritizes fewer, higher-confluence signals. It does not guarantee profitable trades.

## What changed

- Removed the overlapping RSI bands that favored CALL around RSI 48–52.
- Required mature 1M, 5M and 15M candle histories before evaluation.
- Removed the artificial ADX=25 fallback for incomplete data.
- Uses 15M as a soft macro bias, while 5M structure and 1M EMA/MACD remain mandatory entry confirmations.
- Required the latest closed 1M candle to confirm the proposed direction.
- Rejected stale 1M data before a signal can enter the 16-pair ranking.
- Added strength-based tie-breaking using score, alignment, ADX, EMA separation and MACD acceleration.
- Removed the same-pair cooldown. Every minute all 16 pairs are freshly ranked and the strongest currently qualified setup may be selected again.
- Connected `SIGNALS_ENABLED` and the configured signal threshold to the live dispatcher.
- Removed embedded Telegram and Pusher credentials. Configure them as environment variables.

## Required environment variables

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. If Pusher is needed, also set `PUSHER_APP_ID`, `PUSHER_KEY`, `PUSHER_SECRET`, and optionally `PUSHER_CLUSTER`.

The credentials previously committed to the repository should be rotated before deployment.

## Validation

From the `QuotexSignalBoard` directory run:

```bash
python -m unittest discover -s tests -v
```

Before enabling Telegram, run the bot in observation mode by setting `SIGNALS_ENABLED = False` and compare every pair's evaluation with the same broker/feed used for execution.

## Important data-feed limitation

This project analyzes Deriv candles but labels the output as a Quotex signal. Deriv and Quotex candles can differ. For broker-level accuracy, the analysis feed must match the market and symbol used for execution, especially for OTC instruments.
