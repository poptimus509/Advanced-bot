# tools/

Standalone analysis scripts. These are NOT part of the running bot and are
never imported or called by bot.py / monitor.py - nothing here runs
automatically on Render.

## backtest_deriv.py

Walk-forward backtest of strategy.py against real historical Deriv candles.
See the module docstring at the top of the file for full details. Reports:
threshold win rates (with train/test split), per-component contribution,
ATR-volatility tercile breakdown, hour-of-day (UTC session) breakdown, and
optional multi-pair aggregation.

Run manually (locally, or from Render's Shell tab):

    pip install -r ../requirements.txt   # already includes pandas, numpy, websocket-client

    # single pair
    python backtest_deriv.py --bot-dir .. --symbol EURUSD --weeks 6

    # multiple pairs aggregated (more reliable, slower to fetch)
    python backtest_deriv.py --bot-dir .. --symbols EURUSD,GBPJPY,USDJPY,AUDUSD --weeks 6

Results print to the terminal. Add --output-csv results.csv to also save
every replayed candle's score/outcome for your own further analysis. Add
--use-cache on repeat runs to skip re-fetching from Deriv.
