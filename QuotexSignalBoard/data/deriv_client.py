import hashlib
import math
import random
import threading
import time
from typing import Dict, List

import config as cfg


class DerivClient:
    """
    Offline synthetic market-data client.

    IMPORTANT:
    - No external WebSocket
    - No broker API
    - No orders
    - No account access
    - No real-money execution

    Existing project compatibility-এর জন্য class/file name
    unchanged রাখা হয়েছে।
    """

    BASE_PRICES = {
        "EURUSD": 1.17000,
        "GBPUSD": 1.34000,
        "USDJPY": 147.000,
        "USDCHF": 0.79000,
        "AUDUSD": 0.66000,
        "USDCAD": 1.38000,
        "NZDUSD": 0.59000,
        "EURJPY": 172.000,
        "GBPJPY": 197.000,
        "EURGBP": 0.87000,
        "EURCHF": 0.92500,
        "GBPCHF": 1.06000,
        "AUDJPY": 97.000,
        "CADJPY": 106.000,
        "CHFJPY": 186.000,
        "AUDCAD": 0.91000,
    }

    def __init__(
        self,
        on_tick_callback=None,
        on_candle_callback=None,
    ):
        self.on_tick_callback = (
            on_tick_callback
        )

        self.on_candle_callback = (
            on_candle_callback
        )

        self.tick_handlers = {}

        self.connected = False
        self.is_running = False

        self._lock = threading.RLock()

        self._prices: Dict[str, float] = {}

        self._tick_count: Dict[str, int] = {}

        self._last_tick: Dict[str, dict] = {}

        self._rng: Dict[str, random.Random] = {}

        self._thread = None

        for symbol in cfg.FOREX_PAIRS:
            self._prices[symbol] = (
                self.BASE_PRICES.get(
                    symbol,
                    1.0,
                )
            )

            self._tick_count[symbol] = 0

            self._rng[symbol] = (
                random.Random(
                    self._stable_seed(
                        symbol
                    )
                )
            )

    @staticmethod
    def _stable_seed(symbol):
        digest = hashlib.sha256(
            symbol.encode("utf-8")
        ).digest()

        return int.from_bytes(
            digest[:8],
            "big",
        )

    @property
    def is_connected(self):
        return self.connected

    @staticmethod
    def _clean_symbol(symbol):
        return (
            str(symbol)
            .replace("frx", "")
            .replace("/", "")
            .upper()
        )

    def _deriv_symbol(self, symbol):
        # Compatibility helper only.
        # No external provider symbol is used.
        return self._clean_symbol(
            symbol
        )

    def register_tick_handler(
        self,
        symbol,
        handler,
    ):
        clean = self._clean_symbol(
            symbol
        )

        self.tick_handlers[
            clean
        ] = handler

    def get_server_time(self):
        return int(time.time())

    def fetch_server_epoch_sync(self):
        return int(time.time())

    # ========================================================
    # SYNTHETIC LIVE FEED
    # ========================================================

    def start(self):
        if self.is_running:
            return

        self.is_running = True
        self.connected = True

        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="SyntheticMarketFeed",
        )

        self._thread.start()

    def disconnect(self):
        self.is_running = False
        self.connected = False

    def _volatility_for(
        self,
        symbol,
    ):
        if symbol.endswith("JPY"):
            return 0.008

        return 0.00006

    def _run(self):
        interval = float(
            getattr(
                cfg,
                "SIM_TICK_INTERVAL_SECONDS",
                0.5,
            )
        )

        while self.is_running:
            now_epoch = int(
                time.time()
            )

            receipt = time.monotonic()

            for symbol in cfg.FOREX_PAIRS:
                rng = self._rng[
                    symbol
                ]

                previous = self._prices[
                    symbol
                ]

                volatility = (
                    self._volatility_for(
                        symbol
                    )
                )

                # Zero-drift random walk.
                movement = rng.gauss(
                    0.0,
                    volatility,
                )

                price = (
                    previous
                    + movement
                )

                if (
                    not math.isfinite(price)
                    or price <= 0
                ):
                    price = previous

                self._prices[
                    symbol
                ] = price

                with self._lock:
                    self._tick_count[
                        symbol
                    ] += 1

                    self._last_tick[
                        symbol
                    ] = {
                        "epoch":
                            now_epoch,
                        "quote":
                            price,
                        "receipt":
                            receipt,
                    }

                handler = (
                    self.tick_handlers.get(
                        symbol
                    )
                )

                if handler:
                    try:
                        handler(
                            now_epoch,
                            price,
                            receipt,
                        )
                    except Exception:
                        pass

                if self.on_tick_callback:
                    try:
                        self.on_tick_callback(
                            {
                                "symbol":
                                    symbol,
                                "epoch":
                                    now_epoch,
                                "quote":
                                    price,
                            }
                        )
                    except Exception:
                        pass

            time.sleep(
                max(
                    0.1,
                    interval,
                )
            )

    # ========================================================
    # HISTORICAL SYNTHETIC CANDLES
    # ========================================================

    def _make_history(
        self,
        symbol,
        count,
        granularity,
    ):
        symbol = self._clean_symbol(
            symbol
        )

        count = max(
            20,
            int(count),
        )

        granularity = max(
            60,
            int(granularity),
        )

        now = int(
            time.time()
        )

        current_boundary = (
            now // granularity
        ) * granularity

        first_epoch = (
            current_boundary
            - count * granularity
        )

        seed_text = (
            f"{symbol}:"
            f"{first_epoch}:"
            f"{granularity}"
        )

        rng = random.Random(
            self._stable_seed(
                seed_text
            )
        )

        price = self.BASE_PRICES.get(
            symbol,
            1.0,
        )

        volatility = (
            self._volatility_for(
                symbol
            )
        )

        candles = []

        for i in range(count):
            epoch = (
                first_epoch
                + i * granularity
            )

            open_price = price

            # Multiple internal moves make OHLC
            # more realistic than one random number.
            path = [
                open_price
            ]

            internal_steps = 12

            for _ in range(
                internal_steps
            ):
                price += rng.gauss(
                    0.0,
                    volatility * 2.5,
                )

                path.append(
                    price
                )

            close_price = path[-1]

            high_price = max(
                path
            )

            low_price = min(
                path
            )

            candles.append(
                {
                    "epoch":
                        epoch,
                    "time":
                        epoch,
                    "open":
                        float(
                            open_price
                        ),
                    "high":
                        float(
                            high_price
                        ),
                    "low":
                        float(
                            low_price
                        ),
                    "close":
                        float(
                            close_price
                        ),
                    "ticks_count":
                        internal_steps,
                }
            )

        # Last generated historical price
        # becomes initial synthetic live price.
        if candles:
            with self._lock:
                self._prices[
                    symbol
                ] = float(
                    candles[-1][
                        "close"
                    ]
                )

        return candles

    def fetch_historical_candles_batch_sync(
        self,
        jobs: List[dict],
        timeout=3.0,
        allow_fallback=True,
    ):
        del timeout
        del allow_fallback

        output = {}

        for job in jobs:
            key = job.get(
                "key",
                "",
            )

            symbol = job.get(
                "symbol",
                key,
            )

            count = int(
                job.get(
                    "count",
                    1000,
                )
            )

            granularity = int(
                job.get(
                    "granularity",
                    60,
                )
            )

            output[key] = (
                self._make_history(
                    symbol,
                    count,
                    granularity,
                )
            )

        return output

    # ========================================================
    # DIAGNOSTICS
    # ========================================================

    def diagnostics(self):
        now = time.monotonic()

        with self._lock:
            symbols = {}

            for symbol in cfg.FOREX_PAIRS:
                last = (
                    self._last_tick.get(
                        symbol
                    )
                )

                symbols[symbol] = {
                    "subscription_requests":
                        0,
                    "subscription_last_error":
                        None,
                    "tick_count":
                        int(
                            self._tick_count.get(
                                symbol,
                                0,
                            )
                        ),
                    "last_tick_epoch":
                        (
                            last.get(
                                "epoch"
                            )
                            if last
                            else None
                        ),
                    "last_tick_quote":
                        (
                            last.get(
                                "quote"
                            )
                            if last
                            else None
                        ),
                    "receipt_age_seconds":
                        (
                            round(
                                now
                                - last[
                                    "receipt"
                                ],
                                3,
                            )
                            if last
                            else None
                        ),
                }

        return {
            "mode":
                "OFFLINE_SYNTHETIC",
            "connected":
                bool(
                    self.connected
                ),
            "tick_rx_total":
                sum(
                    self._tick_count.values()
                ),
            "last_api_error":
                {},
            "symbols":
                symbols,
        }

    def resubscribe_stale_symbols(
        self,
        stale_seconds=45.0,
    ):
        del stale_seconds

        # No subscriptions exist in
        # offline simulation.
        return []
