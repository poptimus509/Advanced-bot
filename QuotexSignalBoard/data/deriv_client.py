import json
import logging
import threading
import time
from typing import Any, Dict, List

import websocket

import config as cfg

logger = logging.getLogger("QuotexSignalBoard")


class DerivClient:

    def __init__(
        self,
        on_tick_callback=None,
        on_candle_callback=None,
    ):
        self.on_tick_callback = on_tick_callback
        self.on_candle_callback = on_candle_callback

        self.ws = None
        self.is_running = False

        self.subscribed_symbols = set()

        self.server_time = int(time.time())
        self._server_time_receipt_monotonic = (
            time.monotonic()
        )

        self.connected = False

        self.tick_handlers = {}

        # ------------------------------------------------------
        # Live feed diagnostics
        # ------------------------------------------------------

        self._diag_lock = threading.RLock()

        self._tick_rx_total = 0

        self._tick_rx_by_symbol: Dict[str, int] = {}

        self._last_tick_by_symbol: Dict[str, dict] = {}

        self._subscription_status: Dict[str, dict] = {}

        self._last_api_error: Dict[str, Any] = {}

        # ------------------------------------------------------
        # Request tracking
        # ------------------------------------------------------

        self._req_id_counter = 0
        self._req_lock = threading.Lock()

        self._pending_requests: Dict[
            int,
            threading.Event,
        ] = {}

        self._request_results: Dict[
            int,
            Any,
        ] = {}

    @property
    def is_connected(self):
        return self.connected

    # ==========================================================
    # SERVER TIME
    # ==========================================================

    def _set_server_time(
        self,
        epoch: int,
    ):
        try:
            epoch = int(epoch)

            if epoch > 0:
                self.server_time = epoch
                self._server_time_receipt_monotonic = (
                    time.monotonic()
                )

        except Exception:
            pass

    def get_server_time(self):

        age = (
            time.monotonic()
            - self._server_time_receipt_monotonic
        )

        if age <= 120:

            return int(
                self.server_time
                + max(
                    0.0,
                    age,
                )
            )

        return int(time.time())

    def fetch_server_epoch_sync(self):

        if not self.ws or not self.connected:
            return self.get_server_time()

        req_id = self._get_next_req_id()

        event = threading.Event()

        self._pending_requests[
            req_id
        ] = event

        try:

            self.ws.send(
                json.dumps(
                    {
                        "time": 1,
                        "req_id": req_id,
                    }
                )
            )

            if event.wait(timeout=2.0):

                value = self._request_results.pop(
                    req_id,
                    None,
                )

                if (
                    isinstance(
                        value,
                        (int, float),
                    )
                    and value > 0
                ):

                    self._set_server_time(
                        int(value)
                    )

                    return int(value)

        except Exception:

            logger.debug(
                "Deriv server-time request failed",
                exc_info=True,
            )

        finally:

            self._pending_requests.pop(
                req_id,
                None,
            )

            self._request_results.pop(
                req_id,
                None,
            )

        return self.get_server_time()

    # ==========================================================
    # HELPERS
    # ==========================================================

    def _get_next_req_id(self) -> int:

        with self._req_lock:

            self._req_id_counter += 1

            return self._req_id_counter

    @staticmethod
    def _deriv_symbol(
        symbol: str,
    ) -> str:

        clean = (
            str(symbol)
            .replace("frx", "")
            .replace("/", "")
            .upper()
        )

        return f"frx{clean}"

    # ==========================================================
    # HISTORICAL CANDLES
    # ==========================================================

    def fetch_historical_candles_batch_sync(
        self,
        jobs: List[dict],
        timeout: float = 3.0,
        allow_fallback: bool = True,
    ) -> Dict[str, List[dict]]:

        results: Dict[
            str,
            List[dict],
        ] = {}

        if not self.ws or not self.connected:

            return {
                job.get(
                    "key",
                    "",
                ): []
                for job in jobs
            }

        pending = []

        for job in jobs:

            key = job.get("key")

            symbol = job.get(
                "symbol",
                key,
            )

            count = int(
                job.get(
                    "count",
                    100,
                )
            )

            granularity = int(
                job.get(
                    "granularity",
                    60,
                )
            )

            target_sym = (
                self._deriv_symbol(
                    symbol
                )
            )

            req_id = (
                self._get_next_req_id()
            )

            event = threading.Event()

            self._pending_requests[
                req_id
            ] = event

            req = {
                "ticks_history":
                    target_sym,
                "adjust_start_time": 1,
                "count": count,
                "end": "latest",
                "granularity":
                    granularity,
                "style": "candles",
                "req_id": req_id,
            }

            try:

                self.ws.send(
                    json.dumps(req)
                )

                pending.append(
                    (
                        key,
                        target_sym,
                        count,
                        granularity,
                        req_id,
                        event,
                    )
                )

            except Exception as exc:

                logger.error(
                    "Error requesting history for %s: %s",
                    symbol,
                    exc,
                )

                results[key] = []

                self._pending_requests.pop(
                    req_id,
                    None,
                )

                self._request_results.pop(
                    req_id,
                    None,
                )

        deadline = (
            time.monotonic()
            + max(
                0.25,
                float(timeout),
            )
        )

        failed = []

        for (
            key,
            target_sym,
            count,
            granularity,
            req_id,
            event,
        ) in pending:

            try:

                remaining = max(
                    0.0,
                    deadline
                    - time.monotonic(),
                )

                if (
                    event.is_set()
                    or (
                        remaining > 0
                        and event.wait(
                            timeout=remaining
                        )
                    )
                ):

                    candles = (
                        self._request_results.pop(
                            req_id,
                            [],
                        )
                    )

                    if (
                        isinstance(
                            candles,
                            list,
                        )
                        and candles
                    ):

                        results[key] = candles

                    else:

                        results[key] = []

                        failed.append(
                            (
                                key,
                                target_sym,
                                count,
                                granularity,
                            )
                        )

                else:

                    results[key] = []

                    failed.append(
                        (
                            key,
                            target_sym,
                            count,
                            granularity,
                        )
                    )

            finally:

                self._pending_requests.pop(
                    req_id,
                    None,
                )

                self._request_results.pop(
                    req_id,
                    None,
                )

        if allow_fallback:

            for (
                key,
                target_sym,
                count,
                granularity,
            ) in failed:

                fallback = (
                    self._fetch_single_history(
                        target_sym,
                        count,
                        granularity,
                    )
                )

                if fallback:
                    results[key] = fallback

        for job in jobs:

            results.setdefault(
                job.get(
                    "key",
                    "",
                ),
                [],
            )

        return results

    def _fetch_single_history(
        self,
        symbol: str,
        count: int,
        granularity: int,
    ) -> List[dict]:

        target_sym = (
            self._deriv_symbol(
                symbol
            )
        )

        req_id = (
            self._get_next_req_id()
        )

        event = threading.Event()

        self._pending_requests[
            req_id
        ] = event

        req = {
            "ticks_history":
                target_sym,
            "adjust_start_time": 1,
            "count": int(count),
            "end": "latest",
            "granularity":
                int(granularity),
            "style": "candles",
            "req_id": req_id,
        }

        try:

            self.ws.send(
                json.dumps(req)
            )

            if event.wait(timeout=3.0):

                result = (
                    self._request_results.pop(
                        req_id,
                        [],
                    )
                )

                return (
                    result
                    if isinstance(
                        result,
                        list,
                    )
                    else []
                )

        except Exception:

            logger.debug(
                "Fallback history request failed for %s",
                target_sym,
                exc_info=True,
            )

        finally:

            self._pending_requests.pop(
                req_id,
                None,
            )

            self._request_results.pop(
                req_id,
                None,
            )

        return []

    # ==========================================================
    # TICK HANDLER REGISTRATION
    # ==========================================================

    def register_tick_handler(
        self,
        symbol: str,
        handler,
    ):

        clean = (
            symbol
            .replace(
                "frx",
                "",
            )
            .replace(
                "/",
                "",
            )
            .upper()
        )

        variants = [
            symbol,
            clean,
            f"frx{clean}",
            (
                f"{clean[:3]}/{clean[3:]}"
                if len(clean) == 6
                else clean
            ),
        ]

        for variant in variants:
            self.tick_handlers[
                variant
            ] = handler

    # ==========================================================
    # CONNECTION
    # ==========================================================

    def start(self):
        self.connect()

    def connect(self):

        app_id = getattr(
            cfg,
            "APP_ID",
            1089,
        )

        url = (
            "wss://ws.derivws.com/"
            f"websockets/v3?app_id={app_id}"
        )

        self.is_running = True

        def run():

            while self.is_running:

                try:

                    logger.info(
                        "Connecting to Deriv WebSocket API..."
                    )

                    self.ws = (
                        websocket.WebSocketApp(
                            url,
                            on_open=self.on_open,
                            on_message=self.on_message,
                            on_error=self.on_error,
                            on_close=self.on_close,
                        )
                    )

                    self.ws.run_forever(
                        ping_interval=20,
                        ping_timeout=10,
                    )

                except Exception as exc:

                    logger.error(
                        "Deriv WebSocket connection error: %s",
                        exc,
                    )

                self.connected = False

                if self.is_running:
                    time.sleep(5)

        threading.Thread(
            target=run,
            daemon=True,
        ).start()

    def on_open(
        self,
        ws,
    ):

        logger.info(
            "Connected to Deriv API successfully."
        )

        self.connected = True

        api_token = getattr(
            cfg,
            "API_TOKEN",
            None,
        )

        if api_token:

            try:

                ws.send(
                    json.dumps(
                        {
                            "authorize":
                                api_token,
                        }
                    )
                )

            except Exception:

                logger.debug(
                    "Authorization request could not be sent",
                    exc_info=True,
                )

        self.subscribe_symbols(ws)

        try:

            ws.send(
                json.dumps(
                    {
                        "time": 1
                    }
                )
            )

        except Exception:
            pass

    # ==========================================================
    # LIVE TICK SUBSCRIPTION
    # ==========================================================

    def subscribe_symbols(
        self,
        ws,
    ):

        pairs_to_sub = set()

        if (
            hasattr(
                cfg,
                "FOREX_PAIRS",
            )
            and isinstance(
                cfg.FOREX_PAIRS,
                dict,
            )
        ):

            pairs_to_sub.update(
                cfg.FOREX_PAIRS.keys()
            )

        if hasattr(
            cfg,
            "ACTIVE_SYMBOLS",
        ):

            pairs_to_sub.update(
                cfg.ACTIVE_SYMBOLS
            )

        logger.info(
            "DerivClient: Subscribing to live ticks for %s pairs.",
            len(pairs_to_sub),
        )

        for symbol in sorted(
            pairs_to_sub
        ):

            target_sym = (
                self._deriv_symbol(
                    symbol
                )
            )

            try:

                ws.send(
                    json.dumps(
                        {
                            "ticks":
                                target_sym,
                            "subscribe": 1,
                        }
                    )
                )

                with self._diag_lock:

                    state = (
                        self._subscription_status
                        .setdefault(
                            target_sym,
                            {},
                        )
                    )

                    state.update(
                        {
                            "requested_at":
                                int(
                                    time.time()
                                ),
                            "last_request_monotonic":
                                time.monotonic(),
                            "request_count":
                                int(
                                    state.get(
                                        "request_count",
                                        0,
                                    )
                                ) + 1,
                            "last_error":
                                None,
                        }
                    )

            except Exception as exc:

                with self._diag_lock:

                    state = (
                        self._subscription_status
                        .setdefault(
                            target_sym,
                            {},
                        )
                    )

                    state[
                        "last_error"
                    ] = str(exc)

                logger.exception(
                    "Failed to subscribe to %s",
                    target_sym,
                )

            time.sleep(0.04)

    # ==========================================================
    # DIAGNOSTICS
    # ==========================================================

    def diagnostics(self):

        now_mono = time.monotonic()

        with self._diag_lock:

            symbols = {}

            all_keys = (
                set(
                    self._subscription_status
                )
                | set(
                    self._last_tick_by_symbol
                )
            )

            for sym in sorted(
                all_keys
            ):

                sub = dict(
                    self._subscription_status.get(
                        sym,
                        {},
                    )
                )

                tick = dict(
                    self._last_tick_by_symbol.get(
                        sym,
                        {},
                    )
                )

                receipt = tick.get(
                    "receipt_monotonic"
                )

                symbols[sym] = {
                    "subscription_requests":
                        int(
                            sub.get(
                                "request_count",
                                0,
                            )
                        ),
                    "subscription_last_error":
                        sub.get(
                            "last_error"
                        ),
                    "tick_count":
                        int(
                            self._tick_rx_by_symbol.get(
                                sym,
                                0,
                            )
                        ),
                    "last_tick_epoch":
                        tick.get(
                            "epoch"
                        ),
                    "last_tick_quote":
                        tick.get(
                            "quote"
                        ),
                    "receipt_age_seconds":
                        (
                            round(
                                now_mono
                                - receipt,
                                3,
                            )
                            if receipt
                            else None
                        ),
                }

            return {
                "connected":
                    bool(
                        self.connected
                    ),
                "tick_rx_total":
                    int(
                        self._tick_rx_total
                    ),
                "last_api_error":
                    dict(
                        self._last_api_error
                    ),
                "symbols":
                    symbols,
            }

    # ==========================================================
    # RESUBSCRIBE STALE STREAMS
    # ==========================================================

    def resubscribe_stale_symbols(
        self,
        stale_seconds: float = 45.0,
    ):

        if (
            not self.ws
            or not self.connected
        ):
            return []

        now_mono = time.monotonic()

        requested = []

        pairs = set()

        if (
            hasattr(
                cfg,
                "FOREX_PAIRS",
            )
            and isinstance(
                cfg.FOREX_PAIRS,
                dict,
            )
        ):

            pairs.update(
                cfg.FOREX_PAIRS.keys()
            )

        if hasattr(
            cfg,
            "ACTIVE_SYMBOLS",
        ):

            pairs.update(
                cfg.ACTIVE_SYMBOLS
            )

        for symbol in sorted(
            pairs
        ):

            target = (
                self._deriv_symbol(
                    symbol
                )
            )

            with self._diag_lock:

                tick = (
                    self._last_tick_by_symbol.get(
                        target
                    )
                )

                sub = (
                    self._subscription_status.get(
                        target,
                        {},
                    )
                )

                receipt = (
                    tick.get(
                        "receipt_monotonic"
                    )
                    if tick
                    else None
                )

                last_req = sub.get(
                    "last_request_monotonic",
                    0.0,
                )

                stale = (
                    receipt is None
                    or (
                        now_mono
                        - receipt
                    )
                    > stale_seconds
                )

                request_old_enough = (
                    (
                        now_mono
                        - last_req
                    )
                    > min(
                        15.0,
                        stale_seconds,
                    )
                )

            if not (
                stale
                and request_old_enough
            ):
                continue

            try:

                self.ws.send(
                    json.dumps(
                        {
                            "ticks":
                                target,
                            "subscribe": 1,
                        }
                    )
                )

                with self._diag_lock:

                    state = (
                        self._subscription_status
                        .setdefault(
                            target,
                            {},
                        )
                    )

                    state.update(
                        {
                            "requested_at":
                                int(
                                    time.time()
                                ),
                            "last_request_monotonic":
                                now_mono,
                            "request_count":
                                int(
                                    state.get(
                                        "request_count",
                                        0,
                                    )
                                ) + 1,
                        }
                    )

                requested.append(
                    target
                )

            except Exception as exc:

                with self._diag_lock:

                    (
                        self._subscription_status
                        .setdefault(
                            target,
                            {},
                        )
                    )[
                        "last_error"
                    ] = str(exc)

        return requested

    # ==========================================================
    # MESSAGE HANDLER
    # ==========================================================

    def on_message(
        self,
        ws,
        message,
    ):

        try:

            data = json.loads(
                message
            )

            msg_type = data.get(
                "msg_type"
            )

            req_id = data.get(
                "req_id"
            )

            # --------------------------------------------------
            # API ERROR
            # --------------------------------------------------

            if data.get("error"):

                error = (
                    data.get("error")
                    or {}
                )

                echo = (
                    data.get("echo_req")
                    or {}
                )

                target = str(
                    echo.get("ticks")
                    or echo.get(
                        "ticks_history"
                    )
                    or ""
                )

                with self._diag_lock:

                    self._last_api_error = {
                        "code":
                            error.get(
                                "code"
                            ),
                        "message":
                            error.get(
                                "message"
                            ),
                        "target":
                            target or None,
                        "time":
                            int(
                                time.time()
                            ),
                    }

                    if target:

                        (
                            self._subscription_status
                            .setdefault(
                                target,
                                {},
                            )
                        )[
                            "last_error"
                        ] = error.get(
                            "message"
                        )

                logger.warning(
                    "Deriv API error target=%s code=%s message=%s",
                    target
                    or "UNKNOWN",
                    error.get(
                        "code"
                    ),
                    error.get(
                        "message",
                        error,
                    ),
                )

                if (
                    req_id is not None
                    and req_id
                    in self._pending_requests
                ):

                    self._request_results[
                        req_id
                    ] = []

                    self._pending_requests[
                        req_id
                    ].set()

                return

            # --------------------------------------------------
            # LIVE TICK
            # --------------------------------------------------

            if msg_type == "tick":

                tick = data.get(
                    "tick"
                )

                if tick:

                    symbol = str(
                        tick.get(
                            "symbol",
                            "",
                        )
                    )

                    epoch = int(
                        tick.get(
                            "epoch",
                            time.time(),
                        )
                    )

                    quote = float(
                        tick.get(
                            "quote",
                            0.0,
                        )
                    )

                    self._set_server_time(
                        epoch
                    )

                    with self._diag_lock:

                        self._tick_rx_total += 1

                        self._tick_rx_by_symbol[
                            symbol
                        ] = (
                            self._tick_rx_by_symbol.get(
                                symbol,
                                0,
                            )
                            + 1
                        )

                        first_tick = (
                            symbol
                            not in
                            self._last_tick_by_symbol
                        )

                        self._last_tick_by_symbol[
                            symbol
                        ] = {
                            "epoch":
                                epoch,
                            "quote":
                                quote,
                            "receipt_monotonic":
                                time.monotonic(),
                        }

                        sub = (
                            self._subscription_status
                            .setdefault(
                                symbol,
                                {},
                            )
                        )

                        sub[
                            "last_error"
                        ] = None

                    if (
                        first_tick
                        and getattr(
                            cfg,
                            "LIVE_TICK_DIAGNOSTICS",
                            False,
                        )
                    ):

                        logger.info(
                            "TICK_RX first symbol=%s epoch=%s quote=%s",
                            symbol,
                            epoch,
                            quote,
                        )

                    clean = (
                        symbol
                        .replace(
                            "frx",
                            "",
                        )
                        .replace(
                            "/",
                            "",
                        )
                        .upper()
                    )

                    handler = (
                        self.tick_handlers.get(
                            symbol
                        )
                        or self.tick_handlers.get(
                            clean
                        )
                        or self.tick_handlers.get(
                            f"frx{clean}"
                        )
                        or self.tick_handlers.get(
                            (
                                f"{clean[:3]}/"
                                f"{clean[3:]}"
                            )
                        )
                    )

                    if handler:

                        try:

                            handler(
                                epoch,
                                quote,
                                time.monotonic(),
                            )

                        except Exception as exc:

                            logger.error(
                                "Error in tick handler for %s: %s",
                                symbol,
                                exc,
                            )

                    elif getattr(
                        cfg,
                        "LIVE_TICK_DIAGNOSTICS",
                        False,
                    ):

                        logger.warning(
                            "TICK_HANDLER_MISSING symbol=%s normalized=%s",
                            symbol,
                            clean,
                        )

                    if self.on_tick_callback:

                        self.on_tick_callback(
                            tick
                        )

            # --------------------------------------------------
            # HISTORICAL CANDLES
            # --------------------------------------------------

            elif msg_type == "candles":

                candles_raw = data.get(
                    "candles",
                    [],
                )

                formatted_candles = []

                for candle in candles_raw:

                    t_val = int(
                        candle.get("epoch")
                        or candle.get("time")
                        or 0
                    )

                    formatted_candles.append(
                        {
                            "epoch":
                                t_val,
                            "time":
                                t_val,
                            "open":
                                float(
                                    candle.get(
                                        "open",
                                        0.0,
                                    )
                                ),
                            "high":
                                float(
                                    candle.get(
                                        "high",
                                        0.0,
                                    )
                                ),
                            "low":
                                float(
                                    candle.get(
                                        "low",
                                        0.0,
                                    )
                                ),
                            "close":
                                float(
                                    candle.get(
                                        "close",
                                        0.0,
                                    )
                                ),
                            "ticks_count":
                                int(
                                    candle.get(
                                        "count",
                                        1,
                                    )
                                ),
                        }
                    )

                if (
                    req_id is not None
                    and req_id
                    in self._pending_requests
                ):

                    self._request_results[
                        req_id
                    ] = formatted_candles

                    self._pending_requests[
                        req_id
                    ].set()

            # --------------------------------------------------
            # SERVER TIME
            # --------------------------------------------------

            elif msg_type == "time":

                epoch = int(
                    data.get("time")
                    or 0
                )

                if epoch > 0:

                    self._set_server_time(
                        epoch
                    )

                if (
                    req_id is not None
                    and req_id
                    in self._pending_requests
                ):

                    self._request_results[
                        req_id
                    ] = epoch

                    self._pending_requests[
                        req_id
                    ].set()

            # --------------------------------------------------
            # FALLBACK ERROR TYPE
            # --------------------------------------------------

            elif msg_type == "error":

                if (
                    req_id is not None
                    and req_id
                    in self._pending_requests
                ):

                    self._request_results[
                        req_id
                    ] = []

                    self._pending_requests[
                        req_id
                    ].set()

                error = (
                    data.get("error")
                    or {}
                )

                logger.warning(
                    "Deriv API error: %s",
                    error.get(
                        "message",
                        error,
                    ),
                )

        except Exception as exc:

            logger.error(
                "Error processing Deriv message: %s",
                exc,
            )

    # ==========================================================
    # CONNECTION EVENTS
    # ==========================================================

    def on_error(
        self,
        ws,
        error,
    ):

        self.connected = False

        logger.warning(
            "Deriv WebSocket error: %s",
            error,
        )

    def on_close(
        self,
        ws,
        close_status_code,
        close_msg,
    ):

        self.connected = False

        logger.warning(
            "Deriv WebSocket closed: code=%s message=%s",
            close_status_code,
            close_msg,
        )

    def disconnect(self):

        self.is_running = False

        self.connected = False

        if self.ws:
            self.ws.close()
