import json
import logging
import math
import threading
import time

from typing import Callable, Dict, List, Optional

import websocket

from config import (
    DERIV_APP_ID,
    DERIV_PING_INTERVAL_SECONDS,
    DERIV_RECONNECT_MAX_BACKOFF,
    DERIV_WS_URL,
)


logger = logging.getLogger("DerivClient")


class DerivClient:
    def __init__(
        self,
        app_id: int = DERIV_APP_ID,
        ws_url: str = DERIV_WS_URL,
    ):
        self.app_id = app_id
        self.ws_url = ws_url

        self.ws: Optional[websocket.WebSocketApp] = None
        self._thread = None
        self._watchdog_thread = None

        self._is_running = False
        self.is_connected = False

        self._stop_event = threading.Event()
        self._state_lock = threading.RLock()

        self._tick_subscribers = {}
        self._history_callbacks = {}

        self._backoff = 1
        self._request_counter = 0
        self._time_requests = {}

        self.server_epoch = 0.0
        self.server_epoch_local_time = 0.0
        self._server_anchor_monotonic = 0.0

        self.connected_at_local = 0.0
        self.last_tick_local = 0.0

        self._connected_monotonic = 0.0
        self._last_tick_monotonic = 0.0
        self._last_time_request = 0.0
        self._last_rejection_summary = 0.0

        self._received_symbols = set()
        self._subscription_errors = {}
        self._last_symbol_epoch = {}
        self._last_handler_error = {}

        self.active_symbols = []

    # ========================================================
    # PUBLIC METHODS
    # ========================================================

    def register_tick_handler(
        self,
        symbol: str,
        handler: Callable[[int, float, float], None],
    ):
        with self._state_lock:
            self._tick_subscribers[symbol] = handler

    def start(self):
        with self._state_lock:
            if self._is_running:
                return

            self._is_running = True
            self._stop_event.clear()

            self._thread = threading.Thread(
                target=self._run_loop,
                daemon=True,
                name="DerivSocket",
            )

            self._watchdog_thread = threading.Thread(
                target=self._watchdog_loop,
                daemon=True,
                name="DerivWatchdog",
            )

            self._thread.start()
            self._watchdog_thread.start()

    def stop(self):
        self._is_running = False
        self._stop_event.set()
        self.is_connected = False

        socket = self.ws

        if socket is not None:
            socket.close()

    def get_server_epoch(self) -> int:
        return math.floor(self.get_server_time())

    def get_server_time(self) -> float:
        with self._state_lock:
            if not self._server_anchor_monotonic:
                return time.time()

            elapsed = (
                time.monotonic() - self._server_anchor_monotonic
            )

            return self.server_epoch + elapsed

    # ========================================================
    # SERVER TIME
    # ========================================================

    def _request_server_time(self, ws):
        with self._state_lock:
            self._request_counter += 1
            request_id = self._request_counter
            now = time.monotonic()

            self._time_requests = {
                key: sent
                for key, sent in self._time_requests.items()
                if now - sent < 60
            }

            self._time_requests[request_id] = now
            self._last_time_request = now

        ws.send(json.dumps({
            "time": 1,
            "req_id": request_id,
        }))

    # ========================================================
    # CONNECTION AND RECONNECT
    # ========================================================

    def _run_loop(self):
        while not self._stop_event.is_set():
            with self._state_lock:
                self._received_symbols.clear()
                self._connected_monotonic = 0.0

            try:
                logger.info("Connecting to Deriv WebSocket.")

                ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )

                self.ws = ws

                interval = max(
                    float(DERIV_PING_INTERVAL_SECONDS),
                    15.0,
                )

                ws.run_forever(
                    ping_interval=interval,
                    ping_timeout=10,
                )

            except Exception:
                logger.exception(
                    "Deriv connection loop failed."
                )

            finally:
                self.is_connected = False

            if self._stop_event.is_set():
                break

            with self._state_lock:
                healthy_session = (
                    bool(self._received_symbols)
                    and self._connected_monotonic > 0
                    and (
                        time.monotonic()
                        - self._connected_monotonic
                    ) >= 60
                )

            # A socket opening without ticks does not reset backoff.
            if healthy_session:
                self._backoff = 1

            delay = min(
                self._backoff,
                DERIV_RECONNECT_MAX_BACKOFF,
            )

            logger.warning(
                "Deriv reconnect scheduled in %s seconds.",
                delay,
            )

            if self._stop_event.wait(delay):
                break

            self._backoff = min(
                self._backoff * 2,
                DERIV_RECONNECT_MAX_BACKOFF,
            )

    def _on_open(self, ws):
        if ws is not self.ws:
            return

        with self._state_lock:
            self.is_connected = True

            self.connected_at_local = time.time()
            self._connected_monotonic = time.monotonic()

            self.last_tick_local = 0.0
            self._last_tick_monotonic = 0.0
            self._last_time_request = 0.0
            self._last_rejection_summary = 0.0

            self._received_symbols.clear()
            self._subscription_errors.clear()
            self._last_symbol_epoch.clear()
            self._time_requests.clear()
            self.active_symbols = []

        logger.info(
            "Deriv socket connected; requesting active symbols "
            "and tick subscriptions."
        )

        threading.Thread(
            target=self._subscribe_all,
            args=(ws,),
            daemon=True,
            name="DerivSubscriptions",
        ).start()

    def _subscribe_all(self, ws):
        try:
            self._request_server_time(ws)

            # Diagnostic request: discover symbols available to this
            # connection. Do not guess replacement symbol names.
            ws.send(json.dumps({
                "active_symbols": "brief",
            }))

            logger.info("Active-symbol list requested.")

            with self._state_lock:
                symbols = list(self._tick_subscribers)

            for symbol in symbols:
                if (
                    self._stop_event.is_set()
                    or ws is not self.ws
                    or not self.is_connected
                ):
                    return

                ws.send(json.dumps({
                    "ticks": symbol,
                    "subscribe": 1,
                }))

                logger.info(
                    "Subscription requested: %s",
                    symbol,
                )

                if self._stop_event.wait(0.1):
                    return

        except Exception:
            logger.exception(
                "Deriv subscription sending failed."
            )
            ws.close()

    # ========================================================
    # ACTIVE-SYMBOL DIAGNOSTICS
    # ========================================================

    def _handle_active_symbols(self, data):
        symbols = data.get("active_symbols", [])

        if not isinstance(symbols, list):
            logger.error(
                "Unexpected active_symbols response format."
            )
            return

        symbols = [
            item for item in symbols
            if isinstance(item, dict)
        ]

        with self._state_lock:
            self.active_symbols = symbols
            requested = set(self._tick_subscribers)

        logger.info(
            "ACTIVE_SYMBOL_COUNT=%s",
            len(symbols),
        )

        available_codes = set()

        for item in symbols:
            symbol = item.get("symbol")

            if isinstance(symbol, str):
                available_codes.add(symbol)

            logger.info(
                "AVAILABLE_SYMBOL symbol=%s name=%s market=%s "
                "is_open=%s suspended=%s",
                symbol,
                item.get("display_name"),
                item.get("market"),
                item.get("exchange_is_open"),
                item.get("is_trading_suspended"),
            )

        missing = sorted(requested - available_codes)
        present = sorted(requested & available_codes)

        logger.info(
            "CONFIGURED_SYMBOLS_PRESENT=%s",
            present,
        )

        if missing:
            logger.warning(
                "CONFIGURED_SYMBOLS_NOT_IN_ACTIVE_LIST=%s",
                missing,
            )

        # Being listed does not by itself prove live tick availability.
        # FIRST_TICK logs provide that evidence.

    # ========================================================
    # MESSAGE PROCESSING
    # ========================================================

    def _on_message(self, ws, message):
        if ws is not self.ws:
            return

        receipt_wall = time.time()
        receipt_mono = time.monotonic()

        try:
            data = json.loads(message)

            if not isinstance(data, dict):
                return

            # API errors can retain the original request's msg_type.
            if data.get("error"):
                error = data["error"]
                request = data.get("echo_req") or {}

                if not isinstance(error, dict):
                    logger.error(
                        "Unexpected Deriv API error format."
                    )
                    return

                if not isinstance(request, dict):
                    request = {}

                symbol = request.get("ticks")

                if symbol:
                    with self._state_lock:
                        self._subscription_errors[symbol] = (
                            error.get("code", "UNKNOWN")
                        )

                logger.error(
                    "DERIV_API_ERROR symbol=%s type=%s "
                    "code=%s message=%s",
                    symbol or request.get("ticks_history", "-"),
                    data.get("msg_type"),
                    error.get("code"),
                    error.get("message"),
                )
                return

            message_type = data.get("msg_type")

            if message_type == "active_symbols":
                self._handle_active_symbols(data)
                return

            if message_type == "time":
                epoch = float(data["time"])

                if not math.isfinite(epoch) or epoch <= 0:
                    return

                with self._state_lock:
                    sent = self._time_requests.pop(
                        data.get("req_id"),
                        None,
                    )

                    if sent is None:
                        return

                    round_trip = receipt_mono - sent

                    if round_trip > 5:
                        logger.warning(
                            "Ignoring delayed server-time "
                            "response: %.2fs",
                            round_trip,
                        )
                        return

                    self.server_epoch = epoch + round_trip / 2
                    self.server_epoch_local_time = receipt_wall
                    self._server_anchor_monotonic = receipt_mono

                return

            if message_type == "tick" and data.get("tick"):
                tick = data["tick"]

                symbol = tick.get("symbol")
                quote = float(tick["quote"])
                epoch = int(tick["epoch"])

                if (
                    not symbol
                    or not math.isfinite(quote)
                    or quote <= 0
                    or epoch <= 0
                ):
                    return

                with self._state_lock:
                    previous_epoch = self._last_symbol_epoch.get(
                        symbol, 0
                    )

                    if epoch < previous_epoch:
                        return

                    first = symbol not in self._received_symbols

                    self._received_symbols.add(symbol)
                    self._subscription_errors.pop(symbol, None)
                    self._last_symbol_epoch[symbol] = epoch

                    self.last_tick_local = receipt_wall
                    self._last_tick_monotonic = receipt_mono

                    handler = self._tick_subscribers.get(symbol)

                if first:
                    logger.info(
                        "FIRST_TICK symbol=%s epoch=%s quote=%s",
                        symbol,
                        epoch,
                        quote,
                    )

                # Do not adjust the server clock from tick timestamps.
                if handler:
                    try:
                        handler(
                            epoch,
                            quote,
                            receipt_wall,
                        )

                    except Exception:
                        last_error = self._last_handler_error.get(
                            symbol, 0
                        )

                        if receipt_mono - last_error >= 30:
                            self._last_handler_error[symbol] = (
                                receipt_mono
                            )

                            logger.exception(
                                "TICK_HANDLER_ERROR symbol=%s",
                                symbol,
                            )

                return

            if message_type == "candles":
                request = data.get("echo_req") or {}
                symbol = str(
                    request.get("ticks_history", "")
                )

                callback = self._history_callbacks.get(symbol)

                if callback:
                    callback(data)

        except Exception:
            logger.exception(
                "Invalid or unhandled Deriv response."
            )

    def _on_error(self, ws, error):
        if ws is self.ws:
            logger.error(
                "Deriv WebSocket error: %s",
                error,
            )

    def _on_close(self, ws, close_status_code, close_msg):
        if ws is self.ws:
            self.is_connected = False

        logger.warning(
            "Deriv socket closed: code=%s reason=%s",
            close_status_code,
            close_msg,
        )

    # ========================================================
    # CONNECTION WATCHDOG
    # ========================================================

    def _watchdog_loop(self):
        while not self._stop_event.wait(5):
            ws = self.ws

            if not self.is_connected or ws is None:
                continue

            try:
                now = time.monotonic()

                if now - self._last_time_request >= 25:
                    self._request_server_time(ws)

                with self._state_lock:
                    reference = (
                        self._last_tick_monotonic
                        or self._connected_monotonic
                    )

                    rejected = dict(self._subscription_errors)
                    total = len(self._tick_subscribers)
                    received_count = len(self._received_symbols)

                if not reference or now - reference <= 45:
                    continue

                if total and len(rejected) == total:
                    # Keep API errors visible instead of reconnecting
                    # continuously to the same rejected symbols.
                    if now - self._last_rejection_summary >= 30:
                        self._last_rejection_summary = now

                        logger.error(
                            "All tick subscriptions rejected: %s. "
                            "Check AVAILABLE_SYMBOL logs.",
                            rejected,
                        )

                    continue

                logger.warning(
                    "NO_LIVE_TICKS for %.1fs; reconnecting. "
                    "Received symbols=%s; rejected=%s",
                    now - reference,
                    received_count,
                    rejected,
                )

                ws.close()

            except Exception:
                logger.exception(
                    "Deriv watchdog failed."
                )

                if ws is self.ws:
                    ws.close()

    # ========================================================
    # SYNCHRONOUS SERVER TIME
    # ========================================================

    def fetch_server_epoch_sync(self) -> int:
        socket = None

        try:
            socket = websocket.create_connection(
                self.ws_url,
                timeout=10,
            )

            socket.send(json.dumps({
                "time": 1,
                "req_id": 1,
            }))

            deadline = time.monotonic() + 10

            while time.monotonic() < deadline:
                remaining = deadline - time.monotonic()

                if remaining <= 0:
                    break

                socket.settimeout(remaining)
                payload = json.loads(socket.recv())

                if not isinstance(payload, dict):
                    continue

                if payload.get("error"):
                    logger.error(
                        "Server-time API error: %s",
                        payload["error"],
                    )
                    break

                if payload.get("msg_type") == "time":
                    epoch = int(payload["time"])

                    if epoch > 0:
                        return epoch

        except Exception as exc:
            logger.warning(
                "Server-time fetch failed: %s",
                exc,
            )

        finally:
            if socket is not None:
                socket.close()

        return self.get_server_epoch()

    # ========================================================
    # HISTORICAL CANDLES
    # ========================================================

    def fetch_historical_candles_batch_sync(
        self,
        requests_list: List[dict],
    ) -> Dict[str, List[dict]]:
        results = {
            item["key"]: []
            for item in requests_list
        }

        if not requests_list:
            return results

        socket = None

        try:
            socket = websocket.create_connection(
                self.ws_url,
                timeout=10,
            )

            pending = {}

            for request_id, item in enumerate(
                requests_list,
                start=1,
            ):
                pending[request_id] = item

                socket.send(json.dumps({
                    "ticks_history": item["symbol"],
                    "adjust_start_time": 1,
                    "count": int(item["count"]),
                    "end": "latest",
                    "granularity": int(item["granularity"]),
                    "style": "candles",
                    "req_id": request_id,
                }))

            deadline = time.monotonic() + 25

            while pending:
                remaining = deadline - time.monotonic()

                if remaining <= 0:
                    break

                socket.settimeout(min(remaining, 5))

                try:
                    payload = json.loads(socket.recv())

                except websocket.WebSocketTimeoutException:
                    continue

                if not isinstance(payload, dict):
                    continue

                request_id = payload.get("req_id")
                item = pending.pop(request_id, None)

                if item is None:
                    continue

                if payload.get("error"):
                    error = payload["error"]

                    logger.error(
                        "HISTORY_API_ERROR key=%s "
                        "code=%s message=%s",
                        item["key"],
                        error.get("code"),
                        error.get("message"),
                    )
                    continue

                candles = payload.get("candles")

                if isinstance(candles, list):
                    results[item["key"]] = candles

            if pending:
                logger.warning(
                    "History requests timed out: %s",
                    [
                        item["key"]
                        for item in pending.values()
                    ],
                )

        except Exception as exc:
            logger.error(
                "Historical batch failed: %s",
                exc,
            )

        finally:
            if socket is not None:
                socket.close()

        return results

    def fetch_historical_candles_sync(
        self,
        symbol: str,
        count: int = 120,
        granularity: int = 60,
    ) -> List[dict]:
        key = f"{symbol}:{granularity}"

        results = self.fetch_historical_candles_batch_sync([
            {
                "key": key,
                "symbol": symbol,
                "count": count,
                "granularity": granularity,
            }
        ])

        return results.get(key, [])
