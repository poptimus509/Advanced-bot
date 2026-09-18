import json
import logging
import threading
import time
from typing import Any, Dict, List

import websocket

import config as cfg

logger = logging.getLogger("QuotexSignalBoard")


class DerivClient:
    def __init__(self, on_tick_callback=None, on_candle_callback=None):
        self.on_tick_callback = on_tick_callback
        self.on_candle_callback = on_candle_callback
        self.ws = None
        self.is_running = False
        self.subscribed_symbols = set()
        self.server_time = int(time.time())
        self._server_time_receipt_monotonic = time.monotonic()
        self.connected = False
        self.tick_handlers = {}

        # Concurrency & request tracking
        self._req_id_counter = 0
        self._req_lock = threading.Lock()
        self._pending_requests: Dict[int, threading.Event] = {}
        self._request_results: Dict[int, Any] = {}

    @property
    def is_connected(self):
        return self.connected

    def _set_server_time(self, epoch: int):
        try:
            epoch = int(epoch)
            if epoch > 0:
                self.server_time = epoch
                self._server_time_receipt_monotonic = time.monotonic()
        except Exception:
            pass

    def get_server_time(self):
        """
        Return an advancing estimate based on the last Deriv server epoch.
        Falls back to system UTC only if a server timestamp has not been
        refreshed for a long time.
        """
        age = time.monotonic() - self._server_time_receipt_monotonic
        if age <= 120:
            return int(self.server_time + max(0.0, age))
        return int(time.time())

    def fetch_server_epoch_sync(self):
        """Fetch Deriv's current server epoch when connected."""
        if not self.ws or not self.connected:
            return self.get_server_time()

        req_id = self._get_next_req_id()
        event = threading.Event()
        self._pending_requests[req_id] = event
        try:
            self.ws.send(json.dumps({"time": 1, "req_id": req_id}))
            if event.wait(timeout=2.0):
                value = self._request_results.pop(req_id, None)
                if isinstance(value, (int, float)) and value > 0:
                    self._set_server_time(int(value))
                    return int(value)
        except Exception:
            logger.debug("Deriv server-time request failed", exc_info=True)
        finally:
            self._pending_requests.pop(req_id, None)
            self._request_results.pop(req_id, None)

        return self.get_server_time()

    def _get_next_req_id(self) -> int:
        with self._req_lock:
            self._req_id_counter += 1
            return self._req_id_counter

    @staticmethod
    def _deriv_symbol(symbol: str) -> str:
        clean = str(symbol).replace("frx", "").replace("/", "").upper()
        return f"frx{clean}"

    def fetch_historical_candles_batch_sync(self, jobs: List[dict]) -> Dict[str, List[dict]]:
        results: Dict[str, List[dict]] = {}
        if not self.ws or not self.connected:
            return {job.get("key", ""): [] for job in jobs}

        for job in jobs:
            key = job.get("key")
            symbol = job.get("symbol", key)
            count = int(job.get("count", 100))
            granularity = int(job.get("granularity", 60))
            target_sym = self._deriv_symbol(symbol)

            req_id = self._get_next_req_id()
            event = threading.Event()
            self._pending_requests[req_id] = event

            req = {
                "ticks_history": target_sym,
                "adjust_start_time": 1,
                "count": count,
                "end": "latest",
                "granularity": granularity,
                "style": "candles",
                "req_id": req_id,
            }

            try:
                self.ws.send(json.dumps(req))
                if event.wait(timeout=3.0):
                    candles = self._request_results.pop(req_id, [])
                    if isinstance(candles, list) and candles:
                        results[key] = candles
                    else:
                        results[key] = self._fetch_single_history(target_sym, count, granularity)
                else:
                    results[key] = self._fetch_single_history(target_sym, count, granularity)
            except Exception as exc:
                logger.error("Error fetching history for %s: %s", symbol, exc)
                results[key] = []
            finally:
                self._pending_requests.pop(req_id, None)
                self._request_results.pop(req_id, None)

        return results

    def _fetch_single_history(self, symbol: str, count: int, granularity: int) -> List[dict]:
        """
        Fallback history request.  Normalize the symbol here as well so the
        fallback can never accidentally send bare EURUSD instead of the
        Deriv symbol frxEURUSD.
        """
        target_sym = self._deriv_symbol(symbol)
        req_id = self._get_next_req_id()
        event = threading.Event()
        self._pending_requests[req_id] = event
        req = {
            "ticks_history": target_sym,
            "adjust_start_time": 1,
            "count": int(count),
            "end": "latest",
            "granularity": int(granularity),
            "style": "candles",
            "req_id": req_id,
        }
        try:
            self.ws.send(json.dumps(req))
            if event.wait(timeout=3.0):
                result = self._request_results.pop(req_id, [])
                return result if isinstance(result, list) else []
        except Exception:
            logger.debug("Fallback history request failed for %s", target_sym, exc_info=True)
        finally:
            self._pending_requests.pop(req_id, None)
            self._request_results.pop(req_id, None)
        return []

    def register_tick_handler(self, symbol: str, handler):
        clean = symbol.replace("frx", "").replace("/", "").upper()
        for variant in [symbol, clean, f"frx{clean}", f"{clean[:3]}/{clean[3:]}"]:
            self.tick_handlers[variant] = handler

    def start(self):
        self.connect()

    def connect(self):
        app_id = getattr(cfg, "APP_ID", 1089)
        url = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"
        self.is_running = True

        def run():
            while self.is_running:
                try:
                    logger.info("Connecting to Deriv WebSocket API...")
                    self.ws = websocket.WebSocketApp(
                        url,
                        on_open=self.on_open,
                        on_message=self.on_message,
                        on_error=self.on_error,
                        on_close=self.on_close,
                    )
                    self.ws.run_forever(ping_interval=20, ping_timeout=10)
                except Exception as exc:
                    logger.error("Deriv WebSocket connection error: %s", exc)

                self.connected = False
                if self.is_running:
                    time.sleep(5)

        threading.Thread(target=run, daemon=True).start()

    def on_open(self, ws):
        logger.info("Connected to Deriv API successfully.")
        self.connected = True
        api_token = getattr(cfg, "API_TOKEN", None)
        if api_token:
            try:
                ws.send(json.dumps({"authorize": api_token}))
            except Exception:
                logger.debug("Authorization request could not be sent", exc_info=True)

        self.subscribe_symbols(ws)
        # Refresh the clock independently of tick arrival.
        try:
            ws.send(json.dumps({"time": 1}))
        except Exception:
            pass

    def subscribe_symbols(self, ws):
        pairs_to_sub = set()
        if hasattr(cfg, "FOREX_PAIRS") and isinstance(cfg.FOREX_PAIRS, dict):
            pairs_to_sub.update(cfg.FOREX_PAIRS.keys())
        if hasattr(cfg, "ACTIVE_SYMBOLS"):
            pairs_to_sub.update(cfg.ACTIVE_SYMBOLS)

        logger.info("DerivClient: Subscribing to live ticks for %s pairs.", len(pairs_to_sub))
        for symbol in pairs_to_sub:
            target_sym = self._deriv_symbol(symbol)
            try:
                ws.send(json.dumps({"ticks": target_sym, "subscribe": 1}))
            except Exception:
                logger.exception("Failed to subscribe to %s", target_sym)
            time.sleep(0.04)

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            msg_type = data.get("msg_type")
            req_id = data.get("req_id")

            if msg_type == "tick":
                tick = data.get("tick")
                if tick:
                    symbol = str(tick.get("symbol", ""))
                    epoch = int(tick.get("epoch", time.time()))
                    quote = float(tick.get("quote", 0.0))
                    self._set_server_time(epoch)

                    clean = symbol.replace("frx", "").replace("/", "").upper()
                    handler = (
                        self.tick_handlers.get(symbol)
                        or self.tick_handlers.get(clean)
                        or self.tick_handlers.get(f"frx{clean}")
                        or self.tick_handlers.get(f"{clean[:3]}/{clean[3:]}")
                    )

                    if handler:
                        try:
                            handler(epoch, quote, time.monotonic())
                        except Exception as exc:
                            logger.error("Error in tick handler for %s: %s", symbol, exc)

                    if self.on_tick_callback:
                        self.on_tick_callback(tick)

            elif msg_type == "candles":
                candles_raw = data.get("candles", [])
                formatted_candles = []
                for candle in candles_raw:
                    t_val = int(candle.get("epoch") or candle.get("time") or 0)
                    formatted_candles.append(
                        {
                            "epoch": t_val,
                            "time": t_val,
                            "open": float(candle.get("open", 0.0)),
                            "high": float(candle.get("high", 0.0)),
                            "low": float(candle.get("low", 0.0)),
                            "close": float(candle.get("close", 0.0)),
                            "ticks_count": int(candle.get("count", 1)),
                        }
                    )

                if req_id is not None and req_id in self._pending_requests:
                    self._request_results[req_id] = formatted_candles
                    self._pending_requests[req_id].set()

            elif msg_type == "time":
                epoch = int(data.get("time") or 0)
                if epoch > 0:
                    self._set_server_time(epoch)
                if req_id is not None and req_id in self._pending_requests:
                    self._request_results[req_id] = epoch
                    self._pending_requests[req_id].set()

            elif msg_type == "error":
                if req_id is not None and req_id in self._pending_requests:
                    self._request_results[req_id] = []
                    self._pending_requests[req_id].set()
                error = data.get("error") or {}
                logger.warning("Deriv API error: %s", error.get("message", error))

        except Exception as exc:
            logger.error("Error processing Deriv message: %s", exc)

    def on_error(self, ws, error):
        self.connected = False
        logger.warning("Deriv WebSocket error: %s", error)

    def on_close(self, ws, close_status_code, close_msg):
        self.connected = False
        logger.warning("Deriv WebSocket closed: code=%s message=%s", close_status_code, close_msg)

    def disconnect(self):
        self.is_running = False
        self.connected = False
        if self.ws:
            self.ws.close()
