import json
import logging
import websocket
import threading
import time
from typing import Dict, List, Any, Optional
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
        self.connected = False
        self.authorized = False
        self.tick_handlers = {}
        self.last_tick_wall_time = time.time()
        self._watchdog_started = False
        self._poller_started = False
        
        # Concurrency & request tracking
        self._req_id_counter = 0
        self._req_lock = threading.Lock()
        self._pending_requests: Dict[int, threading.Event] = {}
        self._request_results: Dict[int, List[dict]] = {}

    @property
    def is_connected(self):
        return self.connected

    def get_server_time(self):
        # Use the latest broker epoch for candle boundaries after first tick.
        return int(self.server_time or time.time())

    def fetch_server_epoch_sync(self):
        return int(self.server_time or time.time())

    def _get_next_req_id(self) -> int:
        with self._req_lock:
            self._req_id_counter += 1
            return self._req_id_counter

    def fetch_historical_candles_batch_sync(self, jobs: List[dict]) -> Dict[str, List[dict]]:
        results = {}
        # A watchdog reconnect can happen between any two history requests.
        # Never send a request through the old/closed WebSocket.
        deadline = time.time() + 12.0
        while time.time() < deadline and (not self.connected or not self.ws):
            time.sleep(0.25)
        if not self.ws or not self.connected:
            return {job.get("key", ""): [] for job in jobs}

        for job in jobs:
            key = job.get("key")
            symbol = job.get("symbol", key)
            count = job.get("count", 100)
            granularity = job.get("granularity", 60)

            clean_symbol = symbol.replace("frx", "").replace("/", "").upper()
            target_sym = f"frx{clean_symbol}"

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
                "req_id": req_id
            }

            try:
                ws = self.ws
                if not self.connected or ws is None:
                    results[key] = []
                    continue
                ws.send(json.dumps(req))
                if event.wait(timeout=3.0):
                    candles = self._request_results.pop(req_id, [])
                    if candles:
                        results[key] = candles
                    else:
                        results[key] = []
                else:
                    results[key] = []
            except Exception as e:
                logger.warning("History request skipped for %s; WebSocket changed state: %s", symbol, e)
                results[key] = []
            finally:
                self._pending_requests.pop(req_id, None)
                self._request_results.pop(req_id, None)

        return results

    def _fetch_single_history(self, symbol: str, count: int, granularity: int) -> List[dict]:
        req_id = self._get_next_req_id()
        event = threading.Event()
        self._pending_requests[req_id] = event
        req = {
            "ticks_history": symbol,
            "adjust_start_time": 1,
            "count": count,
            "end": "latest",
            "granularity": granularity,
            "style": "candles",
            "req_id": req_id
        }
        try:
            ws = self.ws
            if not self.connected or ws is None:
                return []
            ws.send(json.dumps(req))
            if event.wait(timeout=3.0):
                return self._request_results.pop(req_id, [])
        except Exception:
            pass
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

        if not self._watchdog_started:
            self._watchdog_started = True
            threading.Thread(
                target=self._tick_watchdog,
                daemon=True,
                name="DerivTickWatchdog",
            ).start()

        if not self._poller_started:
            self._poller_started = True
            threading.Thread(
                target=self._history_fallback_loop,
                daemon=True,
                name="DerivHistoryFallback",
            ).start()

        def run():
            while self.is_running:
                try:
                    logger.info("Connecting to Deriv WebSocket API...")
                    self.ws = websocket.WebSocketApp(
                        url,
                        on_open=self.on_open,
                        on_message=self.on_message,
                        on_error=self.on_error,
                        on_close=self.on_close
                    )
                    self.ws.run_forever(ping_interval=20, ping_timeout=10)
                except Exception as e:
                    logger.error(f"Deriv WebSocket connection error: {e}")

                self.connected = False
                if self.is_running:
                    time.sleep(5)

        threading.Thread(target=run, daemon=True).start()

    def _tick_watchdog(self):
        while self.is_running:
            time.sleep(5)
            if not self.connected:
                continue

            silence = time.time() - self.last_tick_wall_time
            if silence > 20:
                logger.warning(
                    "No Deriv tick received for %.1f seconds; reconnecting WebSocket.",
                    silence,
                )
                self.connected = False
                try:
                    if self.ws:
                        self.ws.close()
                except Exception:
                    pass

    def _history_fallback_loop(self):
        """Keep candle managers alive when Deriv tick subscriptions go silent."""
        while self.is_running:
            if not self.connected or not self.ws:
                time.sleep(2.0)
                continue

            symbols = set()
            if isinstance(getattr(cfg, "FOREX_PAIRS", None), dict):
                symbols.update(cfg.FOREX_PAIRS.keys())
            symbols.update(getattr(cfg, "ACTIVE_SYMBOLS", []))

            for symbol in symbols:
                if not self.is_running or not self.connected or not self.ws:
                    break
                clean = symbol.replace("frx", "").replace("/", "").upper()
                req_id = self._get_next_req_id()
                event = threading.Event()
                self._pending_requests[req_id] = event
                request = {
                    "ticks_history": f"frx{clean}",
                    "adjust_start_time": 1,
                    "count": 2,
                    "end": "latest",
                    "granularity": 60,
                    "style": "candles",
                    "req_id": req_id,
                }
                try:
                    self.ws.send(json.dumps(request))
                    if event.wait(timeout=2.0):
                        candles = self._request_results.pop(req_id, [])
                        if candles:
                            candle = candles[-1]
                            candle_epoch = int(candle.get("epoch", candle.get("time", 0)))
                            close = float(candle.get("close", 0.0))
                            if candle_epoch and close > 0:
                                # Feed the candle close as a synthetic quote.
                                # CandleManager will close the prior candle when
                                # the next candle boundary is reached.
                                handler = (
                                    self.tick_handlers.get(f"frx{clean}")
                                    or self.tick_handlers.get(clean)
                                )
                                if handler:
                                    handler(candle_epoch + 59, close, time.monotonic())
                except Exception as exc:
                    logger.debug("History fallback failed for %s: %s", symbol, exc)
                finally:
                    self._pending_requests.pop(req_id, None)
                    self._request_results.pop(req_id, None)
                time.sleep(0.12)

            time.sleep(3.0)

    def on_open(self, ws):
        logger.info("Connected to Deriv API successfully.")
        self.connected = True
        self.authorized = False
        self.last_tick_wall_time = time.time()
        api_token = getattr(cfg, "API_TOKEN", None)
        if api_token:
            auth_req = {"authorize": api_token}
            try:
                ws.send(json.dumps(auth_req))
            except Exception:
                pass

        else:
            self.subscribe_symbols(ws)

    def subscribe_symbols(self, ws):
        # Merge symbols from both FOREX_PAIRS and ACTIVE_SYMBOLS
        pairs_to_sub = set()
        if hasattr(cfg, "FOREX_PAIRS") and isinstance(cfg.FOREX_PAIRS, dict):
            pairs_to_sub.update(cfg.FOREX_PAIRS.keys())
        if hasattr(cfg, "ACTIVE_SYMBOLS"):
            pairs_to_sub.update(cfg.ACTIVE_SYMBOLS)

        logger.info(f"DerivClient: Subscribing to live ticks for {len(pairs_to_sub)} pairs.")
        for symbol in pairs_to_sub:
            clean = symbol.replace("frx", "").replace("/", "").upper()
            # Deriv Forex instruments must use the frx-prefixed symbol.
            # Sending both frxSYMBOL and bare SYMBOL creates invalid/duplicate
            # subscriptions and can leave the stream silent after reconnect.
            target_sym = f"frx{clean}"
            req = {"ticks": target_sym, "subscribe": 1}
            try:
                ws.send(json.dumps(req))
            except Exception as exc:
                logger.warning("Tick subscription failed for %s: %s", target_sym, exc)
            time.sleep(0.08)

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            msg_type = data.get("msg_type")
            req_id = data.get("req_id")

            if msg_type == "authorize":
                self.authorized = True
                logger.info("Deriv authorization successful; subscribing to live ticks.")
                self.subscribe_symbols(ws)
                return

            if msg_type == "error":
                err = data.get("error", {})
                logger.error(
                    "Deriv API error: %s - %s",
                    err.get("code", "UNKNOWN"),
                    err.get("message", data),
                )

            if msg_type == "tick":
                tick = data.get("tick")
                if tick:
                    self.last_tick_wall_time = time.time()
                    symbol = str(tick.get("symbol", ""))
                    epoch = int(tick.get("epoch", time.time()))
                    quote = float(tick.get("quote", 0.0))

                    self.server_time = epoch
                    clean = symbol.replace("frx", "").replace("/", "").upper()

                    # Find corresponding handler
                    handler = (
                        self.tick_handlers.get(symbol)
                        or self.tick_handlers.get(clean)
                        or self.tick_handlers.get(f"frx{clean}")
                        or self.tick_handlers.get(f"{clean[:3]}/{clean[3:]}")
                    )

                    if handler:
                        try:
                            handler(epoch, quote, time.monotonic())
                        except Exception as e:
                            logger.error(f"Error in tick handler for {symbol}: {e}")

                    if self.on_tick_callback:
                        self.on_tick_callback(tick)

            elif msg_type == "candles":
                candles_raw = data.get("candles", [])
                formatted_candles = []
                for c in candles_raw:
                    t_val = int(c.get("epoch") or c.get("time") or 0)
                    formatted_candles.append({
                        "epoch": t_val,
                        "time": t_val,
                        "open": float(c.get("open", 0.0)),
                        "high": float(c.get("high", 0.0)),
                        "low": float(c.get("low", 0.0)),
                        "close": float(c.get("close", 0.0)),
                        "ticks_count": int(c.get("count", 1))
                    })

                if req_id is not None and req_id in self._pending_requests:
                    self._request_results[req_id] = formatted_candles
                    self._pending_requests[req_id].set()

            elif msg_type == "error":
                if req_id is not None and req_id in self._pending_requests:
                    self._request_results[req_id] = []
                    self._pending_requests[req_id].set()

        except Exception as e:
            logger.error(f"Error processing Deriv message: {e}")

    def on_error(self, ws, error):
        self.connected = False

    def on_close(self, ws, close_status_code, close_msg):
        self.connected = False

    def disconnect(self):
        self.is_running = False
        self.connected = False
        if self.ws:
            self.ws.close()
