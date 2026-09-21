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
        self.tick_handlers = {}
        
        self._req_id_counter = 0
        self._req_lock = threading.Lock()
        self._pending_requests: Dict[int, threading.Event] = {}
        self._request_results: Dict[int, List[dict]] = {}

    @property
    def is_connected(self):
        return self.connected

    def get_server_time(self):
        return int(time.time())

    def fetch_server_epoch_sync(self):
        return int(time.time())

    def _get_next_req_id(self) -> int:
        with self._req_lock:
            self._req_id_counter += 1
            return self._req_id_counter

    def fetch_historical_candles_batch_sync(self, jobs: List[dict]) -> Dict[str, List[dict]]:
        results = {}
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
                self.ws.send(json.dumps(req))
                if event.wait(timeout=4.0):
                    candles = self._request_results.pop(req_id, [])
                    if candles:
                        results[key] = candles
                    else:
                        results[key] = self._fetch_single_history(clean_symbol, count, granularity)
                else:
                    results[key] = self._fetch_single_history(clean_symbol, count, granularity)
            except Exception as e:
                logger.error(f"Error fetching history for {symbol}: {e}")
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
            self.ws.send(json.dumps(req))
            if event.wait(timeout=4.0):
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
        # This client only reads public ticks and candle history. The current
        # public API needs neither an app_id nor legacy authorize messages.
        url = "wss://api.derivws.com/trading/v1/options/ws/public"
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
                        on_close=self.on_close
                    )
                    self.ws.run_forever(
                        ping_interval=25,
                        ping_timeout=10,
                        # Let websocket-client emit exactly one Origin header.
                        # Default TLS certificate and hostname checks stay enabled.
                        origin="https://deriv.com",
                    )
                except Exception as e:
                    logger.error(f"Deriv WebSocket connection error: {e}")

                self.connected = False
                if self.is_running:
                    time.sleep(15)

        threading.Thread(target=run, daemon=True).start()

    def on_open(self, ws):
        logger.info("Connected to Deriv API successfully.")
        self.connected = True
        self.subscribe_symbols(ws)

    def subscribe_symbols(self, ws):
        pairs_to_sub = set()
        if hasattr(cfg, "FOREX_PAIRS") and isinstance(cfg.FOREX_PAIRS, dict):
            pairs_to_sub.update(cfg.FOREX_PAIRS.keys())
        if hasattr(cfg, "ACTIVE_SYMBOLS"):
            pairs_to_sub.update(cfg.ACTIVE_SYMBOLS)

        logger.info(f"DerivClient: Subscribing to live ticks for {len(pairs_to_sub)} pairs.")
        for symbol in pairs_to_sub:
            clean = symbol.replace("frx", "").replace("/", "").upper()
            target_sym = f"frx{clean}"
            req = {"ticks": target_sym, "subscribe": 1}
            try:
                ws.send(json.dumps(req))
            except Exception:
                logger.exception("Failed to subscribe to %s", target_sym)
            time.sleep(0.08)

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            msg_type = data.get("msg_type")
            req_id = data.get("req_id")

            # Deriv errors retain the request's msg_type (e.g. candles or tick).
            # Check the error payload before dispatching by message type.
            if data.get("error"):
                error = data["error"]
                logger.warning("Deriv API error (%s): %s",
                               error.get("code", "unknown"),
                               error.get("message", "Unknown error"))
                if req_id is not None and req_id in self._pending_requests:
                    self._request_results[req_id] = []
                    self._pending_requests[req_id].set()
                return

            if msg_type == "tick":
                tick = data.get("tick")
                if tick:
                    symbol = str(tick.get("symbol", ""))
                    epoch = int(tick.get("epoch", time.time()))
                    quote = float(tick.get("quote", 0.0))

                    self.server_time = epoch
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
        logger.error("Deriv WebSocket error: %s", error)

    def on_close(self, ws, close_status_code, close_msg):
        self.connected = False

    def disconnect(self):
        self.is_running = False
        self.connected = False
        if self.ws:
            self.ws.close()
