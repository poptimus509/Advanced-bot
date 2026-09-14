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
    SERVER_SYNC_TOLERANCE_SEC,
)

logger = logging.getLogger("DerivClient")

class DerivClient:
    def __init__(self, app_id: int = DERIV_APP_ID, ws_url: str = DERIV_WS_URL):
        self.app_id = app_id
        self.ws_url = ws_url
        self.ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._is_running = False
        self.is_connected = False
        
        self.server_epoch: int = 0
        self.server_epoch_local_time: float = 0.0

        self._tick_subscribers: Dict[str, Callable[[int, float, float], None]] = {}
        self._history_callbacks: Dict[str, Callable[[dict], None]] = {}
        self._backoff = 1

    def register_tick_handler(self, symbol: str, handler: Callable[[int, float, float], None]):
        self._tick_subscribers[symbol] = handler

    def start(self):
        self._is_running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._is_running = False
        if self.ws:
            self.ws.close()

    def get_server_epoch(self) -> int:
        return math.floor(self.get_server_time())

    def get_server_time(self) -> float:
        """Return Deriv-aligned time with sub-second precision."""
        if self.server_epoch == 0:
            return time.time()
        elapsed = time.time() - self.server_epoch_local_time
        return self.server_epoch + elapsed

    def _run_loop(self):
        logger.info("=== Deriv WebSocket background thread loop started ===")
        while self._is_running:
            try:
                logger.info(f"Attempting to connect to Deriv WS at {self.ws_url}...")
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                self.ws.run_forever(ping_interval=DERIV_PING_INTERVAL_SECONDS, ping_timeout=10)
            except Exception as e:
                logger.error(f"CRITICAL WebSocket execution fault: {e}", exc_info=True)

            if not self._is_running:
                break

            self.is_connected = False
            sleep_time = min(self._backoff, DERIV_RECONNECT_MAX_BACKOFF)
            logger.info(f"Reconnecting to Deriv in {sleep_time}s (exponential backoff)...")
            time.sleep(sleep_time)
            self._backoff = min(self._backoff * 2, DERIV_RECONNECT_MAX_BACKOFF)

    def _on_open(self, ws):
        self.is_connected = True
        self._backoff = 1
        logger.info("Deriv WebSocket connection established. Synchronizing server time & subscriptions...")
        
        ws.send(json.dumps({"time": 1}))
        time.sleep(0.5)

        for symbol in self._tick_subscribers.keys():
            sub_req = {"ticks": symbol, "subscribe": 1}
            ws.send(json.dumps(sub_req))
            logger.info(f"Sent tick subscription for {symbol}")
            time.sleep(0.1)

    def _on_message(self, ws, message):
        local_receipt = time.time()
        try:
            data = json.loads(message)
            msg_type = data.get("msg_type")

            if msg_type == "error":
                logger.error(f"Deriv API Error Response: {data.get('error', {}).get('message')}")
                return

            if msg_type == "time":
                self.server_epoch = int(data.get("time", 0))
                self.server_epoch_local_time = local_receipt
                drift = abs(local_receipt - self.server_epoch)
                if drift > SERVER_SYNC_TOLERANCE_SEC:
                    logger.warning(f"Clock drift detected with Deriv: {drift:.2f}s difference.")
                return

            if msg_type == "tick" and "tick" in data:
                tick_data = data["tick"]
                symbol = tick_data.get("symbol")
                quote = float(tick_data.get("quote", 0.0))
                epoch = int(tick_data.get("epoch", math.floor(local_receipt)))
                
                logger.info(f"Received live tick -> Symbol: {symbol} | Quote: {quote}")

                if epoch > self.server_epoch:
                    self.server_epoch = epoch
                    self.server_epoch_local_time = local_receipt

                if symbol in self._tick_subscribers:
                    self._tick_subscribers[symbol](epoch, quote, local_receipt)

            elif msg_type == "candles":
                req_id = str(data.get("echo_req", {}).get("ticks_history", ""))
                if req_id in self._history_callbacks:
                    self._history_callbacks[req_id](data)

        except Exception as e:
            logger.error(f"Error handling Deriv message: {e}")

    def _on_error(self, ws, error):
        logger.error(f"Deriv WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        self.is_connected = False
        logger.info(f"Deriv WebSocket closed: {close_status_code} - {close_msg}")

    def fetch_historical_candles_sync(self, symbol: str, count: int = 120, granularity: int = 60) -> List[dict]:
        try:
            temp_ws = websocket.create_connection(self.ws_url, timeout=10)
            req = {
                "ticks_history": symbol,
                "adjust_start_time": 1,
                "count": count,
                "end": "latest",
                "granularity": granularity,
                "style": "candles"
            }
            temp_ws.send(json.dumps(req))
            res = temp_ws.recv()
            temp_ws.close()
            payload = json.loads(res)
            return payload.get("candles", [])
        except Exception as e:
            logger.error(f"Failed to fetch historical candles for {symbol}: {e}")
            return []
