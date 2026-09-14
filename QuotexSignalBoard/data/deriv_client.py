import json
import logging
import websocket
import threading
import time
from config import APP_ID, API_TOKEN, ACTIVE_SYMBOLS

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
        self.history_cache = {}
        self.history_event = threading.Event()
        
    @property
    def is_connected(self):
        return self.connected

    def get_server_time(self):
        return int(time.time())

    def fetch_server_epoch_sync(self):
        return int(time.time())

    def fetch_historical_candles_batch_sync(self, jobs):
        results = {}
        if not self.ws or not self.connected:
            return results

        for job in jobs:
            key = job.get("key")
            symbol = job.get("symbol")
            count = job.get("count", 100)
            granularity = job.get("granularity", 60)
            
            clean_symbol = symbol.replace("frx", "")
            target_sym = f"frx{clean_symbol}"
            
            req = {
                "ticks_history": target_sym,
                "adjust_start_time": 1,
                "count": count,
                "end": "latest",
                "granularity": granularity,
                "style": "candles"
            }
            
            try:
                self.history_event.clear()
                self.ws.send(json.dumps(req))
                if self.history_event.wait(timeout=2.0):
                    candles = self.history_cache.get(target_sym, [])
                    results[key] = candles
                else:
                    req["ticks_history"] = clean_symbol
                    self.ws.send(json.dumps(req))
                    if self.history_event.wait(timeout=2.0):
                        candles = self.history_cache.get(clean_symbol, [])
                        results[key] = candles
                    else:
                        results[key] = []
            except Exception as e:
                logger.error(f"Error fetching history for {symbol}: {e}")
                results[key] = []
                
        return results

    def register_tick_handler(self, symbol, handler):
        self.tick_handlers[symbol] = handler

    def start(self):
        self.connect()

    def connect(self):
        url = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
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
                    self.ws.run_forever(ping_interval=30, ping_timeout=10)
                except Exception as e:
                    logger.error(f"Deriv WebSocket connection error: {e}")
                
                self.connected = False
                if self.is_running:
                    logger.info("Reconnecting to Deriv in 5 seconds...")
                    time.sleep(5)
                    
        threading.Thread(target=run, daemon=True).start()

    def on_open(self, ws):
        logger.info("Connected to Deriv API successfully.")
        self.connected = True
        if API_TOKEN:
            auth_req = {"authorize": API_TOKEN}
            ws.send(json.dumps(auth_req))
        
        self.subscribe_symbols(ws)

    def subscribe_symbols(self, ws):
        logger.info(f"ACTIVE_SYMBOL_DIAGNOSTIC: Attempting to subscribe to {len(ACTIVE_SYMBOLS)} pairs.")
        
        for symbol in ACTIVE_SYMBOLS:
            clean_symbol = symbol.replace("frx", "")
            for target_sym in [f"frx{clean_symbol}", clean_symbol]:
                req = {"ticks": target_sym, "subscribe": 1}
                ws.send(json.dumps(req))
                time.sleep(0.05)

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            msg_type = data.get("msg_type")
            
            if msg_type == "tick":
                tick = data.get("tick")
                if tick:
                    symbol = tick.get("symbol")
                    epoch = int(tick.get("epoch", time.time()))
                    quote = float(tick.get("quote", 0))
                    
                    self.server_time = epoch
                    
                    if symbol in self.tick_handlers:
                        try:
                            self.tick_handlers[symbol](epoch, quote, time.monotonic())
                        except Exception as e:
                            logger.error(f"Error in tick handler for {symbol}: {e}")
                            
                    if self.on_tick_callback:
                        self.on_tick_callback(tick)
                        
            elif msg_type == "candles":
                echo_req = data.get("echo_req", {})
                sym = echo_req.get("ticks_history")
                candles_raw = data.get("candles", [])
                formatted_candles = []
                for c in candles_raw:
                    t_val = int(c.get("epoch") or c.get("time") or 0)
                    formatted_candles.append({
                        "epoch": t_val,
                        "time": t_val,
                        "open": float(c.get("open", 0)),
                        "high": float(c.get("high", 0)),
                        "low": float(c.get("low", 0)),
                        "close": float(c.get("close", 0))
                    })
                if sym:
                    self.history_cache[sym] = formatted_candles
                self.history_event.set()
                    
            elif msg_type == "error":
                err = data.get("error", {})
                err_code = err.get("code")
                err_msg = err.get("message")
                logger.warning(f"DerivClient Warning/Error [{err_code}]: {err_msg}")
                self.history_event.set()
                
            elif msg_type == "authorize":
                if data.get("authorize"):
                    logger.info("Deriv API Authorization Successful.")
                else:
                    logger.warning("Deriv API Authorization Failed.")
                    
        except Exception as e:
            logger.error(f"Error processing message from Deriv: {e}")

    def on_error(self, ws, error):
        self.connected = False
        logger.error(f"Deriv WebSocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        self.connected = False
        logger.warning(f"Deriv WebSocket closed. Code: {close_status_code}, Message: {close_msg}")

    def disconnect(self):
        self.is_running = False
        self.connected = False
        if self.ws:
            self.ws.close()
