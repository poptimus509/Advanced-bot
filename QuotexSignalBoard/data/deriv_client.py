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
                
                if self.is_running:
                    logger.info("Reconnecting to Deriv in 5 seconds...")
                    time.sleep(5)
                    
        threading.Thread(target=run, daemon=True).start()

    def on_open(self, ws):
        logger.info("Connected to Deriv API successfully.")
        if API_TOKEN:
            auth_req = {"authorize": API_TOKEN}
            ws.send(json.dumps(auth_req))
        
        self.subscribe_symbols(ws)

    def subscribe_symbols(self, ws):
        logger.info(f"ACTIVE_SYMBOL_DIAGNOSTIC: Attempting to subscribe to {len(ACTIVE_SYMBOLS)} pairs.")
        
        for symbol in ACTIVE_SYMBOLS:
            req = {"ticks": symbol, "subscribe": 1}
            ws.send(json.dumps(req))
            time.sleep(0.1)

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            msg_type = data.get("msg_type")
            
            if msg_type == "tick":
                tick = data.get("tick")
                if tick and self.on_tick_callback:
                    self.on_tick_callback(tick)
                    
            elif msg_type == "error":
                err = data.get("error", {})
                err_code = err.get("code")
                err_msg = err.get("message")
                logger.error(f"DerivClient Error [{err_code}]: {err_msg}")
                
            elif msg_type == "authorize":
                if data.get("authorize"):
                    logger.info("Deriv API Authorization Successful.")
                else:
                    logger.warning("Deriv API Authorization Failed.")
                    
        except Exception as e:
            logger.error(f"Error processing message from Deriv: {e}")

    def on_error(self, ws, error):
        logger.error(f"Deriv WebSocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"Deriv WebSocket closed. Code: {close_status_code}, Message: {close_msg}")

    def disconnect(self):
        self.is_running = False
        if self.ws:
            self.ws.close()
