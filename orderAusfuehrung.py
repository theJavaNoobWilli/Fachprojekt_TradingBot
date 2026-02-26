import aiohttp
import asyncio
import hmac
import hashlib  # Verschlüsselung des API-Secrets
import time
from urllib.parse import urlencode      # für Umformung in URL
import math

class BinanceFuturesAPI:                                        # SIGNED Endpoint
    def __init__(self, base_url = "https://fapi.binance.com"):
        self.api_key = "VNLRck8rtT5WXOx9RxFsLyNorJch3s0kvJ5fKSluPgC1lx1JtX4CUMzmv3z7FVIJ"   # public
        api_secret = "aHAlibYMXukuVEQCv5fkWoaux7tYDhQmggh236hauCvj0oHoow7yCEXYqQDQnwiX"     # private
        self.api_secret = api_secret.encode("utf-8")                                        # aus String in Byte-Objekt
        self.base_url = base_url.rstrip("/")                                                # enfernt eventuellen Slash am Ende
        self.session = None
        self.orders = []

    async def market_order(self, symbol, side, qty, reduce_only = False):
        params: dict = {"symbol": symbol,
                        "side": side,
                        "type": "MARKET",
                        "quantity": qty}

        if reduce_only:
            params["reduceOnly"] = "true"   

        raw = await self.request("POST", "/fapi/v1/order", params)  # Order-Befehl geht raus

        id = raw.get("orderId")             # Antwort für die Abspeicherung der Order in der DB
        timestamp = raw.get("timestamp")
        price = float(raw.get("avgPrice") or 0.0)   # market: avgPrice oft vorhanden, sonst 0
        qty = float(raw.get("executedQty") or qty)
        
        self.orders.append({"id": id, 'timestamp': timestamp, 'side': side.lower(), "price": price, "qty": qty, "reduce_only": reduce_only, "raw": raw,})

    async def request(self, method, path, params: dict) -> dict:    # path z.B. '/fapi/v1/order'
        if self.session is None or self.session.closed:             # erstellt neue TCP-Session
            self.session = aiohttp.ClientSession()

        # Binance: timestamp ist Pflicht bei signed Endpoints
        params.setdefault("recvWindow", 5000)                   # Binance akzeptiert Anfrage nur, wenn Lag weniger als 5sek ist 
        params["timestamp"] = int(time.time() * 1000)           # Timestamp in ms hinzufügen

        qs = urlencode(params, doseq=True)                                              # Query String bauen
        sig = hmac.new(self.api_secret, qs.encode("utf-8"), hashlib.sha256).hexdigest() # denn Binance verlangt timestamp + SHA256 Signatur 
        url = f"{self.base_url}{path}?{qs}&signature={sig}"                             # URL zsm-setzen

        headers = {"X-MBX-APIKEY": self.api_key}    # API Header, damit Request valide ist

        async with self.session.request(method, url, headers=headers) as resp:  # HTTP Request versenden
            data = await resp.json(content_type=None)                           # Antwort wird empfangen

            if resp.status >= 400:                                          # API-Fehler abfangen
                raise RuntimeError(f"Binance HTTP {resp.status}: {data}")
            return data
        
class OrderAusfuehrungBinance:
    def __init__(self):
        self.client = BinanceFuturesAPI()
        self.symbol = "BTCUSDT"
        self.last_action = None  # "buy"/ "sell"
        self.signal = None
        self.orders = self.client.orders

    async def order(self, qty, leverage = 1):
        while True:

            while self.signal is None:
                await asyncio.sleep(6)

            account = await self.client.request("GET", "/fapi/v2/account", {})  # wie viel Kapital habe ich gerade?

            for a in account["assets"]:
                if a["asset"] == "USDT":
                    usdt = a
                    break
                    
            quantity = float(usdt["availableBalance"]) * 0.1 # 10% von der Position

            ticker = await self.client.request("GET", "/fapi/v1/ticker/price",{"symbol": self.symbol}) # was ist der aktuelle Preis?
            price = float(ticker["price"])

            real_qty = quantity / price 

            qty = math.floor(real_qty / 0.001) * 0.001           # Binance erlaubt nur bestimmte, symbolabhängige Stepsize

            try:
                # Entry
                if self.signal == 1 and self.last_action == None:
                    await self.client.request("POST", "/fapi/v1/leverage", {"symbol": self.symbol, "leverage": leverage})   # setze den Leverage

                    await self.client.market_order(symbol=self.symbol, side="BUY", qty=qty, reduce_only=False)  # Entry
                    self.last_action = "buy"
                
                elif self.signal == -1 and self.last_action == None:
                    await self.client.request("POST", "/fapi/v1/leverage", {"symbol": self.symbol, "leverage": leverage})   # setze den Leverage

                    await self.client.market_order(symbol=self.symbol, side="SELL", qty=qty, reduce_only=False)  # Entry
                    self.last_action = "sell"
                
                # Exit
                if self.signal == 1 and self.last_action == "sell":
                    await self.client.request("POST", "/fapi/v1/leverage", {"symbol": self.symbol, "leverage": leverage})   # setze den Leverage

                    await self.client.market_order(symbol=self.symbol, side="BUY", qty=qty, reduce_only=True)  # Entry
                    self.last_action = None
 
                elif self.signal == -1 and self.last_action == "buy":
                    await self.client.request("POST", "/fapi/v1/leverage", {"symbol": self.symbol, "leverage": leverage})

                    await self.client.market_order(symbol=self.symbol, side="SELL", qty=qty, reduce_only=True)
                    self.last_action = None

            except Exception as e:
                print(f"❌ Binance Orderfehler: {e}")

            await asyncio.sleep(60)
 
""" Kritik: 
1) API Codes stehen direkt im Code
2) keine echte Positionsprüfung: ich verlasse mich auf last_action
3) kein Stop-Loss --> schwierigeres Risk Management
"""
