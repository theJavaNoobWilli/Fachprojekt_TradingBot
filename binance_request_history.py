import aiohttp                          # asynchrone HTTP-Bib: um nicht blockierende API-Anfragen zu schicken (z.B. um OB zu holen)   
from aiohttp import ClientSession       # Typing
import asyncio                          # um synchrone Methoden durchzuführen                            
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

class Daten:
    
    def __init__(self, symbol = "BTCUSDT", interval="1m", limit=64):  # mit self lassen sich theoretisch mehrere Objekte initiieren
        self.symbol = symbol
        self.session: ClientSession | None = None                               # Session wird später gesetzt
        self.candles: list[tuple[datetime, float, float, float, float]] = []
        self.lastRequest = 0                                                    # Rate-Limit Schutz für unsere API-Requests
        self.interval = interval                                                # Zeitintervall, wie häufig Daten abgerufen werden können
        self.limit = limit    
        self.wartezeit = 3                     
        self.lock = asyncio.Lock()                                              # shared Lock: wir wollen ein Lock für alle Methoden
        self.ohlc: dict[str, float] = {"open": 0.0,
                                       "high": 0.0,
                                       "low": 0.0,
                                       "close": 0.0}

    async def fetch_candles(self, start=None, end=datetime.now(), limit = 1000):
        start = datetime(2024, 3, 1, tzinfo=timezone.utc)                           # kein lokaler Offset, Sommerzeit-Bug
        end = end.replace(tzinfo=timezone.utc)

        start_ms = int(start.timestamp() * 1000)                # Binance akzeptiert nur UNIX               
        end_ms = int(end.timestamp() * 1000) if end else None

        url = self.get_url(start_ms=start_ms, end_ms=end_ms, limit=limit)
        raw = await self.get_OHLC(url)

        candles = []

        for entry in raw:
            open_time_ms = entry[0]
            o = float(entry[1])
            h = float(entry[2])
            l = float(entry[3])
            c = float(entry[4])
            ts = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
            candles.append((ts, o, h, l, c))

        return candles
    
    def get_url(self, start_ms, end_ms, limit):
        params = {"symbol": self.symbol, "interval": self.interval, "limit": limit} # urlencode() braucht ein Dict

        params["startTime"] = start_ms
        params["endTime"] = end_ms

        return "https://api.binance.com/api/v3/klines?" + urlencode(params) #...= "symbol=BTCUSDT&interval=1m&limit=1000&startTime=...&endTime=...""
    
    async def get_OHLC(self, url):    
        if self.session is None or self.session.closed:                                                        
            self.session = aiohttp.ClientSession()      # öffnet TCP-Verbindung im Hintergrund                          
        
        async with self.lock:   # wie schnell hintereinander geben wir eine Request ab?
            now = time.time()
            wait = self.wartezeit - (now - self.lastRequest) 
            
            if wait > 0:
                await asyncio.sleep(wait)
                
            self.lastRequest = time.time()
            
        async with self.session.get(url) as resp:
            return await resp.json()



