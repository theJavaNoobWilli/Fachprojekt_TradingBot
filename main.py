import asyncio
from binance_request_history import Daten
from datenbank import Orders, Session, init_db, upsert_candle, insert_signal, upsert_indikator, set_utc #, test_read
from algo import trad_strat
from orderAusfuehrung import OrderAusfuehrungBinance
import pandas as pd
from sqlalchemy import text   
from backtest import run_backtest

daten_client = Daten()  # das Objekt gibt Auskunft darüber welche Daten ich eigentlich will
trader = Orders()

async def main():

    await init_db()         # DB-Tables initiieren

    await get_candles() 

    dataFrame = await load_history()

    signals = trad_strat(dataFrame) # gibt die Indikatorenwerte zurück, die auch als Signale interpretiert werden können

    async with Session() as session:
        async with session.begin():
            await insert_signal(session, signals)  

    # OrderAusfuehrung
    bot = OrderAusfuehrungBinance()
    
    #await bot.order(qty=0.01, leverage=1)

    async with Session() as session:
        async with session.begin():                     # begin() bei insert/ update/ delete
            await upsert_indikator(session, signals)

            for order in bot.client.orders:
                session.add(Orders(ts=set_utc(order["timestamp"]), side=order["side"], price=order["price"], qty=order["qty"], pos_after=0.0, balance_after=0.0, realized_pnl=0.0))

    await run_backtest(signals) # beinhaltet nicht nur die Signale sondern auch alle Backtest Resulate

async def get_candles():
    candles = await daten_client.fetch_candles()    # liefert (ts, o, h, l, c)

    async with Session() as session:        # Candles in die DB schreiben
        async with session.begin():         # sorgt für BEGIN / COMMIT, architektonisch immer außerhalb der Funktion, session.execute() immer dort wo auch inserted wird
            for ts, o, h, l, c in candles:
                await upsert_candle(session, ts, o, h, l, c)

async def load_history():
    async with Session() as session:
        result = await session.execute(text("""SELECT ts, open, high, low, close
                                               FROM candles_15m
                                               ORDER BY ts"""))
        
        rows = result.fetchall()

    return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close"])

if __name__ == "__main__":
    asyncio.run(main())

