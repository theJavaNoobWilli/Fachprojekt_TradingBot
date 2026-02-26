import asyncio
from dataclasses import dataclass
from datetime import datetime
from sqlalchemy import text

from datenbank import Session

@dataclass
class Gap:
    prev_ts: datetime
    ts: datetime
    prev_close: float
    open: float
    diff: float

async def fetch_candles(timeframe_table = "candles_1m") -> list[tuple[datetime, float, float]]: # lädt (ts, open, close) sortiert nach ts.
    q = text(f"""SELECT ts, open, close
                 FROM {timeframe_table}
                 ORDER BY ts""")
    
    async with Session() as session:
        result = await session.execute(q)   # SELECT-Anfragen immer mit execute()
        rows = result.fetchall()            # holt alle Zeilen auf

    return rows

def find_gaps(rows: list[tuple[datetime, float, float]]):  # prüft ob relative Differenz > Toleranzwert
    gaps = []

    prev_ts, prev_open, prev_close = rows[0][0], rows[0][1], rows[0][2]

    for i in range(1, len(rows)):
        ts, o, c = rows[i]

        diff = abs(o - prev_close) / abs(prev_close)

        if diff > 0.001:                                                                        # 0.001 ist die Toleranz
            gaps.append(Gap(prev_ts=prev_ts, ts=ts, prev_close=prev_close, open=o, diff=diff))

        prev_ts, prev_open, prev_close = ts, o, c

    return gaps

async def main():
    rows = await fetch_candles(timeframe_table="candles_1m")
    print(f"Loaded candles: {len(rows)}")

    gaps = find_gaps(rows)

    if not gaps:
        print(f"Keine Gaps gefunden")
        return

    print(f"Gefundene Gaps: {len(gaps)}")   
    
    diffs = []
    for g in gaps:
        diffs.append(g.diff)

    print(f"max_abs_diff: {max(diffs)}")
    print(f"min_abs_diff: {min(diffs)}")

    print("\n Gaps:")

    for g in gaps:
        rel_pct = g.diff * 100

        print(f"- prev_ts={g.prev_ts.isoformat()} | ts={g.ts.isoformat()} | prev_close={g.prev_close} | open={g.open} | rel_diff_pct={rel_pct:.3f}%")

if __name__ == "__main__":
    asyncio.run(main())
