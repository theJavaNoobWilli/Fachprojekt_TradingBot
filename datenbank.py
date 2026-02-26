from sqlalchemy import text, DateTime, String, Float, Integer                               # das ORM (Object Relational Mapping) – statt SQL-Strings Python-Klassen & Objekte
from sqlalchemy.dialects.postgresql import insert, JSONB                                    # spezielles Insert für Postgres, das ON CONFLICT DO UPDATE (UPSERT) unterstützt
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker                  # 1) baut eine asynchrone DB-Verbindung, 2) erzeugt Sessions (pro Request eine Session)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column                           # 1) Basisklasse, von der alle Tabellen-Klassen erben, 2+3) Attributzuweisung des Objekts
from datetime import datetime, timezone
import os
import math

class Base(DeclarativeBase):
    pass

class Candle1m(Base):
    __tablename__ = "candles_1m"                                                    # diese Klasse mappt auf die Tabelle candles_1m in Postgres
    
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    open:   Mapped[float]
    high:   Mapped[float]                                        
    low:    Mapped[float]
    close:  Mapped[float]

class Signal(Base):
    __tablename__ = "signals"

    ts = mapped_column(DateTime(timezone=True), primary_key=True)
    signal_name = mapped_column(String, primary_key=True)           # "(Imm) Long/ Short"

class Orders(Base):
    __tablename__ = "orders"
    id:     Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ts:     Mapped[datetime] = mapped_column(DateTime(timezone=True))
    side:   Mapped[str]         # 'buy' | 'sell'

    # wird benötigt für den Pinify Vergleich
    price:  Mapped[float]               # Fill-Preis (Backtest: close)
    qty:    Mapped[float]               # gehandelte Menge
    pos_after: Mapped[float]            # Position nach Ausführung
    balance_after: Mapped[float]        # Equity nach Ausführung
    realized_pnl: Mapped[float]         # kann 0 sein bei Entry

class BacktestResult(Base):
    __tablename__ = "trading_performance"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)

    start_balance: Mapped[float] = mapped_column(Float) # mapped_column() = mache aus dem Attribut eine Spalte
    end_balance: Mapped[float] = mapped_column(Float)
    total_return: Mapped[float] = mapped_column(Float)

    total_trades: Mapped[int] = mapped_column(Integer)
    long_trades: Mapped[int] = mapped_column(Integer)
    short_trades: Mapped[int] = mapped_column(Integer)
    winning_trades: Mapped[int] = mapped_column(Integer)
    losing_trades: Mapped[int] = mapped_column(Integer)

    winrate: Mapped[float] = mapped_column(Float)
    max_drawdown: Mapped[float] = mapped_column(Float)
    sharpe: Mapped[float] = mapped_column(Float)

class EquityCurve(Base):
    __tablename__ = "equity_curve"

    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    equity: Mapped[dict] = mapped_column(Float)

class Indikatoren(Base):
    __tablename__ = "indikatoren"

    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)

    rsi_fast: Mapped[float]
    rsi_slow: Mapped[float]

    bb_mid:   Mapped[float]
    bb_upper: Mapped[float]
    bb_lower: Mapped[float]

    env_mid:   Mapped[float]
    env_upper: Mapped[float]
    env_lower: Mapped[float]

    sma_10: Mapped[float]

DB_DSN = os.getenv("DB_DSN", "postgresql+asyncpg://bot2:will@localhost:5432/tradingbot")    # Connection-String (DSN)
engine = create_async_engine(DB_DSN, pool_size=10, max_overflow=20)                         # Verbindung zum DB-Server, mit Connection Pool (10 Verbindungen gleichzeitig möglich)

Session = async_sessionmaker(engine, expire_on_commit=False)            # 'expire_on_commit': Objekte bleiben nach commit im Speicher nutzbar und müssen nicht neu geladen werden (Performance)

TF_SUFFIX: dict[str, str] = {"min1": "1m",      # Mapping für Frontend-Param
                             "min15": "15m",
                             "h": "1h",
                             "d": "1d"}

async def init_db():
    async with engine.begin() as conn:                  # gebe mir Connection, nicht nur Session (BEGIN), da wir createn, nicht nur selecten wollen
        await conn.run_sync(Base.metadata.create_all)   # Erzeugung der klassischen Tabelle

        #await conn.execute(text("""CREATE EXTENSION IF NOT EXISTS timescaledb;""")) # 2) TimescaleDB Extension aktivieren (bereits im Terminal erstellt)

        await conn.execute(text("""SELECT create_hypertable('public.candles_1m',
                                                            'ts',                   
                                                            if_not_exists => TRUE,          
                                                            migrate_data  => TRUE);"""))    # 3) candles_1m → Hypertable
        
        await conn.execute(text("""SELECT create_hypertable('public.signals', 
                                                            'ts',
                                                            if_not_exists => TRUE, 
                                                            migrate_data => TRUE);""")) # 'ts' ist die Zeitachse nach der partitioniert wird   

        await conn.execute(text("""SELECT create_hypertable('public.indikatoren', 
                                                            'ts',
                                                            if_not_exists => TRUE, 
                                                            migrate_data => TRUE);""")) 

        await conn.execute(text("""SELECT create_hypertable('public.equity_curve', 
                                                            'ts',
                                                            if_not_exists => TRUE, 
                                                            migrate_data => TRUE);"""))         
        
    async with engine.connect() as conn:    
        conn = await conn.execution_options(isolation_level="AUTOCOMMIT")

        # Aggregierungen
        await conn.execute(text("""CREATE MATERIALIZED VIEW IF NOT EXISTS candles_15m
                                   WITH (timescaledb.continuous) AS
                                   SELECT
                                    time_bucket('15 minutes', ts) AS ts,
                                    first(open, ts)  AS open,
                                    max(high)        AS high,
                                    min(low)         AS low,
                                    last(close, ts)  AS close
                                   FROM candles_1m
                                   GROUP BY 1;""")) # 15 Minuten

        await conn.execute(text("""CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1h
                                   WITH (timescaledb.continuous) AS
                                   SELECT
                                    time_bucket('1 hour', ts) AS ts,
                                    first(open, ts)  AS open,
                                    max(high)        AS high,
                                    min(low)         AS low,
                                    last(close, ts)  AS close
                                   FROM candles_15m
                                   GROUP BY 1;""")) # 1 Stunde

        await conn.execute(text("""CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1d
                                   WITH (timescaledb.continuous) AS
                                   SELECT
                                    time_bucket('1 day', ts) AS ts,
                                    first(open, ts)  AS open,
                                    max(high)        AS high,
                                    min(low)         AS low,
                                    last(close, ts)  AS close
                                   FROM candles_1h
                                   GROUP BY 1;""")) # 1 Tag

        # Refreshes
        await conn.execute(text("""DO $$
                                   BEGIN
                                   PERFORM add_continuous_aggregate_policy(
                                    'candles_15m',
                                    start_offset => INTERVAL '30 days',
                                    end_offset   => INTERVAL '2 minutes',
                                    schedule_interval => INTERVAL '4 minutes');
                                   EXCEPTION WHEN others THEN
                                   END $$;"""))                 # Intervall hier 30d zurück bis 2 min vor Ende

        await conn.execute(text("""DO $$
                                   BEGIN
                                   PERFORM add_continuous_aggregate_policy(
                                    'candles_1h',
                                    start_offset => INTERVAL '90 days',
                                    end_offset   => INTERVAL '5 minutes',
                                    schedule_interval => INTERVAL '20 minutes');
                                   EXCEPTION WHEN others THEN
                                   END $$;"""))                 

        await conn.execute(text("""DO $$
                                   BEGIN
                                   PERFORM add_continuous_aggregate_policy(
                                    'candles_1d',
                                    start_offset => INTERVAL '3 years',
                                    end_offset   => INTERVAL '1 hour',
                                    schedule_interval => INTERVAL '120 minutes');
                                   EXCEPTION WHEN others THEN
                                   END $$;"""))                 
        
async def upsert_candle(session, ts, o, h, l, c):
    ts = set_utc(ts)                                # Normalisierung (gleiche Zeitzone), wichtig damit on-conflict Abfrage funktioniert
    
    inserted = insert(Candle1m).values(ts=ts, open=o, high=h, low=l, close=c)   # fügt eine Zeile ein (was später ausgeführt wird)
    
    # wenn 'ts' schon existiert --> aktualisiere die Werte (update)
    inserted = inserted.on_conflict_do_update(index_elements=[Candle1m.ts], set_={"open": o, "high": h, "low": l, "close": c})
    await session.execute(inserted)

async def insert_signal(session, df):                                               # erwartet df mit Spalte "ts" und bool-Spalten für Signale
    sig_cols = ["long_entry", "short_entry", "long_imm_entry", "short_imm_entry"]

    rows = []
    for _, r in df.iterrows():
        ts = set_utc(r["ts"])
        for col in sig_cols:
            if bool(r.get(col, False)):
                rows.append({"ts": ts, "signal_name": col})

    if not rows:
        return

    stmt = (
        insert(Signal)
        .values(rows)
        .on_conflict_do_nothing(index_elements=["ts", "signal_name"])
    )
    await session.execute(stmt)

async def upsert_indikator(session, df, chunk_size: int = 1000):
    need = ["ts","rsi_fast","rsi_slow","bb_mid","bb_upper","bb_lower",
            "env_mid","env_upper","env_lower","sma_10"]

    sub = df[need].copy()
    sub["ts"] = sub["ts"].map(set_utc)
    sub = sub.where(sub.notna(), None)
    sub = sub.dropna(subset=need[1:], how="all")

    rows = sub.to_dict("records")
    if not rows:
        return

    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i+chunk_size]
        ins = insert(Indikatoren).values(chunk)
        stmt = ins.on_conflict_do_update(
            index_elements=[Indikatoren.ts],
            set_={"rsi_fast": ins.excluded.rsi_fast,
                  "rsi_slow": ins.excluded.rsi_slow,
                  "bb_mid":   ins.excluded.bb_mid,
                  "bb_upper": ins.excluded.bb_upper,
                  "bb_lower": ins.excluded.bb_lower,
                  "env_mid":  ins.excluded.env_mid,
                  "env_upper":ins.excluded.env_upper,
                  "env_lower":ins.excluded.env_lower,
                  "sma_10":   ins.excluded.sma_10})
        
        await session.execute(stmt)

def set_utc(ts):
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)  # ts ist schon in UTC, also einfach zurückgeben

    return ts.astimezone(timezone.utc)   # In echte UTC umrechnen und tzinfo entfernen (später einfacher für SQLAlchemy)

async def upsert_backtest(session, result):
    values = run_values(result)
    values["id"] = 1

    stmt = insert(BacktestResult).values(**values)

    update_cols = {k: stmt.excluded[k] for k in values.keys() if k != "id"}

    stmt = stmt.on_conflict_do_update(index_elements=[BacktestResult.id], set_=update_cols)

    await session.execute(stmt)

def run_values(result):
    return {"start_balance": 10_000.0,
            "end_balance": float(result.end_balance),
            "total_return": float(result.total_return),

            "total_trades": int(result.total_trades),
            "long_trades": int(result.long_trades),
            "short_trades": int(result.short_trades),
            "winning_trades": int(result.winning_trades),
            "losing_trades": int(result.losing_trades),

            "winrate": float(result.winrate),
            "max_drawdown": float(result.max_drawdown),
            "sharpe": float(result.sharpe)}

async def upsert_equity(session, equity_points):
    if equity_points is None:
            return
    
    batch = []
    
    for ts, eq in equity_points:
        if ts is None or eq is None:
            continue

        if hasattr(ts, "to_pydatetime"):
            ts = ts.to_pydatetime()

        ts = set_utc(ts)  # macht tz-aware UTC

        try:
            eq_f = float(eq)
        except (TypeError, ValueError):
            continue

        if not math.isfinite(eq_f):
            continue

        batch.append({"ts": ts, "equity": eq_f})

        if len(batch) >= 5000:
            stmt = insert(EquityCurve).values(batch)
            stmt = stmt.on_conflict_do_update(index_elements=[EquityCurve.ts],set_={"equity": stmt.excluded.equity})

            await session.execute(stmt)
            batch.clear()

    if batch:        
        stmt = insert(EquityCurve).values(batch)

        stmt = stmt.on_conflict_do_update(index_elements=[EquityCurve.ts], set_={"equity": stmt.excluded.equity})

        await session.execute(stmt)
