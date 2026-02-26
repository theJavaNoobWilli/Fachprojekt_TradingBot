import numpy as np  
from datetime import datetime, timezone

from datenbank import BacktestResult, Orders, Session, set_utc, upsert_backtest, upsert_equity

class BacktestState:
    balance = 10_000.0
    qty = 0.0
    entry_price = None

    total_trades = 0
    long_trades = 0
    short_trades = 0
    winning_trades = 0
    losing_trades = 0

    total_return = 0.0

    peak_balance = 10_000.0
    max_drawdown = 0.0

    equity_curve = []

    cooldown = 0

    last_funding_index = None
    funding_paid_total = 0.0

async def run_backtest(signals):
    state = BacktestState()

    async with Session() as session:
        async with session.begin():
            for i, row in signals.iterrows():
                last_ts, last_price = None, None

                ts = row["ts"]
                price = float(row["close"])
                last_ts, last_price = ts, price # werden laufend überschrieben, damit am Ende der letzte close verfügbar ist

                apply_funding(state, ts, price, funding_rate=0.0001, interval_hours=8)

                equity = actual_equity(state.balance, state.qty, state.entry_price, price)  # Balance + Unrealized
                state.equity_curve.append((ts, equity))

                if state.cooldown > 0:          # Optional: Cooldown für 1min, wenn die Indikatoren an Schwellenwerten oszillieren
                    state.cooldown -= 1

                    continue

                if i % 50000 == 0:
                    print(f"Verarbeite Candle {i} | ts={row['ts']}")

                signal = pick_signal(row)   # priorisiert imm_signale über normale

                if signal is None:
                    continue

                direction = 1 if signal.startswith("long") else -1

                # 1) ENTRY, wenn gar keine Position offen ist
                if state.qty == 0.0:
                    await entry(session, state, ts, price, direction)

                    continue

                # 2) gleiches Signal wie aktuelle Richtung -> ignorieren                        ## funktioniert das zu 100%                        
                if (state.qty > 0 and direction == 1) or (state.qty < 0 and direction == -1):

                    continue

                # 3) Gegensignal -> NUR CLOSE (kein Flip/keine neue Entry-Order)
                await close(session, state, ts, price)
                state.cooldown = 1

                continue

            # offene Position zum letzten Preis schließen                           
            if state.qty != 0.0 and last_ts is not None and last_price is not None:
                await close(session, state, last_ts, last_price)                              # wenn kein Signal im Laufe des Backtests, crasht es hier!

    result = compute(state)

    print_results(result)

    equity_points = state.equity_curve  # list[(ts, equity)]
    ts_start = equity_points[0][0]
    ts_end   = equity_points[-1][0]

    async with Session() as session:
        async with session.begin():
            await upsert_backtest(session, result=result)
            await upsert_equity(session, state.equity_curve)

    return result

def actual_equity(balance, qty, entry_price, price):
    if qty == 0.0 or entry_price is None:               # wenn keine Pos offen ist
        return balance
                                       
    unrealized = (price - entry_price) * qty    

    return balance + unrealized

def pick_signal(row):                                               # bekommt Signale + Backtest-Resultate; gibt das Signal zurück
    if row.get("long_imm_entry", False):  return "long_imm_entry"   # get: wenn Signal nicht vorhanden gebe False
    if row.get("short_imm_entry", False): return "short_imm_entry"  #... returne also nichts wenn False
    if row.get("long_entry", False):      return "long_entry"
    if row.get("short_entry", False):     return "short_entry"  # Priorität: imm > normal (deine Wahl)

    return None         

async def entry(session, state: BacktestState, ts, price, direction):
    side = "buy" if direction == 1 else "sell"                          # nur für das Order-Logging der DB relevant

    anteil = 0.1
    state.qty = state.balance / price * anteil

    state.total_trades += 1

    if direction == 1:
        state.long_trades += 1
    else:
        state.short_trades += 1
        state.qty = -state.qty

    state.entry_price = price

    session.add(Orders(ts=set_utc(ts), side=side, price=price, qty=state.qty, pos_after=state.qty, balance_after=state.balance, realized_pnl=0.0))

async def close(session, state: BacktestState, ts: datetime, price): # Schließt die aktuell offene Position (Exit)
    side = "sell" if state.qty > 0 else "buy"

    realized = (price - state.entry_price) * state.qty 

    if realized > 0:
        state.winning_trades += 1
    elif realized < 0:
        state.losing_trades += 1

    state.balance += realized

    update_drawdown(state)

    # Reset Position
    state.qty = 0.0
    state.entry_price = None

    session.add(Orders(ts=set_utc(ts), side=side, price=price, qty=state.qty, pos_after=state.qty, balance_after=state.balance, realized_pnl=realized))

def update_drawdown(state: BacktestState):  # rechnet über die Balance Differenz, nicht über die Order direkt!
    if state.balance > state.peak_balance:
        state.peak_balance = state.balance

    if state.peak_balance > 0:
        drawdown = (state.peak_balance - state.balance) / state.peak_balance    # drawdown ist also relativ

        if drawdown > state.max_drawdown:
            state.max_drawdown = drawdown * 100 # in %

def apply_funding(state: BacktestState, ts, price: float, funding_rate: float = 0.0001, interval_hours: int = 8):
    """
    Simuliert Funding für Perpetuals.

    payment = - notional * funding_rate * sign(qty)
      - Long (qty>0): bei funding_rate > 0 -> bezahlt (negativ)
      - Short (qty<0): bei funding_rate > 0 -> erhält (positiv)
      - Bei funding_rate < 0 umgekehrt.

    Annahmen:
    - Funding alle `interval_hours` Stunden (Binance i.d.R. 8h).
    - Preis am Candle-Close als Approximation für den Funding-Preis.
    """
    if state.qty == 0.0:
        return

    # ts UTC-sicher machen (dein set_utc macht tz-aware -> utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)

    interval_seconds = interval_hours * 3600
    funding_index = int(ts.timestamp() // interval_seconds)

    if state.last_funding_index is None:
        state.last_funding_index = funding_index
        return

    missed = funding_index - state.last_funding_index
    if missed <= 0:
        return

    notional = abs(state.qty) * float(price)

    # pro verpasstem Funding-Zeitpunkt buchen
    direction = 1.0 if state.qty > 0 else -1.0
    payment_per_event = -notional * float(funding_rate) * direction
    payment = payment_per_event * missed

    state.balance += payment
    state.funding_paid_total += payment
    state.last_funding_index = funding_index

def compute(state: BacktestState, periods_per_year = 365 * 24 * 60, start_balance = 10_000.0):  # periods_per_year für Sharpe
    end_balance = state.balance
    total_return = (end_balance / start_balance - 1.0) * 100.0                                      # = %   ## wieso -1?
    winrate = (state.winning_trades / state.total_trades * 100.0) if state.total_trades else 0.0   

    equity = np.array([e for _, e in state.equity_curve])   # Eq-Curve sollte mind. 3 Werte haben und nie auf 0 fallen, sonst x/0  
    renditen = equity[1:] / equity[:-1] - 1.0               # bildet immer die Rendite in % 2er aufeinanderfolgender Closes                       

    r_mean = renditen.mean()     # Avg.-Periodenrendite
    r_std = renditen.std(ddof=1)

    sharpe = np.sqrt(periods_per_year) * r_mean / r_std # berechnet sich über Volatilität & Zeit    

    return BacktestResult(end_balance = end_balance,
                          total_return = total_return,
                          winrate   = winrate,
                          max_drawdown = state.max_drawdown,
                          sharpe    = sharpe,
                          total_trades = state.total_trades,
                          long_trades = state.long_trades,
                          short_trades = state.short_trades,
                          winning_trades = state.winning_trades,
                          losing_trades = state.losing_trades)

def print_results(result: BacktestResult):
    print("end_balance: ", result.end_balance)
    print("total_return_%: ", result.total_return)
    print("total_trades: ", result.total_trades)
    print("long_trades", result.long_trades)
    print("short_trades", result.short_trades)
    print("winning_trades: ", result.winning_trades)
    print("losing_trades: ", result.losing_trades)
    print("winrate_%: ", result.winrate)
    print("max_drawdown_%: ", result.max_drawdown)
    print("sharpe_time_1m: ", result.sharpe)

"""
async def main():
    await run_backtest()

if __name__ == "__main__":
    asyncio.run(main())
"""

## anstatt mit Zuständen mit Flanken arbeiten

## es fehlt noch Funding