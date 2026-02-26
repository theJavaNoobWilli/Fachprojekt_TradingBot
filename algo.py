import numpy as np

# Strategie
def trad_strat(candle, rsi_fast_len = 7, rsi_slow_len = 14, bb_len= 20, faktor = 0.4, env_len = 20, env_pct = 0.0015, sma_len = 10): ## Input: Candle-DataFrame mit Spalten Timestamp + 'OHCL'
    out = candle.copy()
    close_price = out["close"]

    # Berechnungen der Indikatoren
    out["rsi_fast"] = get_rsi(close_price, rsi_fast_len)  # RSI  
    out["rsi_slow"] = get_rsi(close_price, rsi_slow_len)

    out["bb_mid"], out["bb_upper"], out["bb_lower"] = get_bb(close_price, bb_len, faktor)    # Bollinger Bänder

    out["env_mid"], out["env_upper"], out["env_lower"] = get_envelope(close_price, env_len, env_pct)  # Envelope

    out["sma_10"] = close_price.rolling(sma_len, min_periods=sma_len).mean()  # SMA 

    rsi_fast = out["rsi_fast"].shift(1)                                             # Bedingung: RSI(7) kreuzt RSI(14)
    rsi_slow = out["rsi_slow"].shift(1)                                             # shift gibt also die Werte in der Zeitreihe davor
    rsi_cross_up = (rsi_fast <= rsi_slow) & (out["rsi_fast"] > out["rsi_slow"])     # Cross von UNTEN nach OBEN: vorher & nachher
    rsi_cross_down = (rsi_fast >= rsi_slow) & (out["rsi_fast"] < out["rsi_slow"])   # Cross von OBEN nach UNTEN

    mom_up_env = out["bb_upper"] >= out["env_upper"]    # Volatilitätscheck:
    mom_down_env = out["bb_lower"] <= out["env_lower"]  # oberes BB >= obere Env

    mom_up_sma = out["sma_10"] >= out["bb_upper"]       # SMA > oberes BB                              
    mom_down_sma = out["sma_10"] <= out["bb_lower"]     # SMA < unteres BB    

    out["long_entry"] = rsi_cross_up & mom_up_env & mom_down_env & mom_up_sma       # Long-Bedingungen vereint
    out["short_entry"] = rsi_cross_down & mom_up_env & mom_down_env & mom_down_sma  # Short

    sma10 = out["sma_10"].shift(1)          # sofortige Bedingung: SMA > Envelope (größere Channelgrenze)
    env_upper = out["env_upper"].shift(1)
    env_lower = out["env_lower"].shift(1)
    
    out["long_imm_entry"] = (sma10 <= env_upper) & (out["sma_10"] > out["env_upper"])   # Cross von UNTEN   
    out["short_imm_entry"] = (sma10 >= env_lower) & (out["sma_10"] < out["env_lower"])  # Cross von OBEN

    return out  # 'out' = pd.Dataframe --> long_entry, short_entry & long_imm_entry, short_imm_entry

# Indikatoren
def get_rsi(series, length):
    delta = series.diff()           # Differenz von x1(close) zu x2(close)

    gain = delta.clip(lower=0.0)    # grüne Kerze, 'clip()' = lower begrenzt hier Werte auf >= 0
    loss = delta.clip(upper=0.0)    # rote Kerze

    avg_gain = gain.ewm(alpha=1/length, adjust=False, min_periods=length).mean()    # "adjust = False" = rekursive EMA-Logik (s. Notizen)
    avg_loss = loss.ewm(alpha=1/length, adjust=False, min_periods=length).mean()    # ewm = alpha * x(t) + (1-alpha) * EMA(t-1)              ## hier wird der EMA benutzt, nicht der RMA, wie in Tradingview beschrieben!

    rs = avg_gain / avg_loss.replace(0, np.nan) # avg(Gain) / NaN = kleinstmögliche Zahl 

    return 100 - (100 / (1 + rs))   # --> umso mehr Gewinne, desto höher der RSI

def get_bb(series, length, faktor):
    basis = series.rolling(length, min_periods=length).mean()       # = SMA, series: pd.Series,  'rolling' bildet ein gleitendes Fenster über 'length' Bars
    stdA = series.rolling(length, min_periods=length).std(ddof=0)   # Std.-A. der Population
    upper = basis + faktor * stdA
    lower = basis - faktor * stdA

    return basis, upper, lower      # ... = Tuple

def get_envelope(series, length, percent):                  # Bsp: upper = sma * 1,15
    mid = series.rolling(length, min_periods=length).mean() # 'min_periods' = erst ab dem Zeitpunkt, wo 'length'-Werte existieren 
    upper = mid * (1.0 + percent)
    lower = mid * (1.0 - percent)

    return mid, upper, lower        # = Tuple

