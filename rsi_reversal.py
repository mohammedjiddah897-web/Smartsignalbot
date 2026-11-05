"""Standalone RSI signals derived only from actual closed broker OHLC."""
from fast_trade import calc_rsi, RSI_PERIOD, RSI_OB, RSI_OS
from entry_confirmation import number


def standalone_momentum_signal(candles):
    """Map a closed-candle RSI extreme to the same-side momentum direction."""
    if len(candles) < RSI_PERIOD + 1:
        return None
    rows = [
        {**c, "max": c.get("max", c.get("high")), "min": c.get("min", c.get("low"))}
        for c in candles[-(RSI_PERIOD + 1):]
    ]
    if any(
        not all(number(c.get(key)) for key in ("time", "open", "close", "max", "min"))
        or c["time"] <= 0 or c["open"] <= 0 or c["close"] <= 0
        or c["max"] < max(c["open"], c["close"])
        or c["min"] > min(c["open"], c["close"])
        or c["max"] < c["min"]
        for c in rows
    ):
        return None
    if any(rows[index]["time"] + 60 != rows[index + 1]["time"]
           for index in range(len(rows) - 1)):
        return None

    row = rows[-1]
    rsi = calc_rsi([c["close"] for c in rows], RSI_PERIOD)
    if rsi is None:
        return None
    side = "CALL" if rsi >= RSI_OB else "PUT" if rsi <= RSI_OS else None
    if side is None:
        return None
    return {
        "side": side, "signal_time": row["time"], "execution_candle": row["time"] + 60,
        "signal_open": row["open"], "signal_close": row["close"],
        "rsi": rsi, "extreme_rsi": rsi,
    }


# Stricter standalone reversal (tightens the existing rule; no new strategy).
REV_RSI_OB = 75          # stricter than the momentum RSI_OB
REV_RSI_OS = 25          # stricter than the momentum RSI_OS
BB_PERIOD = 20
BB_MULT = 2.0
MIN_WICK_RATIO = 0.5     # rejection wick must be at least half the candle range


def _bollinger(closes, period=BB_PERIOD, mult=BB_MULT):
    window = closes[-period:]
    mean = sum(window) / period
    std = (sum((x - mean) ** 2 for x in window) / period) ** 0.5   # population std, as TradingView
    return mean + mult * std, mean - mult * std


def standalone_reversal_signal(candles):
    """Return a standalone 1m RSI reversal from the latest complete candle.

    All must hold on the closed candle:
      1. Intrabar RSI extreme (actual high/low with preceding closes fixed)
         at or beyond REV_RSI_OB / REV_RSI_OS.
      2. The candle pierced the Bollinger(20, 2) band on that side and
         closed back inside it.
      3. A directional body closing away from the extreme, with a rejection
         wick of at least MIN_WICK_RATIO of the candle range.
    Gaps, malformed OHLC, dojis, and ambiguous bars do not produce a signal.
    """
    need = max(RSI_PERIOD + 1, BB_PERIOD)
    if len(candles) < need:
        return None
    rows = [
        {**c, "max": c.get("max", c.get("high")), "min": c.get("min", c.get("low"))}
        for c in candles[-need:]
    ]
    if any(
        not all(number(c.get(key)) for key in ("time", "open", "close", "max", "min"))
        or c["time"] <= 0 or c["open"] <= 0 or c["close"] <= 0
        or c["max"] < max(c["open"], c["close"])
        or c["min"] > min(c["open"], c["close"])
        or c["max"] < c["min"]
        for c in rows
    ):
        return None
    if any(rows[index]["time"] + 60 != rows[index + 1]["time"]
           for index in range(len(rows) - 1)):
        return None

    row = rows[-1]
    closes = [c["close"] for c in rows]
    rsi_closes = closes[-(RSI_PERIOD + 1):]
    overbought = calc_rsi([*rsi_closes[:-1], row["max"]], RSI_PERIOD)
    oversold = calc_rsi([*rsi_closes[:-1], row["min"]], RSI_PERIOD)
    if overbought is None or oversold is None:
        return None

    upper, lower = _bollinger(closes)
    rng = row["max"] - row["min"]
    if rng <= 0:
        return None
    upper_wick = (row["max"] - max(row["open"], row["close"])) / rng
    lower_wick = (min(row["open"], row["close"]) - row["min"]) / rng

    put = (overbought >= REV_RSI_OB
           and row["close"] < row["open"]
           and row["max"] > upper and row["close"] < upper
           and upper_wick >= MIN_WICK_RATIO)
    call = (oversold <= REV_RSI_OS
            and row["close"] > row["open"]
            and row["min"] < lower and row["close"] > lower
            and lower_wick >= MIN_WICK_RATIO)
    if put == call:
        return None
    if put:
        return {
            "side": "PUT", "signal_time": row["time"], "execution_candle": row["time"] + 60,
            "signal_open": row["open"], "signal_close": row["close"],
            "rsi": calc_rsi(rsi_closes, RSI_PERIOD), "extreme_rsi": overbought,
        }
    return {
        "side": "CALL", "signal_time": row["time"], "execution_candle": row["time"] + 60,
        "signal_open": row["open"], "signal_close": row["close"],
        "rsi": calc_rsi(rsi_closes, RSI_PERIOD), "extreme_rsi": oversold,
    }


def reversal_signal(candles, loss_time, lost_side):
    index = next((i for i, c in enumerate(candles) if c["time"] == loss_time), None)
    if index is None or index < RSI_PERIOD:
        return None
    row = candles[index]
    closes = [c["close"] for c in candles[:index + 1]]
    if not all(number(v) for v in [*closes, row["open"], row["max"], row["min"]]):
        return None
    if row["min"] > min(row["open"], row["close"]) or row["max"] < max(row["open"], row["close"]):
        return None
    at_close = calc_rsi(closes)
    # For the existing close-based RSI, fixed preceding closes make RSI
    # monotonic in this candle's price. Its actual high/low therefore prove
    # an intrabar extreme without inventing ticks or using a future candle.
    if lost_side.upper() == "CALL":
        extreme = calc_rsi([*closes[:-1], row["max"]])
        valid = row["close"] 
