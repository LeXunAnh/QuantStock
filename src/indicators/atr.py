import pandas as pd

def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:

    high  = df["highest_price"]
    low   = df["lowest_price"]
    close = df["close_price"]
    prev_close = close.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)

    df["atr14"] = tr.ewm(com=period - 1, adjust=False, min_periods=period).mean().round(4)
    return df