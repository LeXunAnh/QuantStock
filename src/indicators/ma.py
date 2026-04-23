import pandas as pd

def _ma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).mean()

def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False, min_periods=span).mean()

def calc_ma_family(df: pd.DataFrame) -> pd.DataFrame:
    """Tính MA5,10,20,50,200 và EMA9,12,26 từ cột close_price"""
    c = df["close_price"]
    df["ma5"]   = _ma(c, 5).round(4)
    df["ma10"]  = _ma(c, 10).round(4)
    df["ma20"]  = _ma(c, 20).round(4)
    df["ma50"]  = _ma(c, 50).round(4)
    df["ma200"] = _ma(c, 200).round(4)
    df["ema9"]  = _ema(c, 9).round(4)
    df["ema12"] = _ema(c, 12).round(4)
    df["ema26"] = _ema(c, 26).round(4)
    return df