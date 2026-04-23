import pandas as pd
from .ma import _ema

def calc_macd(df: pd.DataFrame) -> pd.DataFrame:
    ema12 = _ema(df["close_price"], 12)
    ema26 = _ema(df["close_price"], 26)
    macd  = ema12 - ema26

    df["macd"]        = macd.round(6)
    df["macd_signal"] = _ema(macd, 9).round(6)
    df["macd_hist"]   = (df["macd"] - df["macd_signal"]).round(6)
    return df