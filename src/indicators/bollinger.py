import pandas as pd
import numpy as np
from .ma import _ma

def calc_bollinger(df: pd.DataFrame, period: int = 20, num_std: float = 2.0) -> pd.DataFrame:
    close = df["close_price"]
    middle = _ma(close, period)
    std = close.rolling(period, min_periods=period).std(ddof=0)

    upper = middle + num_std * std
    lower = middle - num_std * std

    df["bb_middle"] = middle.round(4)
    df["bb_upper"]  = upper.round(4)
    df["bb_lower"]  = lower.round(4)
    df["bb_width"]  = ((upper - lower) / middle.replace(0, np.nan)).round(6)
    return df