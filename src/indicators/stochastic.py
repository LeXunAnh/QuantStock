import pandas as pd
import numpy as np
from .ma import _ma

def calc_stochastic(df: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> pd.DataFrame:
    low_min  = df["lowest_price"].rolling(k_period, min_periods=k_period).min()
    high_max = df["highest_price"].rolling(k_period, min_periods=k_period).max()

    denom = (high_max - low_min).replace(0, np.nan)
    stoch_k = ((df["close_price"] - low_min) / denom * 100)

    df["stoch_k"] = stoch_k.round(4)
    df["stoch_d"] = _ma(stoch_k, d_period).round(4)
    return df