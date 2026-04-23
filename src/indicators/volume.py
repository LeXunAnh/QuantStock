import pandas as pd
import numpy as np
from .ma import _ma

def calc_volume_indicators(df: pd.DataFrame) -> pd.DataFrame:
    vol   = df["total_traded_vol"].astype(float)
    close = df["close_price"]

    vol_ma20 = _ma(vol, 20)
    df["vol_ma20"]  = vol_ma20.round(2)
    df["vol_ratio"] = (vol / vol_ma20.replace(0, np.nan)).round(4)

    direction = np.sign(close.diff()).fillna(0)
    df["obv"]  = (direction * vol).cumsum().round(0)
    return df