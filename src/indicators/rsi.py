import pandas as pd
import numpy as np

def calc_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    delta = df["close_price"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    avg_gain = gain.ewm(com=period - 1, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi14"] = (100 - 100 / (1 + rs)).round(4)
    return df