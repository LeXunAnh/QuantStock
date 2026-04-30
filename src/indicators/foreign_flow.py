import pandas as pd

def calc_volume_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tính các chỉ báo dòng tiền ngoại (foreign flow) từ dữ liệu net_buy_sell.
    - net_foreign_vol_5d  : Tổng khối lượng ròng 5 phiên
    - net_foreign_vol_10d : Tổng khối lượng ròng 10 phiên
    - net_foreign_val_5d  : Tổng giá trị ròng 5 phiên
    - net_foreign_val_10d : Tổng giá trị ròng 10 phiên
    """
    # Các cột cần tồn tại trong df
    if 'net_buy_sell_vol' not in df.columns or 'net_buy_sell_val' not in df.columns:
        raise KeyError("DataFrame thiếu cột 'net_buy_sell_vol' hoặc 'net_buy_sell_val'")

    vol = df['net_buy_sell_vol'].astype(float)
    val = df['net_buy_sell_val'].astype(float)

    # Tổng lũy kế 5 và 10 phiên
    df['net_foreign_vol_5d']  = vol.rolling(5, min_periods=1).sum().round(0)   # số nguyên
    df['net_foreign_vol_10d'] = vol.rolling(10, min_periods=1).sum().round(0)
    df['net_foreign_val_5d']  = val.rolling(5, min_periods=1).sum().round(2)
    df['net_foreign_val_10d'] = val.rolling(10, min_periods=1).sum().round(2)

    return df