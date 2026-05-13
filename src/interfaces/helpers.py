from __future__ import annotations

import logging
from datetime import date as date_type, timedelta

import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy import text
from streamlit_lightweight_charts import renderLightweightCharts

# ── Constants ────────────────────────────────────────────────────────────────

MA_COLORS: dict[str, str] = {
    "MA5": "#3b82f6",
    "MA10": "#8b5cf6",
    "MA20": "#f59e0b",
    "MA50": "#10b981",
    "MA200": "#f43f5e",
}

MA_PERIODS: dict[str, int] = {
    "MA5": 5, "MA10": 10, "MA20": 20, "MA50": 50, "MA200": 200,
}

ALL_SIGNAL_TYPES: list[str] = [
    "MA_GOLDEN_CROSS", "MA_DEATH_CROSS",
    "RSI_OVERSOLD", "RSI_OVERBOUGHT",
    "MACD_BULLISH_CROSS", "MACD_BEARISH_CROSS",
    "BB_SQUEEZE_BREAKOUT_UP", "BB_SQUEEZE_BREAKOUT_DOWN",
    "VOLUME_SPIKE", "FOREIGN_ACCUMULATION", "FOREIGN_DISTRIBUTION",
]


# ── Logging ──────────────────────────────────────────────────────────────────

class _SessionHandler(logging.Handler):
    """Ghi log vào st.session_state và thử st.write (trong st.status context)."""

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        if "log_messages" not in st.session_state:
            st.session_state.log_messages = []
        st.session_state.log_messages.append(msg)
        try:
            st.write(msg)
        except Exception:
            pass


def setup_logging() -> None:
    """Đăng ký _SessionHandler vào root logger (idempotent)."""
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in root.handlers[:]:
        if isinstance(h, _SessionHandler):
            root.removeHandler(h)
    handler = _SessionHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)s  %(message)s")
    )
    root.addHandler(handler)


# ── Data helpers ─────────────────────────────────────────────────────────────

def fetch_price_with_warmup(
        db,
        symbol: str,
        start: date_type,
        end: date_type,
) -> pd.DataFrame:
    """Fetch giá với warmup 270 ngày lịch để MA200 hội tụ đúng."""
    warmup = start - timedelta(days=270)
    q = text("""
        SELECT trading_date, open_price, highest_price, lowest_price,
               close_price, close_price_adjusted, total_match_vol,
               foreign_buy_vol_total, foreign_sell_vol_total
        FROM daily_stock_prices
        WHERE symbol = :sym
          AND trading_date BETWEEN :s AND :e
          AND close_price > 0
          AND close_price_adjusted IS NOT NULL
        ORDER BY trading_date
    """)
    try:
        with db.engine.connect() as conn:
            df = pd.read_sql(q, conn, params={"sym": symbol, "s": warmup, "e": end})
        df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.date
        return df
    except Exception as ex:
        logging.error(f"Lỗi fetch price {symbol}: {ex}")
        return pd.DataFrame()


def fetch_signals_for_chart(
        db,
        symbol: str,
        start: date_type,
        end: date_type,
) -> pd.DataFrame:
    q = text("""
        SELECT signal_date, signal_type, signal_direction, strength, close_price
        FROM trading_signals
        WHERE symbol = :sym AND signal_date BETWEEN :s AND :e
        ORDER BY signal_date
    """)
    try:
        with db.engine.connect() as conn:
            df = pd.read_sql(q, conn, params={"sym": symbol, "s": start, "e": end})
        df["signal_date"] = pd.to_datetime(df["signal_date"]).dt.date
        return df
    except Exception:
        return pd.DataFrame()


def fetch_indicator_data(
        db,
        symbol: str,
        start: date_type,
        end: date_type,
) -> pd.DataFrame:
    q = text("""
        SELECT trading_date, vol_ma20
        FROM technical_indicators
        WHERE symbol = :sym
          AND trading_date BETWEEN :s AND :e
        ORDER BY trading_date
    """)
    try:
        with db.engine.connect() as conn:
            df = pd.read_sql(q, conn, params={"sym": symbol, "s": start, "e": end})
        df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.date
        return df
    except Exception as e:
        logging.error(f"Lỗi lấy indicators cho {symbol}: {e}")
        return pd.DataFrame()


# ── Chart builders ───────────────────────────────────────────────────────────

def compute_adj_prices(raw: pd.DataFrame) -> pd.DataFrame:
    """Thêm cột adj_* và cột time string. Không sửa raw gốc."""
    df = raw.copy()
    factor = (df["close_price_adjusted"] / df["close_price"]).fillna(1.0)
    df["adj_open"] = (df["open_price"] * factor).round(2)
    df["adj_high"] = (df["highest_price"] * factor).round(2)
    df["adj_low"] = (df["lowest_price"] * factor).round(2)
    df["adj_close"] = df["close_price_adjusted"].round(2)
    df["time"] = df["trading_date"].apply(lambda d: d.strftime("%Y-%m-%d"))
    return df


def build_ma_series(
        raw_adj: pd.DataFrame,
        selected_mas: list[str],
        start: date_type,
) -> list[dict]:
    """Tính MA từ adj_close, chỉ render từ start trở đi (warmup đã hội tụ)."""
    series = []
    adj = raw_adj["adj_close"]
    dates = raw_adj["trading_date"]
    for ma in selected_mas:
        n = MA_PERIODS[ma]
        vals = adj.rolling(n, min_periods=n).mean().round(2)
        data = [
            {"time": dates.iloc[i].strftime("%Y-%m-%d"), "value": float(vals.iloc[i])}
            for i in range(len(raw_adj))
            if pd.notna(vals.iloc[i]) and dates.iloc[i] >= start
        ]
        if not data:
            continue
        series.append({
            "type": "Line",
            "data": data,
            "options": {
                "color": MA_COLORS[ma],
                "lineWidth": 1,
                "priceLineVisible": False,
                "lastValueVisible": True,
                "title": ma,
                "priceFormat": {"type": "price", "precision": 2, "minMove": 0.01},
            },
        })
    return series


def build_markers(sig_df: pd.DataFrame) -> list[dict]:
    markers = []
    for _, r in sig_df.iterrows():
        buy = r["signal_direction"] == "BUY"
        markers.append({
            "time": r["signal_date"].strftime("%Y-%m-%d"),
            "position": "belowBar" if buy else "aboveBar",
            "color": "#22c55e" if buy else "#ef4444",
            "shape": "arrowUp" if buy else "arrowDown",
            "text": r["signal_type"].replace("_", " "),
            "size": max(1, min(int(float(r["strength"]) * 3), 3)),
        })
    return sorted(markers, key=lambda m: m["time"])


def render_price_chart(
        price_df: pd.DataFrame,
        ma_series: list[dict],
        markers: list[dict],
        chart_type: str,
        key: str,
) -> None:
    """Render lightweight-charts: panel giá + panel volume."""
    bg = {"type": "solid", "color": "#ffffff"}
    grid = {"vertLines": {"color": "#f0f0f0"}, "horzLines": {"color": "#f0f0f0"}}

    ma_vol_data = [
        {"time": row["time"], "value": float(row["vol_ma20"])}
        for _, row in price_df[["time", "vol_ma20"]].dropna().iterrows()
    ]

    if chart_type == "Nến (Candlestick)":
        main_data = [
            {
                "time": r["time"],
                "open": r["adj_open"],
                "high": r["adj_high"],
                "low": r["adj_low"],
                "close": r["adj_close"],
            }
            for _, r in price_df.iterrows()
        ]
        main_series = {
            "type": "Candlestick",
            "data": main_data,
            "markers": markers,
            "options": {
                "upColor": "#26a69a", "downColor": "#ef5350",
                "borderUpColor": "#26a69a", "borderDownColor": "#ef5350",
                "wickUpColor": "#26a69a", "wickDownColor": "#ef5350",
                "priceFormat": {"type": "price", "precision": 2, "minMove": 0.01},
            },
        }
    else:
        main_data = [
            {"time": r["time"], "value": r["adj_close"]}
            for _, r in price_df.iterrows()
        ]
        main_series = {
            "type": "Line",
            "data": main_data,
            "markers": markers,
            "options": {
                "color": "#2962ff",
                "lineWidth": 2,
                "priceFormat": {"type": "price", "precision": 2, "minMove": 0.01},
            },
        }

    vol_data = [
        {
            "time": r["time"],
            "value": float(r["total_match_vol"]),
            "color": (
                "rgba(38,166,154,0.5)"
                if r["adj_close"] >= r["adj_open"]
                else "rgba(239,83,80,0.5)"
            ),
        }
        for _, r in price_df.iterrows()
    ]

    charts = [
        {
            "chart": {
                "height": 440,
                "layout": {"background": bg, "textColor": "#333"},
                "grid": grid,
                "crosshair": {"mode": 1},
                "timeScale": {"borderColor": "#d1d5db", "rightOffset": 8},
                "rightPriceScale": {"borderColor": "#d1d5db"},
            },
            "series": [main_series] + ma_series,
        },
        {
            "chart": {
                "height": 100,
                "layout": {"background": bg, "textColor": "#333"},
                "grid": grid,
                "timeScale": {"borderColor": "#d1d5db", "visible": False},
                "rightPriceScale": {
                    "borderColor": "#d1d5db",
                    "scaleMargins": {"top": 0.05, "bottom": 0},
                },
            },
            "series": [
                {
                    "type": "Histogram",
                    "data": vol_data,
                    "options": {"priceFormat": {"type": "volume"}, "priceScaleId": ""},
                },
                {
                    "type": "Line",
                    "data": ma_vol_data,
                    "options": {
                        "color": "#FF6D00",
                        "lineWidth": 2,
                        "priceLineVisible": False,
                        "lastValueVisible": True,
                        "title": "MAVol20",
                        "priceFormat": {"type": "volume", "precision": 0},
                        "priceScaleId": "",
                    },
                },
            ],
        },
    ]
    renderLightweightCharts(charts, key=key)
