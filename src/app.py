# app.py  —  QuantStock Streamlit UI
# ════════════════════════════════════════════════════════════════════════════
# streamlit run app.py
# ════════════════════════════════════════════════════════════════════════════

import json
import logging
import numpy as np
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, date as date_type

from matplotlib import pyplot as plt
from sqlalchemy import text
from streamlit_lightweight_charts import renderLightweightCharts

from DatabaseHandler import DatabaseHandler,logger
from api_client import SSIAPIClient
from transformer import DataTransformer
from sync_service import SyncService
from gap_service import GapRepairService
from indicator_service import IndicatorService
from signal_service import SignalService
from pnf_services import PNFService

import config

# ════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG
# ════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="QuantStock",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    [data-testid="metric-container"] {
        background:#f8f9fa; border-radius:8px;
        padding:10px 14px; border:1px solid #e9ecef;
    }
    .block-container { padding-top:1.2rem; }
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════════════════
# LOGGING — capture vào session_state, hiển thị trong st.status
# ════════════════════════════════════════════════════════════════════════════
class _SessionHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        if "log_messages" not in st.session_state:
            st.session_state.log_messages = []
        st.session_state.log_messages.append(msg)
        try:
            st.write(msg)
        except Exception:
            pass

def _setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in root.handlers[:]:
        if isinstance(h, _SessionHandler):
            root.removeHandler(h)
    h = _SessionHandler()
    h.setFormatter(logging.Formatter("%(asctime)s  %(levelname)s  %(message)s"))
    root.addHandler(h)

if "log_messages" not in st.session_state:
    st.session_state.log_messages = []
_setup_logging()


# ════════════════════════════════════════════════════════════════════════════
# SERVICES  (cache_resource — init 1 lần duy nhất)
# ════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def init_services():
    db          = DatabaseHandler()
    api_client  = SSIAPIClient(config)
    transformer = DataTransformer()
    sync_svc    = SyncService(api_client, db, transformer)
    gap_svc     = GapRepairService(db, sync_svc)
    ind_svc     = IndicatorService(db)
    sig_svc     = SignalService(db)

    return db, sync_svc, gap_svc, ind_svc, sig_svc

db, sync_service, gap_service, indicator_svc, signal_svc = init_services()


# ════════════════════════════════════════════════════════════════════════════
# DATA HELPERS
# ════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=3600)
def load_symbols() -> pd.DataFrame:
    try:
        with db.engine.connect() as conn:
            return pd.read_sql(
                text("SELECT symbol, stock_name, market FROM securities ORDER BY symbol"),
                conn,
            )
    except Exception:
        return pd.DataFrame(columns=["symbol", "stock_name", "market"])

def _fetch_price_with_warmup(symbol: str, start: date_type, end: date_type) -> pd.DataFrame:
    """Fetch giá với warmup 270 ngày lịch để MA200 hội tụ đúng."""
    warmup = start - timedelta(days=270)
    q = text("""
        SELECT trading_date, open_price, highest_price, lowest_price,
               close_price, close_price_adjusted, total_match_vol,foreign_buy_vol_total, foreign_sell_vol_total
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
        # Chuẩn hoá trading_date thành Python date — tránh lỗi so sánh Timestamp vs date
        df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.date
        return df
    except Exception as ex:
        logging.error(f"Lỗi fetch price {symbol}: {ex}")
        return pd.DataFrame()

def _fetch_signals_for_chart(symbol: str, start: date_type, end: date_type) -> pd.DataFrame:
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

def _fetch_indicator_data(symbol: str, start: date_type, end: date_type) -> pd.DataFrame:
    """Lấy dữ liệu indicators từ technical_indicators"""
    q = text("""
        SELECT trading_date, vol_ma20
        FROM technical_indicators
        WHERE symbol = :sym
          AND trading_date BETWEEN :s AND :e
        ORDER BY trading_date
    """)
    try:
        with db.engine.connect() as conn:
            df = pd.read_sql(q, conn, params={
                "sym": symbol,
                "s": start,
                "e": end
            })
        df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.date
        return df
    except Exception as e:
        logging.error(f"Lỗi lấy indicators cho {symbol}: {e}")
        return pd.DataFrame()

# ════════════════════════════════════════════════════════════════════════════
# CHART HELPERS
# ════════════════════════════════════════════════════════════════════════════
_MA_COLORS  = {
    "MA5" : "#3b82f6",
    "MA10": "#8b5cf6",
    "MA20": "#f59e0b",
    "MA50": "#10b981",
    "MA200": "#f43f5e",
}
_MA_PERIODS = {"MA5": 5, "MA10": 10, "MA20": 20, "MA50": 50, "MA200": 200}

def _compute_adj_prices(raw: pd.DataFrame) -> pd.DataFrame:
    """Thêm cột adj_* và cột time string. Không sửa raw gốc."""
    df = raw.copy()
    factor       = (df["close_price_adjusted"] / df["close_price"]).fillna(1.0)
    df["adj_open"]  = (df["open_price"]    * factor).round(2)
    df["adj_high"]  = (df["highest_price"] * factor).round(2)
    df["adj_low"]   = (df["lowest_price"]  * factor).round(2)
    df["adj_close"] = df["close_price_adjusted"].round(2)
    df["time"]      = df["trading_date"].apply(lambda d: d.strftime("%Y-%m-%d"))
    return df

def _build_ma_series(raw_adj: pd.DataFrame, selected_mas: list, start: date_type) -> list:
    """Tính MA từ adj_close, chỉ render từ start trở đi (warmup đã hội tụ)."""
    series = []
    adj   = raw_adj["adj_close"]
    dates = raw_adj["trading_date"]
    for ma in selected_mas:
        n    = _MA_PERIODS[ma]
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
                "color"           : _MA_COLORS[ma],
                "lineWidth"       : 1,
                "priceLineVisible": False,
                "lastValueVisible": True,
                "title"           : ma,
                "priceFormat"     : {"type": "price", "precision": 2, "minMove": 0.01},
            },
        })
    return series

def _build_markers(sig_df: pd.DataFrame) -> list:
    markers = []
    for _, r in sig_df.iterrows():
        buy = r["signal_direction"] == "BUY"
        markers.append({
            "time"    : r["signal_date"].strftime("%Y-%m-%d"),
            "position": "belowBar" if buy else "aboveBar",
            "color"   : "#22c55e" if buy else "#ef4444",
            "shape"   : "arrowUp" if buy else "arrowDown",
            "text"    : r["signal_type"].replace("_", " "),
            "size"    : max(1, min(int(float(r["strength"]) * 3), 3)),
        })
    return sorted(markers, key=lambda m: m["time"])

def _render_chart(price_df: pd.DataFrame, ma_series: list, markers: list, chart_type: str, key: str):
    """Render lightweight-charts: panel giá + panel volume."""
    bg   = {"type": "solid", "color": "#ffffff"}
    grid = {"vertLines": {"color": "#f0f0f0"}, "horzLines": {"color": "#f0f0f0"}}

    # --- Dữ liệu Volume MA20 từ price_df  ---
    ma_vol_series = price_df[['time', 'vol_ma20']].dropna()
    ma_vol_data = [
        {"time": row['time'], "value": float(row['vol_ma20'])}
        for _, row in ma_vol_series.iterrows()
    ]

    if chart_type == "Nến (Candlestick)":
        main_data = [
            {"time": r["time"], "open": r["adj_open"],
             "high": r["adj_high"], "low": r["adj_low"], "close": r["adj_close"]}
            for _, r in price_df.iterrows()
        ]
        main_series = {
            "type"   : "Candlestick",
            "data"   : main_data,
            "markers": markers,
            "options": {
                "upColor":"#26a69a","downColor":"#ef5350",
                "borderUpColor":"#26a69a","borderDownColor":"#ef5350",
                "wickUpColor":"#26a69a","wickDownColor":"#ef5350",
                "priceFormat": {"type":"price","precision":2,"minMove":0.01},
            },
        }
    else:
        main_data = [{"time": r["time"], "value": r["adj_close"]}
                     for _, r in price_df.iterrows()]
        main_series = {
            "type"   : "Line",
            "data"   : main_data,
            "markers": markers,
            "options": {
                "color":"#2962ff","lineWidth":2,
                "priceFormat":{"type":"price","precision":2,"minMove":0.01},
            },
        }

    vol_data = [
        {
            "time" : r["time"],
            "value": float(r["total_match_vol"]),
            "color": ("rgba(38,166,154,0.5)" if r["adj_close"] >= r["adj_open"]
                      else "rgba(239,83,80,0.5)"),
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
                    "data": vol_data,   # vẫn dùng vol_data như cũ
                    "options": {"priceFormat":{"type":"volume"},"priceScaleId":""},
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
                        "priceScaleId": ""
                    },
                },
            ],
        },
    ]
    renderLightweightCharts(charts, key=key)

# ════════════════════════════════════════════════════════════════════════════
# LAYOUT
# ════════════════════════════════════════════════════════════════════════════
st.title("📈 QuantStock")
symbols_df = load_symbols()
has_data   = not symbols_df.empty

tab1, tab2, tab3, tab4 = st.tabs([
    "Data", "Chart", "Signals", "🧮 Upcoming",
])

# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — ĐỒNG BỘ DỮ LIỆU
# ════════════════════════════════════════════════════════════════════════════
with tab1:
    st.info(f"🖥️ **Database:** `{db.engine.url.host}` | **Schema:** `{db.engine.url.database}`")

    # --- PHẦN 1: ĐỒNG BỘ DỮ LIỆU THÔ ---
    st.subheader("1. Đồng bộ dữ liệu")
    col1, col2 = st.columns([2, 1])
    with col1:
        func = st.selectbox("Tác vụ đồng bộ", [
            "Đồng bộ danh mục securities (tất cả sàn)",
            "Đồng bộ 1 mã OHLC", "Đồng bộ tất cả mã OHLC",
            "Đồng bộ 1 mã giá chi tiết", "Đồng bộ tất cả mã giá chi tiết",
            "Bảo trì (cập nhật thiếu)", "Vá lỗ hổng dữ liệu",
        ], key="sys_sync_func")
    with col2:
        t1_market = st.selectbox("Sàn", ["HOSE", "HNX", "UPCOM"], key="sys_market")

    # Dynamic inputs dựa trên func
    t1_from = datetime(2021, 1, 1).date()
    t1_to = (datetime.now() - timedelta(days=1)).date()

    c_a, c_b, c_c = st.columns(3)
    if "1 mã" in func:
        with c_a: t1_symbol = st.text_input("Mã chứng khoán", value="SSI", key="sys_sym")

    if "OHLC" in func or "giá chi tiết" in func:
        with c_b: t1_from = st.date_input("Từ ngày", value=datetime(2021, 1, 1), key="sys_from")
        with c_c: t1_to = st.date_input("Đến ngày", value=datetime.now() - timedelta(days=1), key="sys_to")

    if func == "Bảo trì (cập nhật thiếu)":
        with c_b: t1_mode = st.selectbox("Loại dữ liệu", ["ohlc", "price"], key="sys_maint_mode")

    if st.button("▶️ Chạy Đồng bộ", type="primary", use_container_width=True):
        st.session_state.log_messages = []
        with st.status(f"Đang thực hiện: {func}...", expanded=True) as status:
            try:
                f, t = t1_from.strftime("%d/%m/%Y"), t1_to.strftime("%d/%m/%Y")
                s = t1_symbol.strip().upper() if 't1_symbol' in locals() else ""

                if func == "Đồng bộ danh mục securities (tất cả sàn)":
                    sync_service.sync_all_markets()
                elif func == "Đồng bộ 1 mã OHLC":
                    sync_service.sync_one_ohlc(s, f, t)
                elif func == "Đồng bộ tất cả mã OHLC":
                    sync_service.sync_all_ohlc(t1_market, f, t)
                elif func == "Đồng bộ 1 mã giá chi tiết":
                    sync_service.sync_one_stock_price(s, f, t)
                elif func == "Đồng bộ tất cả mã giá chi tiết":
                    sync_service.sync_all_stock_prices(t1_market, f)
                elif func == "Bảo trì (cập nhật thiếu)":
                    sync_service.maintenance_sync(t1_market, t1_mode)
                elif func == "Vá lỗ hổng dữ liệu":
                    gap_service.repair_all_gaps(t1_market)

                status.update(label="✅ Đồng bộ hoàn tất!", state="complete", expanded=False)
                st.cache_data.clear()
            except Exception as e:
                logging.exception(e)
                status.update(label=f"❌ Lỗi: {e}", state="error")

    st.markdown("---")

    # --- PHẦN 2: INDICATORS & SIGNALS ---
    st.subheader("2. Xử lý Indicators & Signals")
    col_ind, col_sig = st.columns(2)

    with col_ind:
        st.markdown("#### 🧮 Tính Indicators")
        i_mode = st.radio("Chế độ Indicators", ["Bảo trì (thiếu)", "1 mã", "Toàn sàn"], horizontal=True, key="i_m")
        i_mkt = st.selectbox("Sàn", ["HOSE", "HNX", "UPCOM"], key="i_mkt")
        i_sym = st.text_input("Mã", value="SSI", key="i_s") if i_mode == "1 mã" else ""
        i_date = st.date_input("Từ ngày (trống=all)", value=None, key="i_d") if i_mode != "Bảo trì (thiếu)" else None

        if st.button("Tính Indicators", use_container_width=True):
            with st.status("Đang tính...") as s:
                fd = i_date.strftime("%Y-%m-%d") if i_date else None
                if i_mode == "Bảo trì (thiếu)":
                    indicator_svc.run_maintenance(i_mkt)
                elif i_mode == "1 mã":
                    indicator_svc.run_one(i_sym.upper(), fd)
                else:
                    indicator_svc.run_all(i_mkt, fd)
                s.update(label="✅ Xong", state="complete")

    with col_sig:
        st.markdown("#### 🔔 Phát hiện Signals")
        s_mode = st.radio("Chế độ Signals", ["Bảo trì (thiếu)", "1 mã", "Toàn sàn"], horizontal=True, key="s_m")
        s_mkt = st.selectbox("Sàn", ["HOSE", "HNX", "UPCOM"], key="s_mkt")
        s_sym = st.text_input("Mã", value="SSI", key="s_s") if s_mode == "1 mã" else ""
        s_date = st.date_input("Từ ngày (trống=all)", value=None, key="s_d") if s_mode != "Bảo trì (thiếu)" else None

        if st.button("Tìm Signals", use_container_width=True):
            with st.status("Đang quét...") as s:
                fd = s_date.strftime("%Y-%m-%d") if s_date else None
                if s_mode == "Bảo trì (thiếu)":
                    signal_svc.run_maintenance(s_mkt)
                elif s_mode == "1 mã":
                    signal_svc.run_one(s_sym.upper(), fd)
                else:
                    signal_svc.run_all(s_mkt, fd)
                s.update(label="✅ Xong", state="complete")

    with st.expander("Logs hệ thống"):
        st.code("\n".join(st.session_state.get("log_messages", [])) or "Chưa có log.")
# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — BIỂU ĐỒ
# ════════════════════════════════════════════════════════════════════════════
with tab2:
    if not has_data:
        # Không dùng st.stop() — chỉ hiện warning, các tab khác vẫn chạy bình thường
        st.warning("Chưa có dữ liệu. Hãy đồng bộ danh mục securities ở Tab Đồng bộ trước.")
    else:
        # ── Controls ─────────────────────────────────────────────
        c1, c2, c3, c4, c5 = st.columns([0.80, 1, 0.6, 1.3, 1.7])

        with c1:
            sym_list = symbols_df["symbol"].tolist()
            default_ix = sym_list.index("SSI") if "SSI" in sym_list else 0
            t2_symbol = st.selectbox("Mã chứng khoán", sym_list, index=default_ix, key="t2_sym")
        with c2:
            t2_chart_type = st.selectbox("Loại biểu đồ",
                                         ["Nến (Candlestick)", "Đường (Close)"], key="t2_chart_type")
        with c3:
            t2_period = st.selectbox("Chu kỳ",
                                     ["1 tháng", "3 tháng", "6 tháng", "1 năm", "2 năm", "Toàn bộ"],
                                     index=3, key="t2_period")
        with c4:
            t2_mas = st.multiselect("Đường MA overlay",
                                    ["MA5", "MA10", "MA20", "MA50", "MA200"],
                                    default=["MA20", "MA50"], key="t2_mas")
        with c5:
            ALL_SIGNAL_TYPES = [
                "MA_GOLDEN_CROSS", "MA_DEATH_CROSS", "RSI_OVERSOLD", "RSI_OVERBOUGHT",
                "MACD_BULLISH_CROSS", "MACD_BEARISH_CROSS",
                "BB_SQUEEZE_BREAKOUT_UP", "BB_SQUEEZE_BREAKOUT_DOWN",
                "VOLUME_SPIKE", "FOREIGN_ACCUMULATION", "FOREIGN_DISTRIBUTION"
            ]
            t2_sig_filter = st.multiselect(
                "Hiển thị tín hiệu",
                ALL_SIGNAL_TYPES,
                default=['MA_GOLDEN_CROSS'],  # mặc định bật toàn bộ
                key="t2_sig_filter"
                    )

        st.markdown("P&F Settings")

        cA, cB, cC, cD = st.columns(4)

        with cA:
            pnf_method = st.selectbox("Method",
                                      ["h/l", "ohlc", "l/h", "hlc", "cl"],
                                      key="pnf_method")

        with cB:
            pnf_reversal = st.number_input("Reversal", 1, 5, 3, key="pnf_rev")

        with cC:
            pnf_scaling = st.selectbox("Scaling",
                                       ["log", "abs", "cla", "atr"],
                                       key="pnf_scaling")

        with cD:
            pnf_boxsize = st.number_input("Boxsize", value=2.0, key="pnf_bs")

        pnf_show_bo = st.checkbox("Breakouts", True)
        pnf_show_tl = st.checkbox("Trendlines", False)

        # ── Date range ───────────────────────────────────────────
        today = datetime.now().date()
        period_days = {
            "1 tháng": 30, "3 tháng": 90, "6 tháng": 180,
            "1 năm": 365, "2 năm": 730, "Toàn bộ": 3650,
        }
        start_date = today - timedelta(days=period_days[t2_period])
        t2_show_sig = len(t2_sig_filter) > 0

        # ── Fetch & process ──────────────────────────────────────
        raw_df = _fetch_price_with_warmup(t2_symbol, start_date, today)

        if raw_df.empty:
            st.warning(f"Không có dữ liệu giá cho {t2_symbol}.")
        else:
            raw_adj  = _compute_adj_prices(raw_df)
            price_df = raw_adj[raw_adj["trading_date"] >= start_date].reset_index(drop=True)
            ind_df = _fetch_indicator_data(t2_symbol, start_date, today)

            if not ind_df.empty:
                price_df = price_df.merge(ind_df, on="trading_date", how="left")
            else:
                # Nếu không có indicator, tạo cột vol_ma20 rỗng để tránh lỗi
                price_df["vol_ma20"] = np.nan

            if price_df.empty:
                st.warning("Không đủ dữ liệu sau khi filter theo ngày.")
            else:
                # Metric cards
                last = price_df.iloc[-1]
                prev = price_df.iloc[-2] if len(price_df) > 1 else last
                chg     = float(last["adj_close"]) - float(prev["adj_close"])
                chg_pct = chg / float(prev["adj_close"]) * 100 if prev["adj_close"] else 0

                info_row   = symbols_df[symbols_df["symbol"] == t2_symbol]
                stock_name = info_row["stock_name"].values[0] if not info_row.empty else t2_symbol

                st.markdown(
                    f"#### {t2_symbol} &nbsp;"
                    f"<span style='font-size:14px;color:#6b7280'>{stock_name}</span>",
                    unsafe_allow_html=True,
                )
                # Hiển thị thông tin
                m1, m2, m3, m4, m5, m6 = st.columns(6)
                m1.metric("Đóng cửa",   f"{last['adj_close']:,.0f}",
                                         f"{chg:+.2f} ({chg_pct:+.0f}%)")
                m2.metric("Cao nhất",   f"{last['adj_high']:,.0f}")
                m3.metric("Thấp nhất",  f"{last['adj_low']:,.0f}")
                m4.metric("Khối lượng", f"{last['total_match_vol']/1e6:.2f}M")
                m5.metric("Khối ngoại mua", f"{last['foreign_buy_vol_total'] / 1e6:.2f}M")
                m6.metric("Khối ngoại bán", f"{last['foreign_sell_vol_total'] / 1e6:.2f}M")

                # MA series & signal markers
                ma_series = _build_ma_series(raw_adj, t2_mas, start_date) if t2_mas else []

                sig_df = pd.DataFrame()
                if t2_show_sig:
                    sig_df = _fetch_signals_for_chart(t2_symbol, start_date, today)
                    if not sig_df.empty:
                        sig_df = sig_df[sig_df["signal_type"].isin(t2_sig_filter)]

                markers = _build_markers(sig_df) if not sig_df.empty else []

                # ═══ Layout: Biểu đồ (trái) + Tín hiệu (phải) ═══
                col_left, col_right = st.columns([3, 2])

                with col_left:
                    chart_key = (f"c_{t2_symbol}_{start_date}_{t2_chart_type}"
                                 f"_{''.join(t2_mas)}_{t2_show_sig}")
                    _render_chart(price_df, ma_series, markers, t2_chart_type, chart_key)

                    # MA Legend
                    if t2_mas:
                        parts = []
                        for ma in t2_mas:
                            n = _MA_PERIODS[ma]
                            vals = raw_adj["adj_close"].rolling(n, min_periods=n).mean().dropna()
                            v = f"{vals.iloc[-1]:,.2f}" if not vals.empty else "—"
                            parts.append(
                                f"<span style='background:{_MA_COLORS[ma]};color:#fff;"
                                f"padding:2px 9px;border-radius:10px;"
                                f"font-size:12px;margin:2px'>{ma}: {v}</span>"
                            )
                        st.markdown(" ".join(parts), unsafe_allow_html=True)

                with col_right:
                    with st.spinner("Đang tính toán P&F..."):
                        try:
                            pnf_svc = PNFService(db)

                            chart = pnf_svc.build_chart(
                                t2_symbol,  # dùng chung symbol
                                method=pnf_method,
                                reversal=pnf_reversal,
                                boxsize=pnf_boxsize,
                                scaling=pnf_scaling
                            )

                            fig = PNFService.get_plot(chart,
                                show_breakouts=pnf_show_bo,
                                show_trendlines=pnf_show_tl
                            )

                            st.pyplot(fig)
                            plt.close(fig)

                        except Exception as e:
                            st.error(f"Lỗi P&F: {e}")

                st.markdown("Signals")
                col_signal1,col_signal2 = st.columns(2)
                with col_signal1:
                    if t2_show_sig and not sig_df.empty:
                        st.markdown(f"#### Có {len(sig_df)} tín hiệu")
                        t_disp = sig_df[["signal_date", "signal_type", "signal_direction",
                                         "strength", "close_price"]].copy()
                        t_disp.columns = ["Ngày", "Loại tín hiệu", "Chiều", "Strength", "Giá"]
                        t_disp = t_disp.sort_values("Ngày", ascending=False).reset_index(drop=True)
                        st.dataframe(
                            t_disp.style.format({"Strength": "{:.2%}", "Giá": "{:,.2f}"}),
                            use_container_width=True, height=320,
                        )
                    elif t2_show_sig:
                        st.info("Không có tín hiệu nào trong kỳ.")
                        
# ════════════════════════════════════════════════════════════════════════════
# TAB 3 — SIGNALS SCREENER
# ════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("🔔 Screener tín hiệu giao dịch")

    f1, f2, f3, f4, f5 = st.columns([1.2, 1, 1, 1.5, 1])
    with f1:
        scr_market    = st.selectbox("Sàn", ["HOSE","HNX","UPCOM","Tất cả"], key="scr_mkt")
    with f2:
        scr_direction = st.selectbox("Chiều", ["Tất cả","BUY","SELL"], key="scr_dir")
    with f3:
        scr_strength  = st.slider("Strength tối thiểu", 0.0, 1.0, 0.2, 0.05, key="scr_str")
    with f4:
        scr_types = st.multiselect(
            "Loại tín hiệu (trống = tất cả)",
            ["MA_GOLDEN_CROSS","MA_DEATH_CROSS","RSI_OVERSOLD","RSI_OVERBOUGHT",
             "MACD_BULLISH_CROSS","MACD_BEARISH_CROSS",
             "BB_SQUEEZE_BREAKOUT_UP","BB_SQUEEZE_BREAKOUT_DOWN",
             "VOLUME_SPIKE","FOREIGN_ACCUMULATION","FOREIGN_DISTRIBUTION"],
            default=[], key="scr_types",
        )
    with f5:
        scr_date = st.date_input("Ngày (trống = mới nhất)", value=None, key="scr_date")

    if st.button("🔍 Tìm tín hiệu", type="primary", key="scr_search"):
        with st.spinner("Đang truy vấn…"):
            try:
                result = signal_svc.get_latest_signals(
                    market       = None if scr_market == "Tất cả" else scr_market,
                    date         = scr_date.strftime("%Y-%m-%d") if scr_date else None,
                    direction    = None if scr_direction == "Tất cả" else scr_direction,
                    min_strength = scr_strength,
                    signal_types = scr_types or None,
                    limit        = 300,
                )
                st.session_state["scr_result"] = result
            except Exception as e:
                st.error(f"Lỗi truy vấn: {e}")
                st.session_state["scr_result"] = pd.DataFrame()

    result = st.session_state.get("scr_result", pd.DataFrame())

    if result.empty:
        st.info("Chưa có kết quả. Nhấn 'Tìm tín hiệu' để bắt đầu.")
    else:
        # Summary metrics
        buy_n  = int((result["signal_direction"] == "BUY").sum())
        sell_n = int((result["signal_direction"] == "SELL").sum())
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Tổng tín hiệu", len(result))
        s2.metric("🟢 BUY",  buy_n)
        s3.metric("🔴 SELL", sell_n)
        s4.metric("Strength TB", f"{result['strength'].mean():.2f}")

        # Phân bổ bar chart
        type_counts = (result
                       .groupby(["signal_type","signal_direction"])
                       .size().reset_index(name="count"))
        if not type_counts.empty:
            with st.expander("Phân bổ theo loại tín hiệu", expanded=True):
                pivot = (type_counts
                         .pivot(index="signal_type", columns="signal_direction", values="count")
                         .fillna(0))
                st.bar_chart(pivot)

        # Bảng chính
        st.markdown("---")
        want_cols = ["signal_date","symbol","stock_name","market",
                     "signal_type","signal_direction","strength","close_price"]
        avail     = [c for c in want_cols if c in result.columns]
        disp      = result[avail].copy().rename(columns={
            "signal_date"     : "Ngày",
            "symbol"          : "Mã",
            "stock_name"      : "Tên",
            "market"          : "Sàn",
            "signal_type"     : "Loại tín hiệu",
            "signal_direction": "Chiều",
            "strength"        : "Strength",
            "close_price"     : "Giá",
        })
        disp = disp.sort_values(["Ngày","Strength"], ascending=[False,False]).reset_index(drop=True)

        # Highlight cột Chiều bằng .map (không dùng .apply axis=1 để tránh bug pandas-styler)
        def _dir_color(val):
            if val == "BUY":
                return "background-color:#d1fae5;color:#065f46;font-weight:600"
            if val == "SELL":
                return "background-color:#fee2e2;color:#991b1b;font-weight:600"
            return ""

        styled = disp.style.format({
            "Strength": "{:.2%}",
            "Giá"     : "{:,.2f}",
        })
        if "Chiều" in disp.columns:
            styled = styled.map(_dir_color, subset=["Chiều"])

        st.dataframe(styled, use_container_width=True, height=460)

        # Detail JSON
        with st.expander("Chi tiết parameters (JSON)"):
            syms = result["symbol"].unique().tolist()
            if syms:
                sel   = st.selectbox("Chọn mã:", syms, key="scr_detail")
                rows  = result[result["symbol"] == sel]
                for _, r in rows.iterrows():
                    color = "#22c55e" if r["signal_direction"] == "BUY" else "#ef4444"
                    try:
                        params = (json.loads(r["parameters"])
                                  if isinstance(r["parameters"], str)
                                  else r["parameters"])
                    except Exception:
                        params = str(r.get("parameters", ""))
                    st.markdown(
                        f"<div style='border-left:4px solid {color};padding:8px 14px;"
                        f"margin:4px 0;background:#f9fafb;border-radius:0 8px 8px 0'>"
                        f"<b>{r['signal_date']}</b> &nbsp; {r['signal_type']} &nbsp;"
                        f"<span style='color:{color}'>{r['signal_direction']}</span> &nbsp;"
                        f"strength={float(r['strength']):.2f} &nbsp; "
                        f"giá={float(r['close_price']):,.2f}</div>",
                        unsafe_allow_html=True,
                    )
                    st.json(params)
# ════════════════════════════════════════════════════════════════════════════
# TAB 4 —
# ════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("DEV")
