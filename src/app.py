import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date
import logging, threading, time
from sqlalchemy import text
from streamlit_lightweight_charts import renderLightweightCharts


# Import các module
from DatabaseHandler import DatabaseHandler
from api_client import SSIAPIClient
from transformer import DataTransformer
from sync_service import SyncService
from gap_service import GapRepairService
import config

# ===================== CẤU HÌNH LOGGING CAPTURE =====================
class SessionStateHandler(logging.Handler):
    """Ghi log vào session_state để hiển thị trên UI"""
    def __init__(self, session_state_key="log_messages"):
        super().__init__()
        self.session_state_key = session_state_key

    def emit(self, record):
        log_entry = self.format(record)
        if self.session_state_key in st.session_state:
            st.session_state[self.session_state_key].append(log_entry)
            # Giới hạn chỉ giữ 500 dòng log cuối
            st.session_state[self.session_state_key] = st.session_state[self.session_state_key][-1000:]
        else:
            st.session_state[self.session_state_key] = [log_entry]

def setup_logging():
    """Thiết lập logging để capture vào session_state"""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    # Xóa handler cũ để tránh trùng lặp
    for handler in root_logger.handlers[:]:
        if isinstance(handler, SessionStateHandler):
            root_logger.removeHandler(handler)
    handler = SessionStateHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

# Khởi tạo session_state cho log
if "log_messages" not in st.session_state:
    st.session_state.log_messages = []
setup_logging()

# ===================== CACHE CÁC SERVICE =====================
@st.cache_resource
def init_services():
    """Khởi tạo các đối tượng service (chỉ một lần)"""
    db = DatabaseHandler()
    api_client = SSIAPIClient(config)
    transformer = DataTransformer()
    sync_service = SyncService(api_client, db, transformer)
    gap_service = GapRepairService(db, sync_service)
    return db, sync_service, gap_service

db, sync_service, gap_service = init_services()

# ===================== GIAO DIỆN STREAMLIT =====================
st.set_page_config(page_title="Stock Data Sync", layout="wide")
st.title("📈 Quản lý dữ liệu chứng khoán")

# Tạo 2 tab
tab1, tab2, tab3 = st.tabs(["Đồng bộ dữ liệu", "Biểu đồ","Developing"])

# --------------------- TAB 1: ĐỒNG BỘ DỮ LIỆU ---------------------
with tab1:
    st.header("Chạy tác vụ đồng bộ")

    # Chọn chức năng
    func = st.selectbox("Chọn chức năng", [
        "Đồng bộ danh mục securities (tất cả sàn)",
        "Đồng bộ 1 mã OHLC",
        "Đồng bộ tất cả mã OHLC",
        "Đồng bộ 1 giá chi tiết",
        "Đồng bộ tất cả mã giá chi tiết",
        "Bảo trì (cập nhật thiếu)",
        "Vá lỗ hổng dữ liệu"
    ])

    # Các tham số tùy chỉnh
    #market = st.text_input("Sàn (HOSE/HNX/UPCOM)", value="HOSE")
    col1, col2 = st.columns(2)
    with col1:
        market = st.selectbox("Sàn (HOSE/HNX/UPCOM)", ["HOSE", "HNX", "UPCOM"])
    with col2:
        symbol_input = st.text_input("Mã chứng khoán (nếu chọn đồng bộ 1 mã)", value="SSI")

    # Tham số phụ thuộc vào chức năng
    if func in ["Đồng bộ 1 mã OHLC", "Đồng bộ tất cả mã OHLC"]:
        col1, col2 = st.columns(2)
        with col1:
            from_date = st.date_input("Từ ngày", value=datetime(2015, 1, 1))
        with col2:
            to_date = st.date_input("Đến ngày", value=datetime.now() - timedelta(days=1))
    elif func in ["Đồng bộ 1 mã giá chi tiết", "Đồng bộ tất cả mã giá chi tiết"]:
        from_date = st.date_input("Từ ngày", value=datetime(2021, 1, 1))
    elif func == "Bảo trì (cập nhật thiếu)":
        mode = st.selectbox("Loại dữ liệu", ["ohlc", "price"])

    # Khởi tạo session_state cho task
    if "task_running" not in st.session_state: st.session_state.task_running = False
    if "task_error" not in st.session_state: st.session_state.task_error = None

    # Nút chạy
    if st.button("▶️ Chạy tác vụ", type="primary",disabled=st.session_state.task_running):
        st.session_state.log_messages = []
        st.session_state.task_running = True
        st.session_state.task_error = None

        def run_task():
            try:
                if func == "Đồng bộ danh mục securities (tất cả sàn)":
                    sync_service.sync_all_markets()
                elif func == "Đồng bộ 1 mã OHLC":
                    sync_service.sync_one_ohlc(
                        symbol_input.strip().upper(),
                        from_date=from_date.strftime('%d/%m/%Y'),
                        to_date=to_date.strftime('%d/%m/%Y')
                    )
                elif func == "Đồng bộ tất cả mã OHLC":
                    sync_service.sync_all_ohlc(
                        market=market,
                        from_date=from_date.strftime('%d/%m/%Y'),
                        to_date=to_date.strftime('%d/%m/%Y')
                    )
                elif func == "Đồng bộ 1 mã giá chi tiết":
                    sync_service.sync_one_stock_price(
                        symbol_input.strip().upper(),
                        from_date.strftime('%d/%m/%Y'),
                        to_date.strftime('%d/%m/%Y')
                    )
                elif func == "Đồng bộ tất cả mã giá chi tiết":
                    sync_service.sync_all_stock_prices(
                        market=market,
                        from_date=from_date.strftime('%d/%m/%Y')
                    )
                elif func == "Bảo trì (cập nhật thiếu)":
                    sync_service.maintenance_sync(market=market, mode=mode)
                elif func == "Vá lỗ hổng dữ liệu":
                    gap_service.repair_all_gaps(market=market)
            except Exception as e:
                st.session_state.task_error = str(e)
                logging.exception(e)
            finally:
                st.session_state.task_running = False


        thread = threading.Thread(target=run_task, daemon=True)
        thread.start()

    # Hiển thị log real-time
    st.subheader("📋 Log chi tiết")
    log_container = st.empty()

    if st.session_state.task_running:
        with st.spinner("Đang xử lý..."):
            while st.session_state.task_running:
                log_text = "\n".join(st.session_state.log_messages)
                log_container.text_area("Log", value=log_text, height=400,
                                        disabled=True, label_visibility="collapsed")
                time.sleep(0.5)
                st.rerun()
    else:
        if st.session_state.task_error:
            st.error(f"❌ Lỗi: {st.session_state.task_error}")
        elif st.session_state.log_messages:
            st.success("✅ Tác vụ hoàn tất!")
            log_text = "\n".join(st.session_state.log_messages)
            log_container.text_area("Log", value=log_text, height=400,
                                    disabled=True, label_visibility="collapsed")
        else:
            log_container.info("Chưa có log. Hãy chạy tác vụ để xem log.")

# --------------------- TAB 2: BIỂU ĐỒ ---------------------
with tab2:
    st.header("Hiển thị dữ liệu giá")

    # Lấy danh sách mã chứng khoán (chỉ công ty)
    try:
        symbols = db.get_all_symbols_except_CQ(only_companies=True)
    except Exception as e:
        st.error(f"Không thể lấy danh sách mã: {e}")
        symbols = []

    if symbols:
        symbol = st.selectbox("Chọn mã chứng khoán", symbols)

        # Chọn khoảng thời gian
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Từ ngày", value=datetime.now() - timedelta(days=360))
        with col2:
            end_date = st.date_input("Đến ngày", value=datetime.now())

        # Loại biểu đồ
        chart_type = st.radio("Loại biểu đồ", ["Đường (Close)", "Nến (Candlestick)"], horizontal=True)
        # Tùy chọn thang đo
        log_scale = st.checkbox("Thang đo Logarit (trục Y)")

        # Nút tải
        if st.button("📈 Tải biểu đồ"):
            with st.spinner("Đang tải dữ liệu..."):
                try:
                    # Truy vấn dữ liệu từ bảng daily_stock_prices
                    query = text("""
                        SELECT trading_date, open_price, highest_price, lowest_price, close_price, total_match_vol
                        FROM daily_stock_prices
                        WHERE symbol = :symbol
                          AND trading_date BETWEEN :start AND :end
                        ORDER BY trading_date
                    """)
                    with db.engine.connect() as conn:
                        df = pd.read_sql(query, conn, params={
                            "symbol": symbol,
                            "start": start_date,
                            "end": end_date
                        })

                    if df.empty:
                        st.warning("Không có dữ liệu cho khoảng thời gian đã chọn.")
                    else:
                        # Tạo key duy nhất cho biểu đồ dựa trên các tham số
                        chart_key = f"{symbol}_{start_date}_{end_date}_{chart_type}_{log_scale}"
                        # Vẽ biểu đồ
                        if chart_type == "Đường (Close)":
                            fig = go.Figure()
                            fig.add_trace(go.Scatter(
                                x=df['trading_date'],
                                y=df['close_price'],
                                mode='lines',
                                name='Giá đóng cửa',
                                line=dict(color='blue')
                            ))
                            fig.update_layout(
                                title=f"Biểu đồ giá đóng cửa {symbol}",
                                xaxis_title="Ngày",
                                yaxis_title="Giá (VND)",
                                hovermode='x',
                                height=600,
                                margin=dict(l=50, r=50, t=80, b=50)
                            )
                            if log_scale:
                                fig.update_yaxes(type="log")
                            st.plotly_chart(fig, width='stretch', key=chart_key)

                        else:  # Candlestick
                            # Tạo subplot: 2 dòng, chung trục x
                            fig = make_subplots(
                                rows=2, cols=1,
                                shared_xaxes=True,
                                vertical_spacing=0.03,
                                row_heights=[0.7, 0.3],
                                subplot_titles=(f"{symbol} - Biểu đồ nến","")
                            )

                            # Biểu đồ nến
                            fig.add_trace(go.Candlestick(
                                x=df['trading_date'],
                                open=df['open_price'],
                                high=df['highest_price'],
                                low=df['lowest_price'],
                                close=df['close_price'],
                                name="Nến"
                            ), row=1, col=1)

                            # Biểu đồ cột volume
                            colors = ['red' if row['open_price'] > row['close_price'] else 'green'
                                      for _, row in df.iterrows()]
                            fig.add_trace(go.Bar(
                                x=df['trading_date'],
                                y=df['total_match_vol'],
                                name="Volume",
                                marker_color=colors,
                                opacity=0.6
                            ), row=2, col=1)

                            fig.update_layout(
                                title=f"{symbol} - Giá và khối lượng",
                                xaxis_title="Ngày",
                                hovermode='x',
                                height=700,  # Cao hơn để nhìn rõ volume
                                margin=dict(l=50, r=50, t=80, b=50),
                                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                            )
                            if log_scale:
                                fig.update_yaxes(type="log", row=1, col=1)

                            # Ẩn range slider mặc định (có thể bật lại nếu muốn)
                            fig.update_xaxes(rangeslider_visible=False)

                            st.plotly_chart(fig, width='stretch', key=chart_key)

                except Exception as e:
                    st.error(f"Lỗi khi tải dữ liệu: {e}")
                    logging.exception(e)
    else:
        st.info("Chưa có dữ liệu securities. Hãy đồng bộ danh mục trước ở Tab 1.")

# --------------------- TAB 3: BIỂU ĐỒ ---------------------
with tab3:
    st.header("Biểu đồ Lightweight Chart")

    try:
        symbols = db.get_all_symbols_except_CQ(only_companies=True)
    except Exception as e:
        st.error(f"Không thể lấy danh sách mã: {e}")
        symbols = []

    if symbols:
        symbol = st.selectbox("Chọn mã chứng khoán", symbols, key="lwc_symbol")

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Từ ngày", value=date(2021, 1, 1), key="lwc_start")
        with col2:
            end_date = st.date_input("Đến ngày", value=datetime.now(), key="lwc_end")

        chart_type = st.radio("Loại biểu đồ", ["Đường (Close)", "Nến (Candlestick)"], horizontal=True, key="lwc_type")

        if st.button("📈 Tải biểu đồ", key="lwc_btn"):
            with st.spinner("Đang tải dữ liệu..."):
                try:
                    query = text("""
                        SELECT trading_date, open_price, highest_price, lowest_price, close_price, total_match_vol
                        FROM daily_stock_prices
                        WHERE symbol = :symbol
                          AND trading_date BETWEEN :start AND :end
                        ORDER BY trading_date
                    """)
                    with db.engine.connect() as conn:
                        df = pd.read_sql(query, conn, params={
                            "symbol": symbol,
                            "start": start_date,
                            "end": end_date
                        })

                    if df.empty:
                        st.warning("Không có dữ liệu cho khoảng thời gian đã chọn.")
                    else:
                        # Chuẩn hóa cột ngày → string "YYYY-MM-DD"
                        df['time'] = pd.to_datetime(df['trading_date']).dt.strftime('%Y-%m-%d')

                        if chart_type == "Đường (Close)":
                            line_data = [
                                {"time": row["time"], "value": float(row["close_price"])}
                                for _, row in df.iterrows()
                            ]

                            charts = [
                                {
                                    "chart": {
                                        "height": 500,
                                        "layout": {
                                            "background": {"type": "solid", "color": "#ffffff"},
                                            "textColor": "#333333"
                                        },
                                        "grid": {
                                            "vertLines": {"color": "#e0e0e0"},
                                            "horzLines": {"color": "#e0e0e0"}
                                        },
                                        "crosshair": {"mode": 1},
                                        "timeScale": {"borderColor": "#cccccc"}
                                    },
                                    "series": [
                                        {
                                            "type": "Line",
                                            "data": line_data,
                                            "options": {
                                                "color": "#2962ff",
                                                "lineWidth": 2,
                                                "priceFormat": {"type": "price", "precision": 0, "minMove": 1}
                                            }
                                        }
                                    ]
                                }
                            ]

                            st.subheader(f"Biểu đồ giá đóng cửa — {symbol}")
                            renderLightweightCharts(charts, key=f"line_{symbol}_{start_date}_{end_date}")

                        else:  # Candlestick + Volume
                            candle_data = [
                                {
                                    "time": row["time"],
                                    "open":  float(row["open_price"]),
                                    "high":  float(row["highest_price"]),
                                    "low":   float(row["lowest_price"]),
                                    "close": float(row["close_price"])
                                }
                                for _, row in df.iterrows()
                            ]

                            volume_data = [
                                {
                                    "time":  row["time"],
                                    "value": float(row["total_match_vol"]),
                                    "color": "rgba(255,82,82,0.6)"
                                              if row["open_price"] > row["close_price"]
                                              else "rgba(0,150,136,0.6)"
                                }
                                for _, row in df.iterrows()
                            ]

                            charts = [
                                # --- Panel nến ---
                                {
                                    "chart": {
                                        "height": 420,
                                        "layout": {
                                            "background": {"type": "solid", "color": "#ffffff"},
                                            "textColor": "#333333"
                                        },
                                        "grid": {
                                            "vertLines": {"color": "#e0e0e0"},
                                            "horzLines": {"color": "#e0e0e0"}
                                        },
                                        "crosshair": {"mode": 1},
                                        "timeScale": {
                                            "borderColor": "#cccccc",
                                            "visible": True        # ẩn ở panel nến, hiện ở volume
                                        }
                                    },
                                    "series": [
                                        {
                                            "type": "Candlestick",
                                            "data": candle_data,
                                            "options": {
                                                "upColor":   "#26a69a",
                                                "downColor": "#ef5350",
                                                "borderUpColor":   "#26a69a",
                                                "borderDownColor": "#ef5350",
                                                "wickUpColor":   "#26a69a",
                                                "wickDownColor": "#ef5350",
                                                "priceFormat": {"type": "price", "precision": 0, "minMove": 1}
                                            }
                                        }
                                    ]
                                },
                                # --- Panel volume ---
                                {
                                    "chart": {
                                        "height": 160,
                                        "layout": {
                                            "background": {"type": "solid", "color": "#ffffff"},
                                            "textColor": "#333333"
                                        },
                                        "grid": {
                                            "vertLines": {"color": "#e0e0e0"},
                                            "horzLines": {"color": "#e0e0e0"}
                                        },
                                        "timeScale": {"borderColor": "#cccccc"}
                                    },
                                    "series": [
                                        {
                                            "type": "Histogram",
                                            "data": volume_data,
                                            "options": {
                                                "priceFormat": {"type": "volume"},
                                                "priceScaleId": ""    # overlay scale riêng
                                            }
                                        }
                                    ]
                                }
                            ]

                            st.subheader(f"Biểu đồ nến — {symbol}")
                            renderLightweightCharts(charts, key=f"candle_{symbol}_{start_date}_{end_date}")

                except Exception as e:
                    st.error(f"Lỗi khi tải dữ liệu: {e}")
                    logging.exception(e)
    else:
        st.info("Chưa có dữ liệu securities. Hãy đồng bộ danh mục trước ở Tab 1.")
# ===================== NOTE =====================
# Để chạy: streamlit run app.py