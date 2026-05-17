# src/interfaces/page1.py
# ─────────────────────────────────────────────────────────────────────────────
# Tab 1 — Đồng bộ dữ liệu · Indicators · Signals
# ─────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import streamlit as st


def render(db, sync_service, gap_service, indicator_svc, signal_svc) -> None:
    st.info(
        f"🖥️ **Database:** `{db.engine.url.host}` | "
        f"**Schema:** `{db.engine.url.database}`"
    )

    # ── PHẦN 1: ĐỒNG BỘ DỮ LIỆU THÔ ────────────────────────────────────────
    st.subheader("1. Đồng bộ dữ liệu")

    col1, col2 = st.columns([2, 1])
    with col1:
        func = st.selectbox(
            "Tác vụ đồng bộ",
            [
                "Đồng bộ danh mục securities (tất cả sàn)",
                "Đồng bộ 1 mã OHLC",
                "Đồng bộ tất cả mã OHLC",
                "Đồng bộ 1 mã giá chi tiết",
                "Đồng bộ tất cả mã giá chi tiết",
                "Bảo trì (cập nhật thiếu)",
                "Vá lỗ hổng dữ liệu",
            ],
            key="sys_sync_func",
        )
    with col2:
        t1_market = st.selectbox("Sàn", ["HOSE", "HNX", "UPCOM"], key="sys_market")

    # Dynamic inputs
    t1_from = datetime(2021, 1, 1).date()
    t1_to   = (datetime.now() - timedelta(days=1)).date()
    t1_symbol = "SSI"
    t1_mode   = "ohlc"

    c_a, c_b, c_c = st.columns(3)
    if "1 mã" in func:
        with c_a:
            t1_symbol = st.text_input("Mã chứng khoán", value="SSI", key="sys_sym")

    if "OHLC" in func or "giá chi tiết" in func:
        with c_b:
            t1_from = st.date_input("Từ ngày", value=datetime(2021, 1, 1), key="sys_from")
        with c_c:
            t1_to = st.date_input(
                "Đến ngày",
                value=datetime.now() - timedelta(days=1),
                key="sys_to",
            )

    if func == "Bảo trì (cập nhật thiếu)":
        with c_b:
            t1_mode = st.selectbox(
                "Loại dữ liệu", ["ohlc", "price"], key="sys_maint_mode"
            )

    if st.button("▶️ Chạy Đồng bộ", type="primary", use_container_width=True):
        st.session_state.log_messages = []
        with st.status(f"Đang thực hiện: {func}...", expanded=True) as status:
            try:
                f = t1_from.strftime("%d/%m/%Y")
                t = t1_to.strftime("%d/%m/%Y")
                s = t1_symbol.strip().upper()

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

    # ── PHẦN 2: INDICATORS & SIGNALS ─────────────────────────────────────────
    st.subheader("2. Xử lý Indicators & Signals")

    col_ind, col_sig = st.columns(2)

    with col_ind:
        st.markdown("#### 🧮 Tính Indicators")
        i_mode = st.radio(
            "Chế độ Indicators",
            ["Bảo trì (thiếu)", "1 mã", "Toàn sàn"],
            horizontal=True,
            key="i_m",
        )
        i_mkt  = st.selectbox("Sàn", ["HOSE", "HNX", "UPCOM"], key="i_mkt")
        i_sym  = st.text_input("Mã", value="SSI", key="i_s") if i_mode == "1 mã" else ""
        i_date = (
            st.date_input("Từ ngày (trống=all)", value=None, key="i_d")
            if i_mode != "Bảo trì (thiếu)"
            else None
        )

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
        s_mode = st.radio(
            "Chế độ Signals",
            ["Bảo trì (thiếu)", "1 mã", "Toàn sàn"],
            horizontal=True,
            key="s_m",
        )
        s_mkt  = st.selectbox("Sàn", ["HOSE", "HNX", "UPCOM"], key="s_mkt")
        s_sym  = st.text_input("Mã", value="SSI", key="s_s") if s_mode == "1 mã" else ""
        s_date = (
            st.date_input("Từ ngày (trống=all)", value=None, key="s_d")
            if s_mode != "Bảo trì (thiếu)"
            else None
        )

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

    st.subheader("3. Index List Management")
    col1, col2 = st.columns(2)
    with col1:
        selected_market = st.selectbox("Select Market",['HOSE', 'HNX', 'UPCOM'],key='index_market')
    with col2:
        if st.button(
                "🔄 Update Index List",
                use_container_width=True,
                key='btn_update_index'
        ):
            with st.spinner(
                    f"Đang cập nhật index list {selected_market}..."
            ):
                success = sync_service.fetch_index_list(
                        selected_market
                )
                if success:
                    st.success(
                        f"✅ Đã cập nhật index list {selected_market}")
                else:
                    st.error(f"❌ Lỗi cập nhật index list {selected_market}")
        if st.button(
                "🚀 Sync All Markets",
                use_container_width=True,
                key='btn_sync_all_index'
        ):
            with st.spinner("Đang sync toàn bộ index list..."):
                success = sync_service.sync_index_lists()
                if success:
                    st.success("✅ Sync toàn bộ index list thành công")
                else:
                    st.warning("⚠️ Một số market sync thất bại")

    with st.expander("Logs hệ thống"):
        st.code(
            "\n".join(st.session_state.get("log_messages", [])) or "Chưa có log."
        )
