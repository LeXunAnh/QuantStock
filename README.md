# 📈 QuantStock — Hệ thống phân tích & tín hiệu giao dịch chứng khoán Việt Nam

QuantStock là nền tảng thu thập, lưu trữ, tính toán chỉ báo kỹ thuật và phát hiện tín hiệu giao dịch từ dữ liệu thị trường chứng khoán Việt Nam (HOSE, HNX, UPCOM).  
Dự án sử dụng **SSI Market Data API** để đồng bộ dữ liệu giá, sau đó tính toán bộ chỉ báo phong phú và sinh tín hiệu BUY/SELL dựa trên các chiến lược định lượng.  
Giao diện **Streamlit** giúp trực quan hóa biểu đồ kỹ thuật, lọc tín hiệu và quản lý toàn bộ pipeline dữ liệu.

---

## ✨ Tính năng nổi bật

- **Đồng bộ dữ liệu tự động**  
  - Danh mục chứng khoán (3 sàn HOSE, HNX, UPCOM)  
  - Dữ liệu OHLC hàng ngày  
  - Dữ liệu giá chi tiết (giá, khối lượng, nước ngoài, ...)  
  - Phát hiện và vá lỗ hổng dữ liệu (gap repair)

- **Chỉ báo kỹ thuật** (Technical Indicators)  
  - Trend: MA5/10/20/50/200, EMA9/12/26  
  - Momentum: RSI14, MACD, Stochastic %K/%D  
  - Volatility: Bollinger Bands (20,2) + Width, ATR14  
  - Volume: Volume MA20, Volume Ratio, OBV  
  - Dòng tiền khối ngoại: Net Foreign Vol/Val 5D/10D

- **Tín hiệu giao dịch** (Signals)  
  - Golden / Death Cross (MA)  
  - RSI Oversold / Overbought  
  - MACD Bullish / Bearish Cross  
  - Bollinger Squeeze Breakout  
  - Đột biến khối lượng (Volume Spike)  
  - Tích luỹ / Phân phối của khối ngoại  
  - Mỗi tín hiệu có **Strength** (độ mạnh) để lọc dễ dàng

- **Giao diện người dùng** (Streamlit)  
  - Biểu đồ nến / đường với MA overlay, đánh dấu tín hiệu  
  - Screener tín hiệu theo sàn, loại, strength, ngày  
  - Trang quản lý chạy đồng bộ, tính indicators, phát hiện signals

---
