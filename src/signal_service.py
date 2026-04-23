"""
signal_service.py
─────────────────
Đọc dữ liệu từ bảng technical_indicators, phát hiện các tín hiệu
giao dịch và ghi kết quả vào bảng trading_signals.

Chiến lược được implement:
  ┌─────────────────────────────┬───────────────────────────────────────┐
  │ Signal type                 │ Điều kiện                             │
  ├─────────────────────────────┼───────────────────────────────────────┤
  │ MA_GOLDEN_CROSS             │ MA_fast cắt lên MA_slow               │
  │ MA_DEATH_CROSS              │ MA_fast cắt xuống MA_slow             │
  │ RSI_OVERSOLD                │ RSI vừa vượt lên khỏi ngưỡng 30       │
  │ RSI_OVERBOUGHT              │ RSI vừa tụt xuống khỏi ngưỡng 70      │
  │ MACD_BULLISH_CROSS          │ MACD cắt lên Signal Line              │
  │ MACD_BEARISH_CROSS          │ MACD cắt xuống Signal Line            │
  │ BB_SQUEEZE_BREAKOUT_UP      │ Giá đóng cửa vượt BB Upper sau nén    │
  │ BB_SQUEEZE_BREAKOUT_DOWN    │ Giá đóng cửa thủng BB Lower sau nén   │
  │ VOLUME_SPIKE                │ Vol_Ratio >= ngưỡng (mặc định 2.5x)   │
  │ FOREIGN_ACCUMULATION        │ Net foreign vol 5d > ngưỡng dương     │
  │ FOREIGN_DISTRIBUTION        │ Net foreign vol 5d < ngưỡng âm        │
  └─────────────────────────────┴───────────────────────────────────────┘

Strength (0.0 → 1.0) được tính dựa trên độ lớn của tín hiệu,
không phải giá trị cố định — giúp dễ filter ở Execution Engine.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text
from tqdm import tqdm

from DatabaseHandler import DatabaseHandler

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# SIGNAL DETECTOR  (pure-pandas, không phụ thuộc DB)
# ═══════════════════════════════════════════════════════════════

class _SignalDetector:
    """
    Nhận DataFrame indicators của 1 mã (đã sort ASC theo trading_date)
    và trả về list[dict] — mỗi dict là 1 signal row sẵn sàng INSERT.
    """

    # ── Cấu hình ngưỡng mặc định ────────────────────────────────
    RSI_OVERSOLD      = 30.0
    RSI_OVERBOUGHT    = 70.0
    BB_SQUEEZE_THR    = 0.05   # bb_width < ngưỡng này = đang nén
    VOLUME_SPIKE_THR  = 2.5    # vol_ratio >= 2.5x vol_ma20
    FOREIGN_VOL_THR   = 500_000  # |net_foreign_vol_5d| > 500k cổ

    @classmethod
    def _strength_clip(cls, val: float) -> float:
        return float(np.clip(round(val, 4), 0.0, 1.0))

    # ── MA Crossover ────────────────────────────────────────────

    @classmethod
    def detect_ma_cross(
        cls,
        df: pd.DataFrame,
        symbol: str,
        fast_col: str = "ma5",
        slow_col: str = "ma20",
    ) -> list[dict]:
        """
        Golden Cross / Death Cross giữa 2 đường MA tuỳ chọn.
        Strength = khoảng cách tương đối giữa 2 đường tại điểm cắt,
                   chuẩn hoá về [0, 1] theo sigmoid nhẹ.
        """
        signals = []
        prev_fast = df[fast_col].shift(1)
        prev_slow = df[slow_col].shift(1)

        for i, row in df.iterrows():
            f, s = row[fast_col], row[slow_col]
            pf, ps = prev_fast.iloc[i] if i > 0 else np.nan, prev_slow.iloc[i] if i > 0 else np.nan

            if any(pd.isna([f, s, pf, ps])):
                continue

            gap_pct = abs(f - s) / max(abs(s), 1e-9)
            strength = cls._strength_clip(min(gap_pct * 20, 1.0))  # bão hoà ở 5% gap

            if pf <= ps and f > s:
                signals.append(cls._build(
                    symbol       = symbol,
                    date         = row["trading_date"],
                    signal_type  = "MA_GOLDEN_CROSS",
                    direction    = "BUY",
                    strength     = strength,
                    close_price  = row["close_price"],
                    params       = {
                        "fast_ma": fast_col.upper(),
                        "slow_ma": slow_col.upper(),
                        fast_col : round(float(f), 4),
                        slow_col : round(float(s), 4),
                        "gap_pct": round(gap_pct * 100, 3),
                    },
                ))
            elif pf >= ps and f < s:
                signals.append(cls._build(
                    symbol       = symbol,
                    date         = row["trading_date"],
                    signal_type  = "MA_DEATH_CROSS",
                    direction    = "SELL",
                    strength     = strength,
                    close_price  = row["close_price"],
                    params       = {
                        "fast_ma": fast_col.upper(),
                        "slow_ma": slow_col.upper(),
                        fast_col : round(float(f), 4),
                        slow_col : round(float(s), 4),
                        "gap_pct": round(gap_pct * 100, 3),
                    },
                ))
        return signals

    # ── RSI ─────────────────────────────────────────────────────

    @classmethod
    def detect_rsi(cls, df: pd.DataFrame, symbol: str) -> list[dict]:
        """
        RSI_OVERSOLD  : RSI[t-1] < 30 và RSI[t] >= 30 (vừa thoát đáy)
        RSI_OVERBOUGHT: RSI[t-1] > 70 và RSI[t] <= 70 (vừa thoát đỉnh)
        Strength tỉ lệ nghịch với khoảng cách tới ngưỡng trung tâm (50).
        """
        signals = []
        prev_rsi = df["rsi14"].shift(1)

        for i, row in df.iterrows():
            r, pr = row["rsi14"], prev_rsi.iloc[i] if i > 0 else np.nan
            if pd.isna(r) or pd.isna(pr):
                continue

            if pr < cls.RSI_OVERSOLD <= r:
                strength = cls._strength_clip((cls.RSI_OVERSOLD - min(pr, cls.RSI_OVERSOLD)) / cls.RSI_OVERSOLD)
                signals.append(cls._build(
                    symbol, row["trading_date"],
                    "RSI_OVERSOLD", "BUY", strength, row["close_price"],
                    {"rsi14": round(float(r), 4), "prev_rsi": round(float(pr), 4), "threshold": cls.RSI_OVERSOLD},
                ))

            elif pr > cls.RSI_OVERBOUGHT >= r:
                strength = cls._strength_clip((min(pr, 100) - cls.RSI_OVERBOUGHT) / (100 - cls.RSI_OVERBOUGHT))
                signals.append(cls._build(
                    symbol, row["trading_date"],
                    "RSI_OVERBOUGHT", "SELL", strength, row["close_price"],
                    {"rsi14": round(float(r), 4), "prev_rsi": round(float(pr), 4), "threshold": cls.RSI_OVERBOUGHT},
                ))
        return signals

    # ── MACD ────────────────────────────────────────────────────

    @classmethod
    def detect_macd(cls, df: pd.DataFrame, symbol: str) -> list[dict]:
        """
        MACD_BULLISH_CROSS : macd_hist[t-1] < 0 và macd_hist[t] >= 0
        MACD_BEARISH_CROSS : macd_hist[t-1] > 0 và macd_hist[t] <= 0
        Strength tỉ lệ với |macd_hist| tại thời điểm crossover,
        chuẩn hoá theo ATR của chính histogram.
        """
        signals = []
        hist = df["macd_hist"]
        prev_hist = hist.shift(1)
        # Dùng rolling std của hist để chuẩn hoá strength
        hist_std = hist.rolling(60, min_periods=10).std()

        for i, row in df.iterrows():
            h, ph = row["macd_hist"], prev_hist.iloc[i] if i > 0 else np.nan
            if pd.isna(h) or pd.isna(ph):
                continue

            std = hist_std.iloc[i] if not pd.isna(hist_std.iloc[i]) else 1.0
            strength = cls._strength_clip(abs(h) / max(std * 3, 1e-9))

            if ph < 0 <= h:
                signals.append(cls._build(
                    symbol, row["trading_date"],
                    "MACD_BULLISH_CROSS", "BUY", strength, row["close_price"],
                    {
                        "macd"       : round(float(row["macd"]), 6),
                        "macd_signal": round(float(row["macd_signal"]), 6),
                        "macd_hist"  : round(float(h), 6),
                    },
                ))
            elif ph > 0 >= h:
                signals.append(cls._build(
                    symbol, row["trading_date"],
                    "MACD_BEARISH_CROSS", "SELL", strength, row["close_price"],
                    {
                        "macd"       : round(float(row["macd"]), 6),
                        "macd_signal": round(float(row["macd_signal"]), 6),
                        "macd_hist"  : round(float(h), 6),
                    },
                ))
        return signals

    # ── Bollinger Squeeze Breakout ───────────────────────────────

    @classmethod
    def detect_bb_breakout(cls, df: pd.DataFrame, symbol: str) -> list[dict]:
        """
        Phát hiện breakout SAU KHI BB đang nén (squeeze):
          - Ít nhất 3 ngày trước bb_width < BB_SQUEEZE_THR
          - Ngày hiện tại: close > bb_upper  → BREAKOUT_UP
          - Ngày hiện tại: close < bb_lower  → BREAKOUT_DOWN
        Strength = (close - bb_upper) / atr14  — normalized.
        """
        signals = []
        squeeze_streak = 0

        for i, row in df.iterrows():
            bw = row.get("bb_width")
            if pd.isna(bw):
                continue

            if bw < cls.BB_SQUEEZE_THR:
                squeeze_streak += 1
                continue

            if squeeze_streak >= 3:
                close = row["close_price"]
                upper = row["bb_upper"]
                lower = row["bb_lower"]
                atr   = row.get("atr14") or 1.0

                if not pd.isna(upper) and close > upper:
                    strength = cls._strength_clip((close - upper) / max(atr, 1e-9) / 3)
                    signals.append(cls._build(
                        symbol, row["trading_date"],
                        "BB_SQUEEZE_BREAKOUT_UP", "BUY", strength, close,
                        {
                            "bb_upper"     : round(float(upper), 4),
                            "bb_lower"     : round(float(lower), 4),
                            "bb_width"     : round(float(bw), 6),
                            "squeeze_days" : squeeze_streak,
                            "atr14"        : round(float(atr), 4),
                        },
                    ))
                elif not pd.isna(lower) and close < lower:
                    strength = cls._strength_clip((lower - close) / max(atr, 1e-9) / 3)
                    signals.append(cls._build(
                        symbol, row["trading_date"],
                        "BB_SQUEEZE_BREAKOUT_DOWN", "SELL", strength, close,
                        {
                            "bb_upper"     : round(float(upper), 4),
                            "bb_lower"     : round(float(lower), 4),
                            "bb_width"     : round(float(bw), 6),
                            "squeeze_days" : squeeze_streak,
                            "atr14"        : round(float(atr), 4),
                        },
                    ))

            squeeze_streak = 0  # reset sau mỗi nến không nén
        return signals

    # ── Volume Spike ────────────────────────────────────────────

    @classmethod
    def detect_volume_spike(cls, df: pd.DataFrame, symbol: str) -> list[dict]:
        """
        Phát hiện đột biến khối lượng (vol_ratio >= ngưỡng).
        Direction = BUY nếu nến tăng, SELL nếu nến giảm.
        Strength = (vol_ratio - threshold) / threshold, clip [0,1].
        """
        signals = []
        for _, row in df.iterrows():
            vr = row.get("vol_ratio")
            if pd.isna(vr) or vr < cls.VOLUME_SPIKE_THR:
                continue

            direction = "BUY" if row["close_price"] >= row.get("open_price", row["close_price"]) else "SELL"
            strength  = cls._strength_clip((vr - cls.VOLUME_SPIKE_THR) / cls.VOLUME_SPIKE_THR)
            signals.append(cls._build(
                symbol, row["trading_date"],
                "VOLUME_SPIKE", direction, strength, row["close_price"],
                {
                    "vol_ratio"  : round(float(vr), 4),
                    "vol_ma20"   : round(float(row.get("vol_ma20") or 0), 0),
                    "threshold"  : cls.VOLUME_SPIKE_THR,
                },
            ))
        return signals

    # ── Foreign Flow ────────────────────────────────────────────

    @classmethod
    def detect_foreign_flow(cls, df: pd.DataFrame, symbol: str) -> list[dict]:
        """
        FOREIGN_ACCUMULATION : net_foreign_vol_5d > +threshold  (khối ngoại mua ròng)
        FOREIGN_DISTRIBUTION : net_foreign_vol_5d < -threshold  (khối ngoại bán ròng)
        Chỉ phát tín hiệu khi chiều đổi (tránh liên tục mỗi ngày).
        """
        signals = []
        prev_dir = 0  # 0=neutral, +1=acc, -1=dist

        for _, row in df.iterrows():
            nv = row.get("net_foreign_vol_5d")
            if pd.isna(nv):
                continue

            nv = float(nv)
            cur_dir = 0
            if nv > cls.FOREIGN_VOL_THR:
                cur_dir = 1
            elif nv < -cls.FOREIGN_VOL_THR:
                cur_dir = -1

            if cur_dir != 0 and cur_dir != prev_dir:
                strength = cls._strength_clip(abs(nv) / (cls.FOREIGN_VOL_THR * 10))
                signals.append(cls._build(
                    symbol, row["trading_date"],
                    "FOREIGN_ACCUMULATION" if cur_dir == 1 else "FOREIGN_DISTRIBUTION",
                    "BUY" if cur_dir == 1 else "SELL",
                    strength, row["close_price"],
                    {
                        "net_foreign_vol_5d" : int(nv),
                        "net_foreign_val_5d" : round(float(row.get("net_foreign_val_5d") or 0), 2),
                        "threshold"          : cls.FOREIGN_VOL_THR,
                    },
                ))
            prev_dir = cur_dir
        return signals

    # ── Builder ─────────────────────────────────────────────────

    @staticmethod
    def _build(
        symbol: str,
        date,
        signal_type: str,
        direction: str,
        strength: float,
        close_price: float,
        params: dict,
    ) -> dict:
        ts = datetime.combine(date, datetime.min.time()) if hasattr(date, "year") else datetime.fromisoformat(str(date))
        return {
            "symbol"          : symbol,
            "signal_date"     : date,
            "signal_time"     : ts,
            "signal_type"     : signal_type,
            "signal_direction": direction,
            "strength"        : float(round(strength, 4)),
            "source_type"     : "EOD",
            "close_price"     : float(round(close_price, 2)),
            "parameters"      : json.dumps(params, ensure_ascii=False),
            "is_active"       : True,
        }

    # ── Run all detectors ────────────────────────────────────────

    @classmethod
    def run(
        cls,
        df: pd.DataFrame,
        symbol: str,
        ma_pairs: list[tuple[str, str]] | None = None,
        enable: set[str] | None = None,
    ) -> list[dict]:
        """
        Chạy toàn bộ detectors và gộp kết quả.

        Args:
            df       : DataFrame indicators đã sort ASC
            symbol   : Mã chứng khoán
            ma_pairs : Danh sách cặp (fast_col, slow_col), mặc định [(ma5,ma20),(ma10,ma50)]
            enable   : Tập signal_type muốn bật — None = bật tất cả
        """
        if ma_pairs is None:
            ma_pairs = [("ma5", "ma20"), ("ma10", "ma50")]

        all_signals: list[dict] = []

        def _active(name: str) -> bool:
            return enable is None or name in enable

        for fast, slow in ma_pairs:
            cross_type = "MA_GOLDEN_CROSS"
            if _active(cross_type) or _active("MA_DEATH_CROSS"):
                all_signals.extend(cls.detect_ma_cross(df, symbol, fast, slow))

        if _active("RSI_OVERSOLD") or _active("RSI_OVERBOUGHT"):
            all_signals.extend(cls.detect_rsi(df, symbol))

        if _active("MACD_BULLISH_CROSS") or _active("MACD_BEARISH_CROSS"):
            all_signals.extend(cls.detect_macd(df, symbol))

        if _active("BB_SQUEEZE_BREAKOUT_UP") or _active("BB_SQUEEZE_BREAKOUT_DOWN"):
            all_signals.extend(cls.detect_bb_breakout(df, symbol))

        if _active("VOLUME_SPIKE"):
            all_signals.extend(cls.detect_volume_spike(df, symbol))

        if _active("FOREIGN_ACCUMULATION") or _active("FOREIGN_DISTRIBUTION"):
            all_signals.extend(cls.detect_foreign_flow(df, symbol))

        # Sort theo thời gian
        all_signals.sort(key=lambda s: str(s["signal_date"]))
        return all_signals


# ═══════════════════════════════════════════════════════════════
# SIGNAL SERVICE  (tích hợp DB)
# ═══════════════════════════════════════════════════════════════

class SignalService:
    """
    Điều phối phát hiện & lưu trữ tín hiệu giao dịch.

    Cách dùng:
        svc = SignalService(db)
        svc.run_one("SSI")
        svc.run_all("HOSE")
        svc.run_maintenance("HOSE")   # chỉ tính ngày chưa có signal
    """

    def __init__(
        self,
        db_handler: DatabaseHandler,
        ma_pairs: list[tuple[str, str]] | None = None,
        enable: set[str] | None = None,
    ):
        self.db       = db_handler
        self.ma_pairs = ma_pairs  # None → dùng default của _SignalDetector
        self.enable   = enable    # None → bật tất cả

    # ─── DB helpers ─────────────────────────────────────────────

    def _fetch_indicators(self, symbol: str, from_date: Optional[str] = None) -> pd.DataFrame:
        """
        JOIN technical_indicators với daily_stock_prices để lấy giá điều chỉnh
        (close_price_adjusted, open_price * adj_factor) — nhất quán với indicator_service.
        close_price  = close_price_adjusted
        open_price   = open_price_raw * (close_price_adjusted / close_price_raw)
        """
        params: dict = {"symbol": symbol}
        date_clause  = ""
        if from_date:
            date_clause = "AND ti.trading_date >= :from_date"
            params["from_date"] = from_date

        query = text(f"""
            SELECT
                ti.trading_date,
                -- Giá điều chỉnh — khớp với những gì indicator_service đã tính
                sp.close_price_adjusted                                          AS close_price,
                ROUND(sp.open_price *
                    (sp.close_price_adjusted / NULLIF(sp.close_price, 0)), 2)   AS open_price,
                -- Indicators
                ti.ma5, ti.ma10, ti.ma20, ti.ma50, ti.ma200,
                ti.rsi14,
                ti.macd, ti.macd_signal, ti.macd_hist,
                ti.bb_upper, ti.bb_middle, ti.bb_lower, ti.bb_width,
                ti.atr14,
                ti.vol_ma20, ti.vol_ratio,
                ti.net_foreign_vol_5d, ti.net_foreign_val_5d
            FROM technical_indicators ti
            JOIN daily_stock_prices sp
              ON sp.symbol = ti.symbol
             AND sp.trading_date = ti.trading_date
             AND sp.close_price > 0
             AND sp.close_price_adjusted IS NOT NULL
            WHERE ti.symbol = :symbol
              {date_clause}
            ORDER BY ti.trading_date ASC
        """)
        try:
            with self.db.engine.connect() as conn:
                return pd.read_sql(query, conn, params=params)
        except Exception as e:
            logger.error(f"❌ Lỗi fetch indicators {symbol}: {e}")
            return pd.DataFrame()

    def _get_latest_signal_date(self, symbol: str):
        query = text(
            "SELECT MAX(signal_date) FROM trading_signals WHERE symbol = :sym"
        )
        try:
            with self.db.engine.connect() as conn:
                return conn.execute(query, {"sym": symbol}).scalar()
        except Exception as e:
            logger.error(f"Lỗi lấy max signal date {symbol}: {e}")
            return None

    def _ensure_unique_constraint(self):
        """
        Tự động tạo UNIQUE constraint (symbol, signal_date, signal_type)
        nếu chưa tồn tại. Chạy idempotent — safe để gọi nhiều lần.
        """
        check_sql = text("""
            SELECT 1 FROM pg_constraint
            WHERE conname = 'uq_trading_signals_symbol_date_type'
        """)
        try:
            with self.db.engine.connect() as conn:
                exists = conn.execute(check_sql).scalar()

            if not exists:
                logger.info("⚙️  Thêm UNIQUE constraint vào trading_signals...")
                with self.db.engine.begin() as conn:
                    # Xoá duplicate trước (giữ id nhỏ nhất)
                    conn.execute(text("""
                        DELETE FROM trading_signals
                        WHERE id NOT IN (
                            SELECT MIN(id)
                            FROM trading_signals
                            GROUP BY symbol, signal_date, signal_type
                        )
                    """))
                    conn.execute(text("""
                        ALTER TABLE trading_signals
                            ADD CONSTRAINT uq_trading_signals_symbol_date_type
                            UNIQUE (symbol, signal_date, signal_type)
                    """))
                logger.info("✅ Đã thêm UNIQUE constraint thành công.")
        except Exception as e:
            logger.error(f"❌ Không thể tạo constraint: {e}")
            raise

    def _save(self, signals: list[dict]):
        if not signals:
            return
        #self._ensure_unique_constraint()
        df = pd.DataFrame(signals)
        self.db.save_data(
            df,
            "trading_signals",
            ["symbol", "signal_date", "signal_type"],
        )

    # ─── Public API ─────────────────────────────────────────────

    def run_one(self, symbol: str, from_date: Optional[str] = None) -> int:
        """
        Phát hiện và lưu tín hiệu cho MỘT mã.
        Trả về số tín hiệu được ghi.

        Args:
            symbol   : Mã chứng khoán, ví dụ 'SSI'
            from_date: 'YYYY-MM-DD' — chỉ tính từ ngày này. None = toàn bộ.
        """
        df = self._fetch_indicators(symbol, from_date)
        if df.empty:
            logger.warning(f"⚠️  {symbol}: Không có indicators")
            return 0

        signals = _SignalDetector.run(df, symbol, self.ma_pairs, self.enable)
        if not signals:
            logger.debug(f"  {symbol}: Không có tín hiệu mới")
            return 0

        self._save(signals)
        logger.info(f"✅ {symbol}: Ghi {len(signals)} tín hiệu")
        return len(signals)

    def run_all(self, market: str = "HOSE", from_date: Optional[str] = None) -> int:
        """Phát hiện tín hiệu cho TOÀN BỘ mã trên sàn."""
        symbols = self.db.get_all_symbols_except_CQ(market=market, only_companies=True)
        logger.info(f"🚀 Phát hiện tín hiệu: {len(symbols)} mã | sàn {market}")

        total = ok = fail = 0
        pbar = tqdm(symbols, desc=f"Signals {market}", unit="sym")
        for sym in pbar:
            pbar.set_postfix({"current": sym})
            try:
                n = self.run_one(sym, from_date)
                total += n
                ok += 1
            except Exception as e:
                logger.error(f"❌ {sym}: {e}")
                fail += 1

        logger.info(f"✅ Hoàn tất: {total} tín hiệu | {ok} mã OK / {fail} lỗi")
        return total

    def run_maintenance(self, market: str = "HOSE") -> int:
        """
        Chế độ BẢO TRÌ: chỉ tính các ngày chưa có tín hiệu.
        An toàn để chạy hàng ngày sau market close.
        """
        symbols = self.db.get_all_symbols_except_CQ(market=market, only_companies=True)
        logger.info(f"🔄 Bảo trì signals: {len(symbols)} mã | sàn {market}")

        today = datetime.now().date()
        total = ok = skip = fail = 0
        pbar = tqdm(symbols, desc=f"Maintenance signals {market}", unit="sym")

        for sym in pbar:
            pbar.set_postfix({"current": sym})
            try:
                last = self._get_latest_signal_date(sym)
                if last and last >= today:
                    skip += 1
                    continue

                from_date = (last + timedelta(days=1)).strftime("%Y-%m-%d") if last else None
                n = self.run_one(sym, from_date)
                total += n
                ok += 1
            except Exception as e:
                logger.error(f"❌ {sym}: {e}")
                fail += 1

        logger.info(f"✅ Bảo trì xong: {total} tín hiệu mới | {ok} cập nhật / {skip} bỏ qua / {fail} lỗi")
        return total

    def get_latest_signals(
        self,
        market: str = "HOSE",
        date: Optional[str] = None,
        direction: Optional[str] = None,
        min_strength: float = 0.0,
        signal_types: Optional[list[str]] = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Query tín hiệu mới nhất — dùng cho screener hoặc dashboard.
        Args:
            market      : Lọc theo sàn
            date        : 'YYYY-MM-DD' — mặc định là ngày gần nhất có tín hiệu
            direction   : 'BUY' | 'SELL' | None (tất cả)
            min_strength: Strength tối thiểu (0.0 → 1.0)
            signal_types: ['MA_GOLDEN_CROSS', ...] — None = tất cả
            limit       : Số kết quả tối đa
        """
        params: dict = {"min_strength": min_strength, "limit": limit}
        clauses = ["ts.strength >= :min_strength"]

        if date:
            clauses.append("ts.signal_date = :date")
            params["date"] = date
        else:
            # Tự lấy ngày gần nhất có signal
            clauses.append("""
                ts.signal_date = (
                    SELECT MAX(signal_date) FROM trading_signals
                )
            """)

        if direction:
            clauses.append("ts.signal_direction = :direction")
            params["direction"] = direction

        if signal_types:
            placeholders = ", ".join([f":st{i}" for i in range(len(signal_types))])
            for i, st in enumerate(signal_types):
                params[f"st{i}"] = st
            clauses.append(f"ts.signal_type IN ({placeholders})")

        if market:
            clauses.append("sec.market = :market")
            params["market"] = market

        where = " AND ".join(clauses)
        query = text(f"""
            SELECT
                ts.signal_date,
                ts.symbol,
                sec.stock_name,
                sec.market,
                ts.signal_type,
                ts.signal_direction,
                ts.strength,
                ts.close_price,
                ts.parameters
            FROM trading_signals ts
            JOIN securities sec ON sec.symbol = ts.symbol
            WHERE {where}
            ORDER BY ts.signal_date DESC, ts.strength DESC
            LIMIT :limit
        """)

        try:
            with self.db.engine.connect() as conn:
                return pd.read_sql(query, conn, params=params)
        except Exception as e:
            logger.error(f"❌ Lỗi query signals: {e}")
            return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    db  = DatabaseHandler()
    svc = SignalService(db)

    while True:
        print("\n" + "=" * 50)
        print("SIGNAL SERVICE")
        print("=" * 50)
        print("1. Phát hiện tín hiệu cho 1 mã")
        print("2. Phát hiện tín hiệu toàn sàn")
        print("3. Bảo trì (chỉ ngày còn thiếu)")
        print("4. Xem tín hiệu mới nhất (screener)")
        print("5. Thoát")

        choice = input("Lựa chọn (1-5): ").strip()

        if choice == "1":
            sym  = input("Mã (ví dụ SSI): ").strip().upper()
            date = input("Từ ngày YYYY-MM-DD (Enter = toàn bộ): ").strip() or None
            svc.run_one(sym, from_date=date)

        elif choice == "2":
            market = input("Sàn (HOSE/HNX/UPCOM, mặc định HOSE): ").strip() or "HOSE"
            date   = input("Từ ngày YYYY-MM-DD (Enter = toàn bộ): ").strip() or None
            svc.run_all(market, from_date=date)

        elif choice == "3":
            market = input("Sàn (mặc định HOSE): ").strip() or "HOSE"
            svc.run_maintenance(market)

        elif choice == "4":
            market    = input("Sàn (mặc định HOSE): ").strip() or "HOSE"
            direction = input("Chiều (BUY/SELL/Enter=tất cả): ").strip().upper() or None
            strength  = float(input("Strength tối thiểu (0.0-1.0, mặc định 0.3): ").strip() or "0.3")
            result    = svc.get_latest_signals(market=market, direction=direction, min_strength=strength)
            if result.empty:
                print("Không có tín hiệu.")
            else:
                print(result.to_string(index=False))

        elif choice == "5":
            print("Thoát.")
            break
        else:
            print("Vui lòng nhập 1-5.")
