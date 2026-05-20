from sqlalchemy import create_engine,text
from sqlalchemy.pool import QueuePool
from datetime import date as date_type, timedelta
import pandas as pd
import config
import logging

logger = logging.getLogger(__name__)

class DatabaseHandler:
    def __init__(self):
        self.db_uri = config.DB_URI
        if not self.db_uri:
            raise ValueError("Not found DB_URI trong file config")
        self.engine = create_engine(
            self.db_uri,
            poolclass=QueuePool,
            pool_size=5,  # Số connection thường trực
            max_overflow=10,  # Cho phép mở thêm khi burst
            pool_timeout=30,  # Timeout chờ connection (giây)
            pool_recycle=1800,  # Recycle connection sau 30 phút (tránh stale)
            pool_pre_ping=True  # Ping trước khi dùng, tránh "connection closed" error
        )

    def save_data(self, df: pd.DataFrame, table_name: str, conflict_columns: list):
        if df.empty:
            return

        cols = df.columns.tolist()
        col_names = ", ".join(cols)
        placeholders = ", ".join([f":{c}" for c in cols])

        update_cols = [c for c in cols if c not in conflict_columns]
        conflict_stmt = ", ".join(conflict_columns)

        if update_cols:
            update_stmt = ", ".join(
                [f"{c} = EXCLUDED.{c}" for c in update_cols]
            )
            conflict_sql = f"DO UPDATE SET {update_stmt}"
        else:
            conflict_sql = "DO NOTHING"

        query = text(f"""
            INSERT INTO {table_name} ({col_names})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_stmt})
            {conflict_sql};
        """)

        batch_size = 1000
        total_rows_affected = 0
        try:
            with self.engine.begin() as conn:
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i + batch_size]
                    result= conn.execute(query,batch.to_dict(orient="records"))
                    total_rows_affected += result.rowcount
            logger.info(f"✅ Finished {table_name}: Processed {len(df)} rows. Affected (Upserted): {total_rows_affected} rows.")
        except Exception as e:
            logger.exception(f"❌ SQL error while saving to {table_name}: {e}")
            raise

    def get_all_symbols(self, market=None):
        """Lấy danh sách symbol từ bảng securities"""
        query = "SELECT symbol FROM securities"
        params = {}

        if market:
            query += " WHERE market = :market"
            params['market'] = market

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params)
                # Trả về một list đơn giản: ['SSI', 'FPT', 'VNM', ...]
                return [row[0] for row in result]
        except Exception as e:
            logger.error(f"❌ Lỗi khi lấy danh sách symbol: {e}")
            return []

    def get_all_symbols_except_CQ(self, market=None, only_companies=True):
        """
        Lấy danh sách mã chứng khoán.
        :param only_companies: Nếu True, chỉ lấy mã 3 ký tự (Cổ phiếu/ETF).
                               Nếu False, lấy tất cả (bao gồm Chứng quyền ~6-8 ký tự).
        """
        query = "SELECT symbol FROM securities WHERE 1=1"
        params = {}
        if only_companies:
            query += " AND symbol ~ '^[A-Z0-9]{3}$'"  # Lấy 3 ký tự (chữ hoặc số như ETF)
        if market:
            query += " AND market = :market"
            params["market"] = market
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params)
                return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách mã công ty: {e}")
            raise

    def get_latest_trading_date(self, table_name, symbol):
        """Lấy ngày giao dịch gần nhất của 1 mã trong bảng được chỉ định"""
        query = text(f"SELECT MAX(trading_date) FROM {table_name} WHERE symbol = :symbol")
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"symbol": symbol}).scalar()
                return result  # Trả về đối tượng date hoặc None
        except Exception as e:
            logger.error(f"Lỗi khi lấy max date của {symbol}: {e}")
            return None

    def optimize_db(self):
        # Dùng AUTOCOMMIT để ANALYZE chạy ngoài transaction block
        with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text("ANALYZE daily_ohlc"))
            conn.execute(text("ANALYZE daily_stock_prices"))
        logger.info("🚀 DB Optimized: Statistics updated for query planner.")

    def get_data_gaps(self, symbol):
        query = text("""
            WITH date_series AS (
                SELECT trading_date,
                       LEAD(trading_date) OVER (ORDER BY trading_date) as next_date
                FROM daily_stock_prices
                WHERE symbol = :symbol
            ),
            gaps AS (
                SELECT trading_date, next_date
                FROM date_series
                WHERE next_date - trading_date > 1
                AND next_date IS NOT NULL
            )
            SELECT g.trading_date + INTERVAL '1 day' as gap_start,
                   g.next_date - INTERVAL '1 day'    as gap_end
            FROM gaps g
            WHERE EXISTS (
                SELECT 1 FROM trading_calendar tc
                WHERE tc.trading_date > g.trading_date
                  AND tc.trading_date < g.next_date
                  AND tc.is_trading_day = TRUE
            )
            ORDER BY gap_start;
        """)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"symbol": symbol})
                return [(row[0], row[1]) for row in result]
        except Exception as e:
            logger.error(f"❌ Lỗi khi tìm gap cho {symbol}: {e}")
            return []

    def get_all_indices(self, market: str) -> list:
        """
        Lấy danh sách index_code từ bảng index_list dựa trên sàn (exchange)
        :param market: Tên sàn cần lấy ('HOSE', 'HNX', 'UPCOM')
        :return: Danh sách các chuỗi index_code, ví dụ: ['VNINDEX', 'VN30']
        """
        excluded_codes = (
            'VNSMALLCAP', 'VNXALLSHARE', 'VN50 GROWTH', 'VNALLSHARE',
            'VNDIVIDEND', 'VNMIDCAP', 'VNMITECH', 'VNSHINE')

        query = """
            SELECT index_code 
            FROM index_list 
            WHERE exchange = :market 
              AND index_code NOT IN :excluded_codes
        """
        try:
            with self.engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(
                    text(query),
                    {
                        "market": market,
                        "excluded_codes": excluded_codes
                    }
                )
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách chỉ số cho sàn {market}: {e}")
            return []

    @staticmethod
    def fetch_price_with_warmup(db, symbol: str, start: date_type, end: date_type, ) -> pd.DataFrame:
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

    @staticmethod
    def fetch_signals_for_chart(db, symbol: str, start: date_type, end: date_type, ) -> pd.DataFrame:
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

    @staticmethod
    def fetch_indicator_data(db, symbol: str, start: date_type, end: date_type, ) -> pd.DataFrame:
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

if __name__ == "__main__":
    #test
    db_manager = DatabaseHandler()
    #print(db_manager.get_all_symbols())
    #print(db_manager.get_all_symbols_except_CQ())
    #db_manager.optimize_db()
    #db_manager.get_data_gaps()
