from sqlalchemy import create_engine,text
from datetime import datetime, timedelta
import pandas as pd
from src import config
import logging

logger = logging.getLogger(__name__)

class DatabaseHandler:
    def __init__(self):
        self.db_uri = config.DB_URI
        if not self.db_uri:
            raise ValueError("Not found DB_URI trong file config")
        self.engine = create_engine(self.db_uri)

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
        with self.engine.connect() as conn:
            conn.execute(text("COMMIT"))  # Cần thoát khỏi transaction
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


if __name__ == "__main__":
    db_manager = DatabaseHandler()
    #print(db_manager.get_all_symbols())
    #print(db_manager.get_all_symbols_except_CQ())
    #db_manager.optimize_db()
    #db_manager.get_data_gaps()
