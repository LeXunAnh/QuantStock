from api_client import SSIAPIClient
from transformer import DataTransformer
from DatabaseHandler import DatabaseHandler
import time
from tqdm import tqdm
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class SyncService:
    """Điều phối quá trình đồng bộ dữ liệu"""
    def __init__(self, api_client: SSIAPIClient, db_handler: DatabaseHandler,
                 transformer: DataTransformer = None):
        self.api = api_client
        self.db = db_handler
        self.transformer = transformer or DataTransformer()

    def sync_securities(self, market: str = 'HOSE') -> bool:
        """Đồng bộ bảng securities"""
        try:
            res = self.api.get_securities(market, 1, 1000)
            status = res.get('status')
            if status == 'Success':
                data = res.get('data', [])
                if not data:
                    logger.warning(f"Không có data securities cho {market}")
                    return False
                df = self.transformer.securities_to_df(data)
                self.db.save_data(df, 'securities', ['symbol'])
                logger.info(f"Đã đồng bộ securities cho {market}")
                return True
            elif status in (401, 429) or res.get('statusCode') in (401, 429):
                logger.error("Lỗi xác thực hoặc rate limit khi lấy securities")
                return False
            else:
                logger.error(f"Lỗi lấy securities {market}: {res}")
                return False
        except Exception as e:
            logger.exception(f"Lỗi trong sync_securities: {e}")
            return False

    def sync_all_markets(self):
        """Đồng bộ tất cả sàn"""
        for market in ['HOSE', 'HNX', 'UPCOM']:
            logger.info(f"🔄 Đang đồng bộ danh mục sàn: {market}")
            self.sync_securities(market)

    def fetch_daily_ohlc(self, symbol: str, from_date: str, to_date: str,max_retries: int = 3) -> bool:
        """Lấy OHLC cho một symbol và lưu vào DB"""
        for attempt in range(max_retries):
            try:
                res = self.api.get_daily_ohlc(symbol, from_date, to_date)
                status = res.get('status')
                if status == 'Success':
                    data = res.get('data', [])
                    df = self.transformer.daily_ohlc_to_df(symbol, data)
                    if not df.empty:
                        self.db.save_data(df, 'daily_ohlc', ['symbol', 'trading_date'])
                    return True
                elif status == 401 or res.get('statusCode') == 401:
                    logger.error("Token hết hạn, đang refresh...")
                    self.api.get_access_token()
                elif status == 429 or res.get('statusCode') == 429:
                    wait = (attempt + 1) * 2
                    logger.warning(f"Rate limit OHLC cho {symbol}, đợi {wait}s")
                    time.sleep(wait)
                else:
                    logger.error(f"Lỗi OHLC cho {symbol}: {res}")
                    return False
            except Exception as e:
                logger.exception(f"Exception trong fetch_daily_ohlc: {e}")
                time.sleep(1)
        return False

    def sync_all_ohlc(self, market: str = 'HOSE',
                      from_date: str = '01/01/2015',
                      to_date: str = '13/02/2026'):
        """Đồng bộ OHLC cho toàn bộ sàn"""
        symbols = self.db.get_all_symbols(market=market)
        pbar = tqdm(symbols, desc=f"🚀 Syncing {market}", unit="symbol")
        for symbol in pbar:
            try:
                pbar.set_postfix({"Current": symbol})
                self.fetch_daily_ohlc(symbol, from_date, to_date)
            except Exception as e:
                logger.error(f"Lỗi tại mã {symbol}: {e}")
            finally:
                time.sleep(1.2)

    def fetch_daily_stock_prices(self, symbol: str, from_date: str, to_date: str,
                                 chunk_days: int = 30, max_retries: int = 3) -> bool:
        """Lấy dữ liệu giá chi tiết theo từng chunk để tránh rate limit"""
        start_dt = datetime.strptime(from_date, '%d/%m/%Y')
        end_dt = datetime.strptime(to_date, '%d/%m/%Y')
        current_start = start_dt
        delta = timedelta(days=chunk_days - 1)  # để inclusive

        with tqdm(desc=f"  ↳ {symbol}", unit="chunk", leave=False) as pbar_chunks:
            while current_start <= end_dt:
                current_end = min(current_start + delta, end_dt)
                str_start = current_start.strftime('%d/%m/%Y')
                str_end = current_end.strftime('%d/%m/%Y')
                pbar_chunks.set_postfix({"range": f"{str_start}-{str_end}"})

                success = False
                for attempt in range(max_retries):
                    if self._execute_fetch_stock_prices(symbol, str_start, str_end):
                        success = True
                        break
                    else:
                        logger.warning(f"⚠️ Thử lại lần {attempt+1} cho {symbol} [{str_start}]")
                        time.sleep(1)
                pbar_chunks.update(1)
                time.sleep(1)

                if success:
                    current_start = current_end + timedelta(days=1)
                else:
                    logger.error(f"Fail at {symbol} {str_start}")
                    current_start = current_end + timedelta(days=1)
        return True

    def _execute_fetch_stock_prices(self, symbol: str, from_date: str, to_date: str) -> bool:
        """Thực hiện một request lấy dữ liệu giá và lưu vào DB"""
        try:
            res = self.api.get_daily_stock_price(symbol, from_date, to_date)
            status = res.get('status')
            if status == 'Success':
                data = res.get('data', [])
                if not data:
                    # Không có dữ liệu (ngày nghỉ) vẫn coi là thành công
                    return True
                df = self.transformer.daily_stock_price_to_df(symbol, data)
                self.db.save_data(df, 'daily_stock_prices', ['symbol', 'trading_date'])
                return True
            elif status in (401, 429) or res.get('statusCode') in (401, 429):
                logger.warning(f"Rate limit hoặc auth lỗi: {status}")
                return False
            else:
                logger.error(f"API Error: {res.get('message')}")
                return False
        except Exception as e:
            logger.error(f"Exception: {e}")
            return False

    def sync_all_stock_prices(self, market: str = 'HOSE', from_date: str = '01/01/2021'):
        """Đồng bộ dữ liệu giá chi tiết cho tất cả mã trên sàn"""
        symbols = self.db.get_all_symbols_except_CQ(market=market, only_companies=True)
        now = datetime.now()
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        to_date = now.date() if now >= market_close else now.date() - timedelta(days=1)
        to_date_str = to_date.strftime('%d/%m/%Y')
        logger.info(f"🚀 Sync toàn bộ {market}: {len(symbols)} mã | {from_date} → {to_date_str}")

        pbar = tqdm(symbols, desc=f"Overall Progress {market}", unit="symbol")
        for symbol in pbar:
            try:
                pbar.set_postfix({"current": symbol})
                self.fetch_daily_stock_prices(symbol, from_date, to_date_str)
            except Exception as e:
                logger.error(f"Lỗi tại mã {symbol}: {e}")
            finally:
                time.sleep(0.3)

    def maintenance_sync(self, market: str = 'HOSE', mode: str = 'ohlc'):
        """Chạy đồng bộ bảo trì (chỉ các ngày thiếu)"""
        symbols = self.db.get_all_symbols_except_CQ(market=market, only_companies=True)
        now = datetime.now()
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        to_date = now.date() if now >= market_close else now.date() - timedelta(days=1)
        to_date_str = to_date.strftime('%d/%m/%Y')

        logger.info(f"🔄 Bắt đầu bảo trì dữ liệu sàn {market} cho {len(symbols)} mã")
        pbar = tqdm(symbols, desc=f"Maintenance {market}")

        for symbol in pbar:
            try:
                pbar.set_postfix({"current": symbol})
                if mode == 'ohlc':
                    last_date = self.db.get_latest_trading_date('daily_ohlc', symbol)
                    start_date = (last_date + timedelta(days=1)) if last_date \
                        else datetime.strptime("01/01/2015", '%d/%m/%Y').date()
                    if start_date <= to_date:
                        self.fetch_daily_ohlc(symbol, start_date.strftime('%d/%m/%Y'), to_date_str)
                elif mode == 'price':
                    last_date = self.db.get_latest_trading_date('daily_stock_prices', symbol)
                    start_date = (last_date + timedelta(days=1)) if last_date \
                        else datetime.strptime("01/01/2021", '%d/%m/%Y').date()
                    if start_date <= to_date:
                        self.fetch_daily_stock_prices(symbol, start_date.strftime('%d/%m/%Y'), to_date_str)
            except Exception as e:
                logger.error(f"Lỗi bảo trì tại mã {symbol}: {e}")
            finally:
                time.sleep(0.3)

    def sync_one_ohlc(self, symbol: str, from_date: str, to_date: str):
        """Đồng bộ OHLC cho một mã cụ thể"""
        logger.info(f"Bắt đầu đồng bộ OHLC cho {symbol} từ {from_date} đến {to_date}")
        return self.fetch_daily_ohlc(symbol, from_date, to_date)

    def sync_one_stock_price(self, symbol: str, from_date: str, to_date: str):
        """Đồng bộ dữ liệu giá chi tiết cho một mã cụ thể"""
        logger.info(f"Bắt đầu đồng bộ giá chi tiết cho {symbol} từ {from_date} đến {to_date}")
        return self.fetch_daily_stock_prices(symbol, from_date, to_date)