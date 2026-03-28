from ssi_fc_data import fc_md_client, model
from src import config
from tqdm import tqdm
from datetime import datetime, timedelta
import pandas as pd
import logging,time,math
from DatabaseHandler import DatabaseHandler

logger = logging.getLogger(__name__)

class ssi_api:
    def __init__(self, config):
        self.config = config
        self.client = fc_md_client.MarketDataClient(self.config)
        self.db = DatabaseHandler()

    def get_access_token(self):
        return self.client.access_token()

    def fetch_and_sync_securities(self, market='HOSE'):
        """Lấy danh sách mã và đồng bộ vào bảng securities"""
        # Giả sử dùng hàm GetSecurities từ tài liệu (mục 4.2)
        req = model.securities(market,1,1000)
        res = self.client.securities(self.config,req)
        status = res.get('status')

        if status == 'Success':
            data = res.get('data', [])
            if not data:
                logger.warning(f"Không có data securities cho {market}")
                return
            df = pd.DataFrame(data)

            # Mapping cột API -> DB
            # Tài liệu SSI: Symbol, StockName, StockEnName, Market
            df_mapped = pd.DataFrame({
                'symbol': df['Symbol'],
                'market': df['Market'],
                'stock_name': df['StockName'],
                'stock_en_name': df['StockEnName']
            })

            self.db.save_data(df_mapped, 'securities', ['symbol'])

        elif status == 401 or res.get('statusCode') == 401:  # ✅ Giờ mới chạy được
            logger.error("Lỗi xác thực: Access Token hết hạn hoặc sai.")
            self.get_access_token()
        else:
            logger.error(f"Lỗi lấy danh sách securities {market}: status={status}, message={res.get('message')}")

    def sync_all_markets(self):
        """Hàm wrapper để quét qua tất cả các sàn quan trọng"""
        markets = ['HOSE', 'HNX', 'UPCOM']
        for mkt in markets:
            logger.info(f"🔄 Đang bắt đầu đồng bộ danh mục sàn: {mkt}")
            self.fetch_and_sync_securities(market=mkt)
        print("Đã insert xong !")

    def fetch_daily_ohlc(self, symbol, from_date, to_date):
        """Lấy dữ liệu OHLC từ năm 2015 (Mục 4.6 tài liệu)"""
        for attempt in range(3):
            req = model.daily_ohlc(symbol, from_date, to_date, 1, 9999,True)
            res = self.client.daily_ohlc(self.config,req)
            status = res.get('status')

            if status == 'Success':
                df = pd.DataFrame(res.get('data', []))
                if df.empty: return

                # Transform dữ liệu
                df_ohlc = pd.DataFrame({
                    'symbol': symbol,
                    'trading_date': pd.to_datetime(df['TradingDate'], dayfirst=True).dt.date,
                    'open_price': pd.to_numeric(df['Open']),
                    'highest_price': pd.to_numeric(df['High']),
                    'lowest_price': pd.to_numeric(df['Low']),
                    'close_price': pd.to_numeric(df['Close']),
                    'volume': pd.to_numeric(df['Volume']).astype(int),
                    'total_value': pd.to_numeric(df['Value'])
                })

                self.db.save_data(df_ohlc, 'daily_ohlc', ['symbol', 'trading_date'])

            elif status == 401 or res.get('statusCode') == 401:  # ✅
                logger.error("Lỗi xác thực: Access Token hết hạn hoặc sai.")
                self.get_access_token()
            elif status == 429 or res.get('statusCode') == 429:
                wait_time = (attempt + 1) * 2  # Đợi tăng dần: 2s, 4s, 6s
                logger.warning(f"⏳ Rate limit OHLC cho {symbol}, đợi {wait_time}s (Lần {attempt + 1})")
                time.sleep(wait_time)
            else:
                logger.error(f"Lỗi lấy OHLC cho {symbol}: status={status}, message={res.get('message')}")

    def sync_all_ohlc(self, market='HOSE', from_date='01/01/2015', to_date='13/02/2026'):
        # 1. Lấy danh sách mã từ DB
        symbols = self.db.get_all_symbols(market=market)
        # 2. Khởi tạo tqdm bao quanh danh sách vòng lặp
        # desc: Mô tả hiện trên thanh progress
        # unit: Đơn vị (ở đây là từng mã 'symbol')
        pbar = tqdm(symbols, desc=f"🚀 Syncing {market}", unit="symbol")

        for symbol in pbar:
            try:
                pbar.set_postfix({"Current": symbol})
                self.fetch_daily_ohlc(symbol, from_date, to_date)
            except Exception as e:
                logger.error(f"Lỗi tại mã {symbol}: {e}")
            finally:
                time.sleep(1.2)

    def fetch_daily_stock_prices(self, symbol, from_date, to_date):
        start_dt = datetime.strptime(from_date, '%d/%m/%Y')
        end_dt = datetime.strptime(to_date, '%d/%m/%Y')
        current_start = start_dt
        chunk_delta = timedelta(days=29)  # ✅ 29 offset = 30 ngày inclusive

        pbar_chunks = tqdm(desc=f"  ↳ {symbol}", unit="chunk", leave=False)

        while current_start <= end_dt:
            current_end = min(current_start + chunk_delta, end_dt)
            str_start = current_start.strftime('%d/%m/%Y')
            str_end = current_end.strftime('%d/%m/%Y')

            logger.info(f"🔍 Fetching {symbol} chi tiết: {str_start} -> {str_end}")
            pbar_chunks.set_postfix({"range": f"{str_start}-{str_end}"})

            success = False
            for attempt in range(3):
                if self._execute_fetch_stock_prices(symbol, str_start, str_end):
                    success = True
                    break
                else:
                    wait = 1
                    logger.warning(f"⚠️ Thử lại lần {attempt + 1} cho {symbol} [{str_start}], chờ {wait}s")
                    time.sleep(1)

            pbar_chunks.update(1)  # ✅ chỉ gọi 1 lần
            time.sleep(1)

            if success:
                current_start = current_end + timedelta(days=1)  # ✅ không overlap
            else:
                logger.error(f"Fail at {symbol} {str_start}")
                current_start = current_end + timedelta(days=1)  # nhảy tiếp

        pbar_chunks.close()

    def _execute_fetch_stock_prices(self, symbol, from_date, to_date):
        try:
            req = model.daily_stock_price(symbol, from_date, to_date, 1, 1000)
            res = self.client.daily_stock_price(self.config, req)

            # ✅ Kiểm tra đúng status
            status = res.get('status')

            if status == 'Success':  # hoặc status code 200 tùy API của bạn
                data = res.get('data', [])
                if not data:
                    logger.warning(f"⚠️ Không có data cho {symbol} [{from_date} - {to_date}]")
                    return True  # Hợp lệ: ngày không có giao dịch (nghỉ lễ, weekend)

                df = pd.DataFrame(data)
                df_prices = pd.DataFrame({
                    'symbol': symbol,
                    'trading_date': pd.to_datetime(df['TradingDate'], dayfirst=True).dt.date,
                    'price_change': pd.to_numeric(df['PriceChange']),
                    'per_price_change': pd.to_numeric(df['PerPriceChange']),
                    'ceiling_price': pd.to_numeric(df['CeilingPrice']),
                    'floor_price': pd.to_numeric(df['FloorPrice']),
                    'ref_price': pd.to_numeric(df['RefPrice']),
                    'open_price': pd.to_numeric(df['OpenPrice']),
                    'highest_price': pd.to_numeric(df['HighestPrice']),
                    'lowest_price': pd.to_numeric(df['LowestPrice']),
                    'close_price': pd.to_numeric(df['ClosePrice']),
                    'average_price': pd.to_numeric(df['AveragePrice']),
                    'close_price_adjusted': pd.to_numeric(df['ClosePriceAdjusted']),
                    'total_match_vol': pd.to_numeric(df['TotalMatchVol']).astype(int),
                    'total_match_val': pd.to_numeric(df['TotalMatchVal']).astype(float),
                    'total_deal_vol': pd.to_numeric(df['TotalDealVol']).astype(int),
                    'total_deal_val': pd.to_numeric(df['TotalDealVal']).astype(float),
                    'foreign_buy_vol_total': pd.to_numeric(df['ForeignBuyVolTotal']).astype(int),
                    'foreign_sell_vol_total': pd.to_numeric(df['ForeignSellVolTotal']).astype(int),
                    'foreign_buy_val_total': pd.to_numeric(df['ForeignBuyValTotal']).astype(float),
                    'foreign_sell_val_total': pd.to_numeric(df['ForeignSellValTotal']).astype(float),
                    'foreign_current_room': pd.to_numeric(df['ForeignCurrentRoom']).astype(float),
                    'net_buy_sell_vol': pd.to_numeric(df['NetBuySellVol']).astype(int),
                    'net_buy_sell_val': pd.to_numeric(df['NetBuySellVal']).astype(int),
                    'total_traded_vol': pd.to_numeric(df['TotalTradedVol']).astype(int),
                    'total_traded_value': pd.to_numeric(df['TotalTradedValue']).astype(float),
                    'total_buy_trade': pd.to_numeric(df['TotalBuyTrade']).astype(float),
                    'total_buy_trade_vol': pd.to_numeric(df['TotalBuyTradeVol']).astype(float),
                    'total_sell_trade': pd.to_numeric(df['TotalSellTrade']).astype(float),
                    'total_sell_trade_vol': pd.to_numeric(df['TotalSellTradeVol']).astype(float),
                    'time_str': df['TradingDate']
                })

                self.db.save_data(df_prices, 'daily_stock_prices', ['symbol', 'trading_date'])
                return True

            elif status == 401 or res.get('statusCode') == 401:
                logger.error("Lỗi xác thực: Access Token hết hạn hoặc sai.")
                self.get_access_token()
                return False

            elif status == 429 or res.get('statusCode') == 429:
                logger.warning(f"⏳ Rate limit 429 cho {symbol}, chờ 2s rồi retry...")
                time.sleep(2)
                return False

            else:
                # ✅ Log rõ lý do thất bại để debug
                logger.error(f"❌ API Error cho {symbol} [{from_date}-{to_date}]: "
                             f"status={status}, message={res.get('message')}")
                return False  # ✅ return False để trigger retry bên ngoài

        except Exception as e:
            logger.error(f"Network/SQL Error: {e}")
            return False

    def sync_all_stock_prices(self, market='HOSE', from_date='01/01/2021'):
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

    def maintenance_sync(self, market='HOSE'):
        print("\nChọn chế độ cập nhật:")
        print("  1. OHLC (daily_ohlc)")
        print("  2. Price (daily_stock_prices)")
        print("  3. Exit)")

        while True:
            choice = input("Nhập lựa chọn (1/2/3): ").strip()
            if choice == '1':
                mode = 'ohlc'
                break
            elif choice == '2':
                mode = 'price'
                break
            elif choice == '3':
                logger.info("Out maintenance")
                return
            else:
                print("Vui lòng nhập 1 2 hoặc 3")

        symbols = self.db.get_all_symbols_except_CQ(market=market, only_companies=True)
        now = datetime.now()
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

        if now >= market_close:
            to_date = now.date()  # Hôm nay nếu sau 16
        else:
            to_date = now.date() - timedelta(days=1)  # Hôm qua nếu trước 16

        to_date_str = to_date.strftime('%d/%m/%Y')

        logger.info(f"🔄 Bắt đầu bảo trì dữ liệu sàn {market} cho {len(symbols)} mã")
        pbar_total = tqdm(symbols, desc=f"Maintenance {market}")

        for symbol in pbar_total:
            try:
                pbar_total.set_postfix({"current": symbol})
                if mode == 'ohlc':
                    last_ohlc = self.db.get_latest_trading_date('daily_ohlc', symbol)
                    start_ohlc = (last_ohlc + timedelta(days=1)) if last_ohlc \
                        else datetime.strptime("01/01/2015", '%d/%m/%Y').date()

                    if start_ohlc <= to_date:
                        self.fetch_daily_ohlc(symbol, start_ohlc.strftime('%d/%m/%Y'), to_date_str)

                elif mode == 'price':
                    last_price = self.db.get_latest_trading_date('daily_stock_prices', symbol)
                    start_price = (last_price + timedelta(days=1)) if last_price \
                        else datetime.strptime("01/01/2021", '%d/%m/%Y').date()

                    if start_price <= to_date:
                        self.fetch_daily_stock_prices(symbol, start_price.strftime('%d/%m/%Y'), to_date_str)

            except Exception as e:
                logger.error(f"Lỗi bảo trì tại mã {symbol}: {e}")
            finally:
                time.sleep(0.3)

    def repair_all_gaps(self, market='HOSE'):
        """Tự động tìm và vá tất cả lỗ hổng cho các mã trên sàn"""
        symbols = self.db.get_all_symbols_except_CQ(market=market, only_companies=True)
        logger.info(f"🛠 Đang kiểm tra lỗ hổng dữ liệu cho {len(symbols)} mã...")

        pbar = tqdm(symbols, desc="Repairing Gaps")
        total_gaps_found = 0
        total_gaps_fixed = 0

        for symbol in pbar:
            gaps = self.db.get_data_gaps(symbol)
            if not gaps:
                continue

            total_gaps_found += len(gaps)
            pbar.set_postfix({"repairing": symbol, "gaps": len(gaps)})

            for gap_start, gap_end in gaps:
                str_start = gap_start.strftime('%d/%m/%Y')
                str_end = gap_end.strftime('%d/%m/%Y')
                logger.info(f"🩹 Vá mã {symbol}: {str_start} -> {str_end}")

                try:
                    self.fetch_daily_stock_prices(symbol, str_start, str_end)
                    total_gaps_fixed += 1
                except Exception as e:
                    logger.error(f"Lỗi vá gap {symbol} [{str_start}-{str_end}]: {e}")

        # ✅ Tổng kết sau khi repair xong
        logger.info(f"✅ Hoàn tất repair: {total_gaps_fixed}/{total_gaps_found} gaps đã được vá")


if __name__ == "__main__":
    api = ssi_api(config)
    #api.fetch_and_sync_securities()
    #api.sync_all_markets()

    #api.fetch_daily_ohlc("SSI","01/01/2015","26/03/2026")
    #api.sync_all_ohlc()

    #api.fetch_daily_stock_prices("SSI","01/01/2021","26/03/2026")
    #api.sync_all_stock_prices(market='HOSE', from_date='01/01/2021')
    api.maintenance_sync(market='HOSE')
    #api.repair_all_gaps(market='HOSE')