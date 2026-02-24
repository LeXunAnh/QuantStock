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

        if res.get('status') != None:
            data = res.get('data', [])
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

        elif res.get('status') == 401:
            logger.error("Lỗi xác thực: Access Token hết hạn hoặc sai.")
            self.get_access_token()
        else:
            logger.error(f"Lỗi lấy danh sách securities")

    def sync_all_markets(self):
        """Hàm wrapper để quét qua tất cả các sàn quan trọng"""
        markets = ['HOSE', 'HNX', 'UPCOM']
        for mkt in markets:
            logger.info(f"🔄 Đang bắt đầu đồng bộ danh mục sàn: {mkt}")
            self.fetch_and_sync_securities(market=mkt)
        print("Đã insert xong !")

    def fetch_daily_ohlc(self, symbol, from_date, to_date):
        """Lấy dữ liệu OHLC từ năm 2015 (Mục 4.6 tài liệu)"""
        req = model.daily_ohlc(symbol, from_date, to_date, 1, 9999,True)
        res = self.client.daily_ohlc(self.config,req)

        if res.get('status') != None:
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

        elif res.get('status') == 401:
            logger.error("Lỗi xác thực: Access Token hết hạn hoặc sai.")
            self.get_access_token()
        else:
            logger.error(f"Lỗi lấy OHLC cho {symbol}: {res}")

    def sync_all_ohlc(self, market='HOSE', from_date='01/01/2015', to_date='13/02/2026'):
        # 1. Lấy danh sách mã từ DB
        symbols = self.db.get_all_symbols(market=market)
        # 2. Khởi tạo tqdm bao quanh danh sách vòng lặp
        # desc: Mô tả hiện trên thanh progress
        # unit: Đơn vị (ở đây là từng mã 'symbol')
        pbar = tqdm(symbols, desc=f"🚀 Syncing {market}", unit="symbol")

        for symbol in pbar:
            try:
                # Cập nhật nội dung hiển thị trên thanh progress (tùy chọn)
                pbar.set_postfix({"Current": symbol})
                # Gọi hàm fetch dữ liệu
                self.fetch_daily_ohlc(symbol, from_date, to_date)
                # Kiểm soát Rate Limit
                time.sleep(0.3)

            except Exception as e:
                logger.error(f"Lỗi tại mã {symbol}: {e}")
                continue

    def fetch_daily_stock_prices(self, symbol, from_date, to_date):
        """Lấy dữ liệu giá chi tiết với giới hạn 30 ngày/request"""
        # Chuyển đổi string sang object datetime để tính toán
        start_dt = datetime.strptime(from_date, '%d/%m/%Y')
        end_dt = datetime.strptime(to_date, '%d/%m/%Y')
        current_start = start_dt

        # Tính tổng số ngày và số lượng request (chunks) cần thực hiện
        total_days = (end_dt - start_dt).days + 1
        total_chunks = math.ceil(total_days / 30)
        pbar_chunks = tqdm(total=total_chunks, desc=f"  ↳ {symbol}", unit="chunk", leave=False)

        while current_start <= end_dt:
            # Tính ngày kết thúc của chunk (không quá 29 ngày kể từ ngày bắt đầu để đủ 30 ngày)
            # Hoặc không vượt quá end_dt tổng thể
            current_end = min(current_start + timedelta(days=29), end_dt)

            str_start = current_start.strftime('%d/%m/%Y')
            str_end = current_end.strftime('%d/%m/%Y')

            logger.info(f"🔍 Fetching {symbol} chi tiết: {str_start} -> {str_end}")
            # Cập nhật thông tin chi tiết đang fetch ngày nào
            pbar_chunks.set_postfix({"range": f"{str_start}-{str_end}"})

            # Gọi API thực tế cho chunk này
            self._execute_fetch_stock_prices(symbol, str_start, str_end)
            # Update thanh bar thêm 1 đơn vị
            pbar_chunks.update(1)
            # Rate limit nghỉ một chút giữa các chunk
            time.sleep(0.2)
            # Tăng ngày bắt đầu cho chunk tiếp theo
            current_start = current_end + timedelta(days=1)

    def _execute_fetch_stock_prices(self, symbol, from_date, to_date):
        """Lấy dữ liệu giá chi tiết từ 2021 (Mục 4.9 tài liệu)"""
        req = model.daily_stock_price(symbol, from_date, to_date)
        res = self.client.daily_stock_price(self.config,req)

        if res.get('status') != None:
            df = pd.DataFrame(res.get('data', []))
            if df.empty: return

            # Mapping cực kỳ quan trọng vì bảng này rất nhiều cột
            # Chú ý: API trả về camelCase, DB dùng snake_case
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
                'time_str': df['TradingDate']  # Lưu tạm string date để debug nếu cần
            })

            self.db.save_data(df_prices, 'daily_stock_prices', ['symbol', 'trading_date'])

        elif res.get('status') == 401:
            logger.error("Lỗi xác thực: Access Token hết hạn hoặc sai.")
            self.get_access_token()
        else:
            logger.error(f"Lỗi lấy detail price cho {symbol}")

    def sync_all_stock_prices(self, market='HOSE', from_date='01/01/2021', to_date='31/01/2026'):
        """Chỉ đồng bộ giá chi tiết cho các mã cổ phiếu 3 ký tự"""
        # 1. Gọi hàm lọc mã 3 ký tự từ DB
        symbols = self.db.get_all_symbols_except_CQ(market=market, only_companies=True)
        total = len(symbols)

        logger.info(f"🚀 Bắt đầu đồng bộ Stock Prices cho {total} mã (3 ký tự) sàn {market}")

        # Thanh progress bar tổng cho danh sách mã
        pbar_total = tqdm(symbols, desc=f"Overall Progress {market}", unit="symbol")

        for symbol in pbar_total:
            try:
                # Cập nhật postfix cho pbar tổng
                pbar_total.set_postfix({"current": symbol})

                # Gọi hàm fetch kèm logic chia nhỏ 30 ngày chúng ta đã viết
                self.fetch_daily_stock_prices(symbol, from_date, to_date)

                # Nghỉ ngắn giữa các mã để tránh dính Rate Limit gắt
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Lỗi tại mã {symbol}: {e}")
                continue

    #Mutithreading in next update

    def maintenance_sync(self, market='HOSE'):
        """Hàm cập nhật dữ liệu hàng ngày cho tất cả các mã"""
        symbols = self.db.get_all_symbols_except_CQ(market=market, only_companies=True)
        today_str = datetime.now().strftime('%d/%m/%Y')
        today_date = datetime.now().date()

        logger.info(f"🔄 Bắt đầu bảo trì dữ liệu sàn {market} cho {len(symbols)} mã")
        pbar_total = tqdm(symbols, desc=f"Maintenance {market}")

        for symbol in pbar_total:
            try:
                pbar_total.set_postfix({"current": symbol})

                # --- PHẦN 1: CẬP NHẬT DAILY_OHLC (Dữ liệu từ 2015) ---
                last_ohlc = self.db.get_latest_trading_date('daily_ohlc', symbol)
                if last_ohlc:
                    # Nếu đã có data, fetch từ ngày tiếp theo
                    start_ohlc = (last_ohlc + timedelta(days=1))
                else:
                    # Nếu chưa có, fetch từ mốc lịch sử 2015
                    start_ohlc = datetime.strptime("01/01/2015", '%d/%m/%Y').date()

                if start_ohlc <= today_date:
                    str_start_ohlc = start_ohlc.strftime('%d/%m/%Y')
                    # Tận dụng hàm fetch_daily_ohlc sẵn có của bạn
                    self.fetch_daily_ohlc(symbol, str_start_ohlc, today_str)

                # --- PHẦN 2: CẬP NHẬT DAILY_STOCK_PRICES (Dữ liệu từ 2021) ---
                last_price = self.db.get_latest_trading_date('daily_stock_prices', symbol)
                if last_price:
                    start_price = (last_price + timedelta(days=1))
                else:
                    start_price = datetime.strptime("01/01/2021", '%d/%m/%Y').date()

                if start_price <= today_date:
                    str_start_price = start_price.strftime('%d/%m/%Y')
                    # Gọi hàm fetch chi tiết (có logic chia nhỏ 30 ngày) của bạn
                    self.fetch_daily_stock_prices(symbol, str_start_price, today_str)

                # Nghỉ ngắn để duy trì kết nối ổn định
                time.sleep(0.3)

            except Exception as e:
                logger.error(f"❌ Lỗi bảo trì tại mã {symbol}: {e}")
                continue


    # def get_data_securities(self, market):
    #     """Lấy danh sách mã chứng khoán và lưu vào bảng securities"""
    #     try:
    #         # Tham số: Market, PageIndex, PageSize
    #         req = model.securities(market, 1, 1000)
    #         res = self.client.securities(self.config, req)
    #
    #         if res and 'data' in res:
    #             df = pd.DataFrame(res['data'])
    #
    #             # Mapping sang các cột viết thường của bảng 'securities'
    #             df_mapped = pd.DataFrame({
    #                 'symbol': df['Symbol'],
    #                 'market': df['Market'],
    #                 'stock_name': df['StockName'],
    #                 'stock_en_name': df['StockEnName']
    #             })
    #
    #             # Upsert dựa trên cột 'symbol'
    #             self.db.save_data(df_mapped, 'securities', ['symbol'])
    #             return df_mapped
    #         return pd.DataFrame()
    #     except Exception as e:
    #         logger.error(f"Lỗi lấy danh mục chứng khoán: {e}")
    #         return pd.DataFrame()
    # def get_data_ohlc_daily(self, symbol: str, market: str):
    #     try:
    #         start_date = "01/02/2021"
    #         end_date = "28/02/2021"
    #
    #         req = model.daily_stock_price(symbol,start_date,end_date,1,1000,market.lower())
    #
    #         res = self.client.daily_stock_price(self.config, req)
    #
    #         if not res or "data" not in res or not res["data"]:
    #             print(f"⚠ Không có dữ liệu từ API cho {symbol}")
    #             return pd.DataFrame()
    #
    #         df = pd.DataFrame(res["data"])
    #
    #         df_mapped = pd.DataFrame({
    #                 "symbol": df["Symbol"],
    #                 "trading_date": pd.to_datetime(df["TradingDate"], format="%d/%m/%Y").dt.date,
    #                 "price_change": df["PriceChange"],
    #                 "per_price_change": df["PerPriceChange"],
    #                 "ceiling_price": df["CeilingPrice"],
    #                 "floor_price": df["FloorPrice"],
    #                 "ref_price": df["RefPrice"],
    #                 "open_price": df["OpenPrice"],
    #                 "highest_price": df["HighestPrice"],
    #                 "lowest_price": df["LowestPrice"],
    #                 "close_price": df["ClosePrice"],
    #                 "average_price": df["AveragePrice"],
    #                 "close_price_adjusted": df["ClosePriceAdjusted"],
    #                 "total_match_vol": df["TotalMatchVol"],
    #                 "total_match_val": df["TotalMatchVal"],
    #                 "total_deal_vol": df["TotalDealVol"],
    #                 "total_deal_val": df["TotalDealVal"],
    #                 "foreign_buy_vol_total": df["ForeignBuyVolTotal"],
    #                 "foreign_sell_vol_total": df["ForeignSellVolTotal"],
    #                 "foreign_buy_val_total": df["ForeignBuyValTotal"],
    #                 "foreign_sell_val_total": df["ForeignSellValTotal"],
    #                 "foreign_current_room": df["ForeignCurrentRoom"],
    #                 "net_buy_sell_vol": df["NetBuySellVol"],
    #                 "net_buy_sell_val": df["NetBuySellVal"],
    #                 "total_traded_vol": df["TotalTradedVol"],
    #                 "total_traded_value": df["TotalTradedValue"],
    #                 "total_buy_trade": df["TotalBuyTrade"],
    #                 "total_buy_trade_vol": df["TotalBuyTradeVol"],
    #                 "total_sell_trade": df["TotalSellTrade"],
    #                 "total_sell_trade_vol": df["TotalSellTradeVol"],
    #                 "time_str": df["Time"]
    #             })
    #
    #         # Convert numeric columns
    #         numeric_cols = df_mapped.columns.drop(["symbol", "trading_date", "time_str"])
    #         df_mapped[numeric_cols] = df_mapped[numeric_cols].apply(pd.to_numeric, errors="coerce")
    #
    #         self.db.save_data(
    #             df_mapped,
    #             "daily_stock_prices",
    #             ["symbol", "trading_date"])
    #
    #         print(f"✅ Đã cập nhật {len(df_mapped)} dòng cho {symbol}")
    #         return df_mapped
    #
    #     except Exception as e:
    #         print(f"❌ Lỗi khi lấy OHLC {symbol}: {e}")
    #         return pd.DataFrame()

if __name__ == "__main__":
    api = ssi_api(config)
    #api.fetch_and_sync_securities()
    #api.sync_all_markets()
    #api.fetch_daily_ohlc("SSI","01/01/2015","01/01/2026")
    #api.sync_all_ohlc()
    #api.fetch_daily_stock_prices("VNM","01/01/2021","31/01/2026")
    api.sync_all_stock_prices()

    #api.maintenance_sync(market='HOSE') USE FOR LATER