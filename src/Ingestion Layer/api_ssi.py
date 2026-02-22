from ssi_fc_data import fc_md_client, model
from src import config
import pandas as pd
import logging
from DatabaseHandler import DatabaseHandler
logger = logging.getLogger(__name__)

class ssi_api:
    def __init__(self, config):
        self.config = config
        self.client = fc_md_client.MarketDataClient(self.config)
        self.db = DatabaseHandler()

    def get_access_token(self):
        # SSI yêu cầu access token để gọi API
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

    def fetch_daily_stock_prices(self, symbol, from_date, to_date):
        """Lấy dữ liệu giá chi tiết từ 2021 (Mục 4.9 tài liệu)"""
        req = model.daily_stock_price(symbol, from_date, to_date)
        res = self.client.daily_stock_price(self.config,req)

        if res.get('status') == 200:
            df = pd.DataFrame(res.get('dataList', []))
            if df.empty: return

            # Mapping cực kỳ quan trọng vì bảng này rất nhiều cột
            # Chú ý: API trả về camelCase, DB dùng snake_case
            df_prices = pd.DataFrame({
                'symbol': symbol,
                'trading_date': pd.to_datetime(df['tradingdate'], dayfirst=True).dt.date,
                'price_change': pd.to_numeric(df['pricechange']),
                'per_price_change': pd.to_numeric(df['perpricechange']),
                'ceiling_price': pd.to_numeric(df['ceilingprice']),
                'floor_price': pd.to_numeric(df['floorprice']),
                'ref_price': pd.to_numeric(df['refprice']),
                'open_price': pd.to_numeric(df['openprice']),
                'highest_price': pd.to_numeric(df['highestprice']),
                'lowest_price': pd.to_numeric(df['lowestprice']),
                'close_price': pd.to_numeric(df['closeprice']),
                'average_price': pd.to_numeric(df['averageprice']),
                'close_price_adjusted': pd.to_numeric(df['closepriceadjusted']),
                'total_match_vol': pd.to_numeric(df['totalmatchvol']).astype(int),
                'total_match_val': pd.to_numeric(df['totalmatchval']).astype(float),
                'total_deal_vol': pd.to_numeric(df['totaldealvol']).astype(int),
                'total_deal_val': pd.to_numeric(df['totaldealval']).astype(float),
                'foreign_buy_vol_total': pd.to_numeric(df['foreignbuyvol']).astype(int),
                'foreign_sell_vol_total': pd.to_numeric(df['foreignsellvol']).astype(int),
                'foreign_buy_val_total': pd.to_numeric(df['foreignbuyval']).astype(float),
                'foreign_sell_val_total': pd.to_numeric(df['foreignsellval']).astype(float),
                'net_buy_sell_vol': pd.to_numeric(df['netforeignvol']).astype(int),
                'total_traded_vol': pd.to_numeric(df['totaltradedvol']).astype(int),
                'total_traded_value': pd.to_numeric(df['totaltradedvalue']).astype(float),
                'time_str': df['tradingdate']  # Lưu tạm string date để debug nếu cần
            })

            self.db.save_data(df_prices, 'daily_stock_prices', ['symbol', 'trading_date'])

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
    api.fetch_daily_ohlc("SSI","01/01/2015","01/01/2026")