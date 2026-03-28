from ssi_fc_data import fc_md_client, model

class SSIAPIClient:
    """Giao tiếp với SSI market data API"""
    def __init__(self, config):
        self.config = config
        self.client = fc_md_client.MarketDataClient(self.config)

    def get_access_token(self):
        return self.client.access_token()

    def get_securities(self, market: str, page: int = 1, page_size: int = 1000) -> dict:
        """Lấy danh sách chứng khoán"""
        req = model.securities(market, page, page_size)
        return self.client.securities(self.config, req)

    def get_daily_ohlc(self, symbol: str, from_date: str, to_date: str,
                       page: int = 1, page_size: int = 9999) -> dict:
        """Lấy dữ liệu OHLC"""
        req = model.daily_ohlc(symbol, from_date, to_date, page, page_size, True)
        return self.client.daily_ohlc(self.config, req)

    def get_daily_stock_price(self, symbol: str, from_date: str, to_date: str,
                              page: int = 1, page_size: int = 1000) -> dict:
        """Lấy dữ liệu giá chi tiết"""
        req = model.daily_stock_price(symbol, from_date, to_date, page, page_size)
        return self.client.daily_stock_price(self.config, req)
