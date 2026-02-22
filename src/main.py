import config
from api_ssi import ssi_api
import time
from datetime import datetime

def run_historical_update():
    api = ssi_api(config)

    # Thiết lập thời gian từ 01/01/2015 đến nay
    start_date = "01/01/2015"
    end_date = datetime.now().strftime("%d/%m/%Y")
    # Chỉ lấy HOSE và HNX
    target_markets = ["hose", "hnx"]

    for mkt in target_markets:
        print(f"🚀 Bắt đầu quét sàn: {mkt.upper()}")
        df_sec = api.get_securities(mkt)

        if not df_sec.empty:
            symbols = df_sec['ymbol'].unique().tolist()
            for i, symbol in enumerate(symbols):
                print(f"[{i + 1}/{len(symbols)}] Đang tải lịch sử: {symbol}")
                try:
                    # Hàm này trong api_ssi.py cũng đã tự động gọi db.save_data
                    api.get_data_ohlc_daily(symbol, start_date, end_date)
                except Exception as e:
                    print(f"Lỗi tại mã {symbol}: {e}")

                if (i + 1) % 50 == 0:
                    print("Waiting 5 sec...")
                    time.sleep(5)


if __name__ == "__main__":
    run_historical_update()