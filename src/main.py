# main.py
import logging
import sys
from datetime import datetime, timedelta

from DatabaseHandler import DatabaseHandler
from api_client import SSIAPIClient
from transformer import DataTransformer
from sync_service import SyncService
from gap_service import GapRepairService
from src import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def main():
    # Khởi tạo các dependency
    try:
        db = DatabaseHandler()
    except Exception as e:
        logger.error(f"Không thể kết nối database: {e}")
        return

    api_client = SSIAPIClient(config)
    transformer = DataTransformer()
    sync_service = SyncService(api_client, db, transformer)
    gap_service = GapRepairService(db, sync_service)

    while True:
        print("\n" + "=" * 50)
        print("CHƯƠNG TRÌNH ĐỒNG BỘ DỮ LIỆU CHỨNG KHOÁN (CLI)")
        print("=" * 50)
        print("1. Đồng bộ danh mục securities (tất cả sàn)")
        print("2. Đồng bộ 1 mã OHLC")
        print("3. Đồng bộ tất cả mã OHLC")
        print("4. Đồng bộ 1 mã giá chi tiết")
        print("5. Đồng bộ tất cả mã giá chi tiết")
        print("6. Bảo trì (cập nhật thiếu)")
        print("7. Vá lỗ hổng dữ liệu")
        print("8. Thoát")

        choice = input("Nhập lựa chọn (1-8): ").strip()

        if choice == '1':
            logger.info("Bắt đầu đồng bộ danh mục securities")
            sync_service.sync_all_markets()
            logger.info("Hoàn tất đồng bộ danh mục")

        elif choice == '2':
            symbol = input("Nhập mã chứng khoán (ví dụ SSI): ").strip().upper()
            from_date = input("Ngày bắt đầu (dd/mm/yyyy, mặc định 01/01/2015): ").strip() or '01/01/2015'
            to_date = input("Ngày kết thúc (dd/mm/yyyy, mặc định hôm qua): ").strip()
            if not to_date:
                to_date = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
            sync_service.sync_one_ohlc(symbol, from_date, to_date)

        elif choice == '3':
            market = input("Sàn (HOSE/HNX/UPCOM, mặc định HOSE): ").strip() or 'HOSE'
            from_date = input("Ngày bắt đầu (dd/mm/yyyy, mặc định 01/01/2015): ").strip() or '01/01/2015'
            to_date = input("Ngày kết thúc (dd/mm/yyyy, mặc định hôm qua): ").strip()
            if not to_date:
                to_date = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
            sync_service.sync_all_ohlc(market, from_date, to_date)

        elif choice == '4':
            symbol = input("Nhập mã chứng khoán: ").strip().upper()
            from_date = input("Ngày bắt đầu (dd/mm/yyyy, mặc định 01/01/2021): ").strip() or '01/01/2021'
            to_date = input("Ngày kết thúc (dd/mm/yyyy, mặc định hôm qua): ").strip()
            if not to_date:
                to_date = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
            sync_service.sync_one_stock_price(symbol, from_date, to_date)

        elif choice == '5':
            market = input("Sàn (HOSE/HNX/UPCOM, mặc định HOSE): ").strip() or 'HOSE'
            from_date = input("Ngày bắt đầu (dd/mm/yyyy, mặc định 01/01/2021): ").strip() or '01/01/2021'
            sync_service.sync_all_stock_prices(market, from_date)

        elif choice == '6':
            market = input("Sàn (mặc định HOSE): ").strip() or 'HOSE'
            mode = input("Loại dữ liệu (ohlc/price, mặc định ohlc): ").strip().lower() or 'ohlc'
            if mode not in ('ohlc', 'price'):
                print("Chế độ không hợp lệ, dùng ohlc")
                mode = 'ohlc'
            sync_service.maintenance_sync(market, mode)

        elif choice == '7':
            market = input("Sàn (mặc định HOSE): ").strip() or 'HOSE'
            gap_service.repair_all_gaps(market)

        elif choice == '8':
            print("Thoát chương trình.")
            break

        else:
            print("Vui lòng nhập số từ 1 đến 8.")

if __name__ == "__main__":
    main()