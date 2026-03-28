import logging
import sys
from datetime import datetime, timedelta

from DatabaseHandler import DatabaseHandler
from api_client import SSIAPIClient
from transformer import DataTransformer
from sync_service import SyncService
from gap_service import GapRepairService
from src import config

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    # Khởi tạo các dependency
    try:
        db = DatabaseHandler()          # Đọc config.DB_URI bên trong
    except Exception as e:
        logger.error(f"Không thể kết nối database: {e}")
        return

    api_client = SSIAPIClient(config)   # config từ src
    transformer = DataTransformer()
    sync_service = SyncService(api_client, db, transformer)
    gap_service = GapRepairService(db, sync_service)

    # Menu
    while True:
        print("\n" + "="*50)
        print("CHƯƠNG TRÌNH ĐỒNG BỘ DỮ LIỆU CHỨNG KHOÁN")
        print("="*50)
        print("1. Đồng bộ danh mục securities (tất cả sàn)")
        print("2. Đồng bộ OHLC toàn bộ")
        print("3. Đồng bộ giá chi tiết toàn bộ")
        print("4. Bảo trì (chỉ đồng bộ các ngày thiếu)")
        print("5. Vá lỗ hổng dữ liệu (repair gaps)")
        print("6. Thoát")

        choice = input("Nhập lựa chọn (1-6): ").strip()

        if choice == '1':
            logger.info("Bắt đầu đồng bộ danh mục securities")
            sync_service.sync_all_markets()
            logger.info("Hoàn tất đồng bộ danh mục")

        elif choice == '2':
            market = input("Nhập mã sàn (HOSE/HNX/UPCOM, để trống mặc định HOSE): ").strip() or 'HOSE'
            from_date = input("Nhập ngày bắt đầu (dd/mm/yyyy, để trống mặc định 01/01/2015): ").strip() or '01/01/2015'
            to_date = input("Nhập ngày kết thúc (dd/mm/yyyy, để trống mặc định hôm qua): ").strip()
            if not to_date:
                # Tính ngày hôm qua
                to_date = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
            sync_service.sync_all_ohlc(market, from_date, to_date)

        elif choice == '3':
            market = input("Nhập mã sàn (HOSE/HNX/UPCOM, để trống mặc định HOSE): ").strip() or 'HOSE'
            from_date = input("Nhập ngày bắt đầu (dd/mm/yyyy, để trống mặc định 01/01/2021): ").strip() or '01/01/2021'
            sync_service.sync_all_stock_prices(market, from_date)

        elif choice == '4':
            market = input("Nhập sàn (HOSE/HNX/UPCOM, mặc định HOSE): ").strip() or 'HOSE'
            mode = input("Chọn dữ liệu (ohlc/price, mặc định ohlc): ").strip().lower() or 'ohlc'
            if mode not in ('ohlc', 'price'):
                print("Chế độ không hợp lệ, mặc định ohlc")
                mode = 'ohlc'
            sync_service.maintenance_sync(market, mode)

        elif choice == '5':
            market = input("Nhập sàn (HOSE/HNX/UPCOM, mặc định HOSE): ").strip() or 'HOSE'
            gap_service.repair_all_gaps(market)

        elif choice == '6':
            print("Thoát chương trình.")
            break

        else:
            print("Vui lòng nhập số từ 1 đến 6.")

if __name__ == "__main__":
    main()