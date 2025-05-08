import pandas as pd
import logging
from config import DATA_PATH

# Thiết lập logging (để ghi log)
logging.basicConfig(
    filename='etl_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8-sig'
)

def read_data():
    """Đọc dữ liệu từ file CSV."""
    try:
        logging.info("Bắt đầu đọc dữ liệu từ file CSV")
        df = pd.read_csv(DATA_PATH)
        logging.info(f"Đọc thành công: {len(df)} bản ghi")
        return df
    except Exception as e:
        logging.error(f"Lỗi khi đọc file CSV: {str(e)}")
        raise

def check_and_handle_missing_data(df):
    """Kiểm tra và xử lý giá trị thiếu (NaN)."""
    logging.info("Kiểm tra giá trị NaN")
    print("Số lượng giá trị NaN theo cột:")
    print(df.isnull().sum())

    # Xử lý NaN cho các cột quan trọng
    df['passenger_count'] = df['passenger_count'].fillna(0).astype(int)  # Điền 0 cho số hành khách
    df['trip_distance'] = df['trip_distance'].fillna(0)  # Điền 0 cho khoảng cách
    df['RatecodeID'] = df['RatecodeID'].fillna(5)  # Điền 5 (Negotiated fare) cho RatecodeID
    df['payment_type'] = df['payment_type'].fillna(5)  # Điền 5 (Unknown) cho payment_type
    df['fare_amount'] = df['fare_amount'].fillna(0)  # Điền 0 cho fare_amount
    df['pickup_longitude'] = df['pickup_longitude'].fillna(0)  # Điền 0 cho kinh độ
    df['pickup_latitude'] = df['pickup_latitude'].fillna(0)  # Điền 0 cho vĩ độ
    df['dropoff_longitude'] = df['dropoff_longitude'].fillna(0)
    df['dropoff_latitude'] = df['dropoff_latitude'].fillna(0)

    # Loại bỏ các hàng có NaN trong cột datetime, Hàm loại bỏ hàng hoặc cột chứa NaN. Tham số subset chỉ định các cột cần kiểm tra.
    df = df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime']) 

    logging.info("Đã xử lý giá trị NaN")
    print("Số lượng giá trị NaN sau xử lý:")
    print(df.isnull().sum())
    return df

# Định nghĩa hàm extract để chạy toàn bộ quy trình Extract.
def extract():
    """Chạy toàn bộ quy trình extract."""
    df = read_data()
    df = check_and_handle_missing_data(df)
    return df