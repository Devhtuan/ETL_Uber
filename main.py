import os
from extract import extract
from transform import transform
from load import load
from config import GOOGLE_CREDENTIALS
import logging

# Thiết lập logging
  logging.basicConfig(
      filename='etl_log.txt',
      level=logging.INFO,
      format='%(asctime)s - %(levelname)s - %(message)s',
      encoding='utf-8-sig'
  )

def main():
    """Chạy toàn bộ quy trình ETL."""
    logging.info("Bắt đầu quy trình ETL")
    
    # Thiết lập môi trường Google Cloud
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS
    
    # Extract
    df = extract()
    
    # Transform
    tables = transform(df)
    
    # Load
    load(tables)
    
    logging.info("Hoàn tất quy trình ETL")

# Kiểm tra xem file main.py có đang được chạy trực tiếp hay không, nếu đúng thì gọi hàm main
if __name__ == "__main__":
    main()