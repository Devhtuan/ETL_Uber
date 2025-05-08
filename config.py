# Cấu hình cho dự án ETL Uber
import os

# Đường dẫn file
DATA_PATH = "data/uber_data.csv"
GOOGLE_CREDENTIALS = ""

# Cấu hình BigQuery
PROJECT_ID = "uber-project-459004"
DATASET_ID = f"{PROJECT_ID}.uber_dataset"
LOCATION = "asia-southeast1"

# Ánh xạ dữ liệu
RATE_CODE_TYPE = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride"
}

PAYMENT_TYPE_NAME = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}