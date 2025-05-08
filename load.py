from google.cloud import bigquery
from pandas_gbq import to_gbq
import logging
from config import PROJECT_ID, DATASET_ID, LOCATION

# Thiết lập logging
logging.basicConfig(
    filename='etl_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8-sig'
)

def create_dataset(client):
    """Tạo dataset trong BigQuery nếu chưa tồn tại."""
    logging.info(f"Tạo dataset {DATASET_ID}")
    dataset = bigquery.Dataset(DATASET_ID)
    dataset.location = LOCATION
    client.create_dataset(dataset, exists_ok=True)
    logging.info(f"Dataset {DATASET_ID} đã sẵn sàng")

def upload_to_bigquery(tables):
    """Tải các bảng lên BigQuery."""
    client = bigquery.Client()
    create_dataset(client)

    for table_name, df in tables.items():
        logging.info(f"Tải bảng {table_name} lên BigQuery")
        try:
            to_gbq(df, f"{DATASET_ID}.{table_name}", project_id=PROJECT_ID, if_exists='replace')
            logging.info(f"Tải bảng {table_name} thành công")
        except Exception as e:
            logging.error(f"Lỗi khi tải bảng {table_name}: {str(e)}")
            raise

def create_analysis_report():
    """Tạo bảng báo cáo phân tích trên BigQuery."""
    client = bigquery.Client()
    query = """
    CREATE OR REPLACE TABLE uber_dataset.tbl_analysis_report AS (
      SELECT
        f.VendorID,
        dt.tpep_pickup_datetime,
        dt.tpep_dropoff_datetime,
        p.passenger_count,
        td.trip_distance,
        rc.RatecodeID,
        f.store_and_fwd_flag,
        pl.pickup_latitude,
        pl.pickup_longitude,
        dl.dropoff_latitude,
        dl.dropoff_longitude,
        pt.payment_type,
        f.fare_amount,
        f.extra,
        f.mta_tax,
        f.tip_amount,
        f.tolls_amount,
        f.improvement_surcharge,
        f.total_amount
      FROM
        uber_dataset.fact_table f
        JOIN uber_dataset.datetime_dim dt ON f.datetime_id = dt.datetime_id
        JOIN uber_dataset.passenger_count_dim p ON f.passenger_count_id = p.passenger_count_id
        JOIN uber_dataset.trip_distance_dim td ON f.trip_distance_id = td.trip_distance_id
        JOIN uber_dataset.rate_code_dim rc ON f.rate_code_id = rc.rate_code_id
        JOIN uber_dataset.pickup_location_dim pl ON f.pickup_location_id = pl.pickup_location_id
        JOIN uber_dataset.dropoff_location_dim dl ON f.dropoff_location_id = dl.dropoff_location_id
        JOIN uber_dataset.payment_type_dim pt ON f.payment_type_id = pt.payment_type_id
    )
    """
    logging.info("Tạo bảng tbl_analysis_report")
    try:
        query_job = client.query(query)
        query_job.result()
        logging.info("Tạo bảng tbl_analysis_report thành công")
    except Exception as e:
        logging.error(f"Lỗi khi tạo bảng tbl_analysis_report: {str(e)}")
        raise

def export_report():
    """Xuất bảng báo cáo ra file CSV."""
    client = bigquery.Client()
    query = f"SELECT * FROM {DATASET_ID}.tbl_analysis_report"
    logging.info("Xuất bảng tbl_analysis_report ra CSV")
    try:
        df_report = client.query(query).to_dataframe()
        df_report.to_csv("tbl_analysis_report.csv", index=False, encoding="utf-8-sig")
        logging.info("Xuất file tbl_analysis_report.csv thành công")
    except Exception as e:
        logging.error(f"Lỗi khi xuất báo cáo: {str(e)}")
        raise

def load(tables):
    """Chạy toàn bộ quy trình load."""
    upload_to_bigquery(tables)
    create_analysis_report()
    export_report()