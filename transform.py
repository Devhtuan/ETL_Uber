import pandas as pd
import logging
from config import RATE_CODE_TYPE, PAYMENT_TYPE_NAME

# Thiết lập logging
logging.basicConfig(
    filename='etl_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8-sig'
)

def check_duplicates(df, table_name, columns):
    """Kiểm tra và xử lý trùng lặp trong bảng."""
    duplicates = df.duplicated(subset=columns, keep=False)
    if duplicates.any():
        logging.warning(f"Bảng {table_name} có {duplicates.sum()} bản ghi trùng lặp")
        print(f"Bảng {table_name} có {duplicates.sum()} bản ghi trùng lặp")
        df = df.drop_duplicates(subset=columns, keep='first')
        logging.info(f"Đã loại bỏ trùng lặp trong bảng {table_name}")
    else:
        logging.info(f"Bảng {table_name} không có trùng lặp")
    return df

def create_datetime_dim(df):
    """Tạo bảng dimension cho thời gian."""
    logging.info("Tạo bảng datetime_dim")
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday
    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday
    datetime_dim['datetime_id'] = datetime_dim.index

    # Kiểm tra trùng lặp
    datetime_dim = check_duplicates(datetime_dim, "datetime_dim", ['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
    return datetime_dim

def create_passenger_count_dim(df):
    """Tạo bảng dimension cho số hành khách."""
    logging.info("Tạo bảng passenger_count_dim")
    passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id', 'passenger_count']]

    # Kiểm tra trùng lặp
    passenger_count_dim = check_duplicates(passenger_count_dim, "passenger_count_dim", ['passenger_count'])
    return passenger_count_dim

def create_trip_distance_dim(df):
    """Tạo bảng dimension cho khoảng cách chuyến đi."""
    logging.info("Tạo bảng trip_distance_dim")
    trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id', 'trip_distance']]

    # Kiểm tra trùng lặp
    trip_distance_dim = check_duplicates(trip_distance_dim, "trip_distance_dim", ['trip_distance'])
    return trip_distance_dim

def create_rate_code_dim(df):
    """Tạo bảng dimension cho mã giá vé."""
    logging.info("Tạo bảng rate_code_dim")
    rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(RATE_CODE_TYPE)
    rate_code_dim = rate_code_dim[['rate_code_id', 'RatecodeID', 'rate_code_name']]

    # Kiểm tra trùng lặp
    rate_code_dim = check_duplicates(rate_code_dim, "rate_code_dim", ['RatecodeID'])
    return rate_code_dim

def create_location_dims(df):
    """Tạo bảng dimension cho địa điểm đón và trả khách."""
    logging.info("Tạo bảng pickup_location_dim và dropoff_location_dim")
    pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id', 'pickup_latitude', 'pickup_longitude']]

    dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id', 'dropoff_latitude', 'dropoff_longitude']]

    # Kiểm tra trùng lặp
    pickup_location_dim = check_duplicates(pickup_location_dim, "pickup_location_dim", ['pickup_longitude', 'pickup_latitude'])
    dropoff_location_dim = check_duplicates(dropoff_location_dim, "dropoff_location_dim", ['dropoff_longitude', 'dropoff_latitude'])
    return pickup_location_dim, dropoff_location_dim

def create_payment_type_dim(df):
    """Tạo bảng dimension cho loại thanh toán."""
    logging.info("Tạo bảng payment_type_dim")
    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(PAYMENT_TYPE_NAME)
    payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_type_name']]

    # Kiểm tra trùng lặp
    payment_type_dim = check_duplicates(payment_type_dim, "payment_type_dim", ['payment_type'])
    return payment_type_dim

def create_fact_table(df, datetime_dim, passenger_count_dim, trip_distance_dim, rate_code_dim, pickup_location_dim, dropoff_location_dim, payment_type_dim):
    """Tạo bảng fact từ dữ liệu gốc và các bảng dimension."""
    logging.info("Tạo bảng fact_table")
    fact_table = df \
        .merge(passenger_count_dim, on='passenger_count') \
        .merge(trip_distance_dim, on='trip_distance') \
        .merge(rate_code_dim, on='RatecodeID') \
        .merge(pickup_location_dim, on=['pickup_longitude', 'pickup_latitude']) \
        .merge(dropoff_location_dim, on=['dropoff_longitude', 'dropoff_latitude']) \
        .merge(datetime_dim, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime']) \
        .merge(payment_type_dim, on='payment_type') \
        [[
            'VendorID', 'datetime_id', 'passenger_count_id', 'trip_distance_id',
            'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id',
            'dropoff_location_id', 'payment_type_id', 'fare_amount', 'extra',
            'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount'
        ]]

    # Kiểm tra trùng lặp trong fact_table
    fact_table = check_duplicates(fact_table, "fact_table", ['datetime_id', 'passenger_count_id', 'trip_distance_id', 'pickup_location_id', 'dropoff_location_id'])
    return fact_table

def transform(df):
    """Chạy toàn bộ quy trình transform."""
    datetime_dim = create_datetime_dim(df)
    passenger_count_dim = create_passenger_count_dim(df)
    trip_distance_dim = create_trip_distance_dim(df)
    rate_code_dim = create_rate_code_dim(df)
    pickup_location_dim, dropoff_location_dim = create_location_dims(df)
    payment_type_dim = create_payment_type_dim(df)
    fact_table = create_fact_table(df, datetime_dim, passenger_count_dim, trip_distance_dim, rate_code_dim, pickup_location_dim, dropoff_location_dim, payment_type_dim)
    return {
        'fact_table': fact_table,
        'datetime_dim': datetime_dim,
        'passenger_count_dim': passenger_count_dim,
        'trip_distance_dim': trip_distance_dim,
        'rate_code_dim': rate_code_dim,
        'pickup_location_dim': pickup_location_dim,
        'dropoff_location_dim': dropoff_location_dim,
        'payment_type_dim': payment_type_dim
    }