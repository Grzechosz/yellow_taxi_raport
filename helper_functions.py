import pandas as pd
import pyarrow.parquet as pq


### augmenting data

morning_start = pd.Timestamp("07:00").time()
morning_end = pd.Timestamp("09:00").time()
evening_start = pd.Timestamp("16:00").time()
evening_end = pd.Timestamp("19:00").time()

def is_weekend(time_stamp):
    return 1.0 if time_stamp.day_of_week >= 5 else 0.0

def is_rush_hour(time_stamp):
    time = time_stamp.time()
    return 1.0 if (morning_start <= time <= morning_end) or (evening_start <= time <= evening_end) else 0.0
    
def what_weekday(time_stamp):
    return time_stamp.day_name()

def what_month(time_stamp):
    return time_stamp.month_name()

def impute_missing_values(data):
    data[['store_and_fwd_flag']] = data[['store_and_fwd_flag']].fillna(0.0)
    data[['RatecodeID']] = data[['RatecodeID']].fillna(1.0)
    data[['passenger_count', 'congestion_surcharge', 'Airport_fee','store_and_fwd_flag', 'RatecodeID']]= data[['passenger_count', 'congestion_surcharge', 'Airport_fee','store_and_fwd_flag', 'RatecodeID']].fillna(data[['passenger_count', 'congestion_surcharge', 'Airport_fee','store_and_fwd_flag', 'RatecodeID']].median())
    return data
### loading data

def load_data():
    months_taxi_data = read_taxi_files()
    return pd.concat(months_taxi_data, ignore_index=True)

def read_taxi_files():
    months_taxi_data = []
    for x in range(1,3):  # to get all files, change 3 to 11
        month_data = pq.read_table('taxi_data/yellow_tripdata_2024-'+str(x).zfill(2)+'.parquet').to_pandas()
        months_taxi_data.append(month_data)
    return months_taxi_data

### data quality analysis

def get_duplicates_indexes(data):
    duplicates = data.duplicated(keep = False)
    return duplicates.where(duplicates == True).dropna().index

def get_missing_values(data):
    return data[data.isna().any(axis = 1)]

def get_nan_counts_for_columns(data):
    nans_in_columns = pd.DataFrame(columns= ["column_name", "nan_count"])

    for column in data.columns:
        column_nan_count = data[column].isna().sum()
        nans_in_columns.loc[len(nans_in_columns)] = [column, column_nan_count]

    return nans_in_columns

def filter_weird_values(data):
    data = data[data["VendorID"].isin([1,2])]
    data = data[data["trip_distance"] < 200]
    data = data[data["total_amount"] > 0]
    return data