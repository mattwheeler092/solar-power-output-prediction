import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from openweather import *
from visualcrossing import *
from dotenv import load_dotenv
from datetime import datetime as dt
from datetime import timedelta
import os
from user_definition import (
    OW_API_KEY,
    VC_API_KEY,
    GS_BUCKET_NAME,
    GS_SERVICE_ACCOUNT_KEY_FILE,
    LAT_LON_TUPLE,
    SCHEDULE_INTERVAL
)
from transform_load import transform_load_to_mongo

client = storage.Client.from_service_account_json(GS_SERVICE_ACCOUNT_KEY_FILE)
last_fetch_date_map = read_last_fetch_date_map(
    GS_BUCKET_NAME, GS_SERVICE_ACCOUNT_KEY_FILE, file_name="VC_last_fetch.json"
)


def get_visualcrossing_history(execution_date, **context):
    """Fetch data from Visual Crossing API and save as a CSV to GCS"""
    timestamp = str(int(execution_date.timestamp()))
    df_list, errors, last_fetch_date_map = fetch_visualcrossing_history(
        last_fetch_date_map, VC_API_KEY
    )
    for coordinate, last_date, df in df_list:
        # file name format: VC_lat_lon_lastFetchDate.csv
        file_name = f"VC_{coordinate[0]}_{coordinate[1]}_{last_date}.csv"
        blob_name = os.path.join(timestamp, file_name)
        write_csv_to_gcs(GS_BUCKET_NAME, blob_name, GS_SERVICE_ACCOUNT_KEY_FILE, df)
    update_last_fetch_date_map(
        last_fetch_date_map, GS_BUCKET_NAME, GS_SERVICE_ACCOUNT_KEY_FILE
    )


def get_openweather_locations():
    """Fetch location data based on coordinates from Open Weather API and save as a CSV file to GCS"""
    locations = LAT_LON_TUPLE
    df, errors = fetch_openweather_locations(locations, OW_API_KEY)
    blob_name = f"OW_coordinate_location.csv"
    write_csv_to_gcs(GS_BUCKET_NAME, blob_name, GS_SERVICE_ACCOUNT_KEY_FILE, df)

def transform_load(execution_date, **context):
    """Wrapper of transform_load.py"""
    timestamp = str(int(execution_date.timestamp()))
    transform_load_to_mongo(folder=timestamp)


# default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": dt(2023, 2, 17),
    "retries": 0,
}

# define the DAG
dag = DAG(
    "historical_weather_etl",
    default_args=default_args,
    description="ETL pipeline of historical weather data",
    schedule_interval=SCHEDULE_INTERVAL,  # run every 3 hours
    catchup=False
)

# operator to fetch data from VC
fetch_history_operator = PythonOperator(
    task_id="get_visualcrossing_history",
    python_callable=get_visualcrossing_history,
    provide_context=True,
    dag=dag,
)

# operator to fetch data from OW
fetch_location_operator = PythonOperator(
    task_id="get_openweather_locations",
    python_callable=get_openweather_locations,
    dag=dag,
)

# operator for spark job
transform_load_operator = PythonOperator(
    task_id='spark_transform_load',
    python_callable=transform_load,
    provide_context=True,
    dag=dag
)

# transform_load_operator = SparkSubmitOperator(
#     task_id="spark_transform_load",
#     packages="com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
#     exclude_packages="javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri",
#     conf={
#         "spark.driver.userClassPathFirst": True,
#         "spark.executor.userClassPathFirst": True,
#     },
#     verbose=True,
#     application="transform_load.py",
#     dag=dag,
# )



[fetch_history_operator, fetch_location_operator] >> transform_load_operator
