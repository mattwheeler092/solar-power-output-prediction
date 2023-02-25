import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
from vc_weather import collect_api_data

from collection_cache import CollectionCache
from spark_cache import SparkCache
from gcp_bucket import GCP_Bucket

from config import (
    AIRFLOW_DAG_ID, 
    AIRFLOW_SCHEDULE,
    AIRFLOW_START_DATE, 
    DATE_FORMAT
)
from utils import (
    load_location_data, 
    generate_gcp_filename,
    kill_airflow_job
)

logging.basicConfig(level=logging.INFO)


def collect_weather_api_data_store_in_gcp():
    """ DAG function to load, process, and store weather API data 
    for a set number of lat / lon locations within GCP. Function also 
    stores file names in spark cache so the next airflow task knows 
    which files need to be processed by spark """

    # Initialise gcp bucket and cache classes
    gcp_bucket = GCP_Bucket()
    collection_cache = CollectionCache()
    spark_cache = SparkCache()

    # Load lat / lon location coordinates
    locations = load_location_data()

    # Loop through location / date range collections assigned to airflow job
    for lat, lon, start, end in collection_cache.generator(locations):

        # Add logging info of location / date range being processed
        logging.info(f"Processing:\t{lat = }\t{lon = }\t{start = }\t{end = }")
        
        # Collect / process visual crossing API data
        api_data = collect_api_data(lat, lon, start, end)

        # Generate filename and upload API data to GCP
        gcp_filename = generate_gcp_filename(lat, lon, start)
        gcp_bucket.upload_file(
            data=api_data, 
            file_name=gcp_filename
        )
        # Add GCP filename to spark cache for future processing
        spark_cache.add_file(gcp_filename)


def process_data_with_spark_store_in_mongo():
    """ DAG function to raw load weather api from GCP / process 
    data with spark / store resulting documents in mongodb """

    # Initialise gcp bucket and spark cache classes
    gcp_bucket = GCP_Bucket()
    collection_cache = CollectionCache()
    spark_cache = SparkCache()

    # Load lat / lon location coordinates
    locations = load_location_data()

    # Kill airflow job if all data collected / processed
    if (collection_cache.collection_complete(locations) and 
            spark_cache.cache_empty()):
        kill_airflow_job()
        return

    # Loop through / process each spark cache file
    for gcp_file in spark_cache.list_cached_files():

        # Add logging info of location / date range being processed
        logging.info(f"Processing:\t{gcp_file = }")

        # TODO: Add functions to process file data with spark and 
        # then upload resulting document to MongoDB server. 
        pass



with DAG(dag_id=AIRFLOW_DAG_ID,
         description="ETL pipeline for historical weather collection",
         start_date=datetime.strptime(AIRFLOW_START_DATE, DATE_FORMAT),
         schedule_interval=AIRFLOW_SCHEDULE) as dag:
    
    task1 = PythonOperator(
        python_callable=collect_weather_api_data_store_in_gcp,
        task_id="task1"
    )
    task2 = PythonOperator(
        python_callable=process_data_with_spark_store_in_mongo,
        task_id="task2"
    )
    task1 >> task2
    
