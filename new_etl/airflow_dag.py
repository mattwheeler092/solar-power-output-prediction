import logging

from collection_cache import CollectionCache
from spark_cache import SparkCache
from gcp_bucket import GCP_Bucket
from vc_weather import collect_api_data
from utils import load_location_data, generate_gcp_filename

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
        # print(f"Processing:\t{lat = }\t{lon = }\t{start = }\t{end = }")
        logging.info(f"Processing:\t{lat = }\t{lon = }\t{start = }\t{end = }")
        
        # Collect / process visual crossing API data
        api_data = collect_api_data(lat, lon, start, end)

        # Generate filename and upload API data to GCP
        gcp_filename = generate_gcp_filename(lat, lon, start)
        gcp_bucket.upload_file(
            data=api_data, 
            file_name=gcp_filename
        )
        # Add GCP filename to spark cache
        spark_cache.add_file(gcp_filename)



def process_data_with_spark_store_in_mongo():
    """ DAG function to """

    # Initialise gcp bucket and spark cache classes
    gcp_bucket = GCP_Bucket()
    spark_cache = SparkCache()

    for gcp_file in spark_cache.list_cached_files():
        pass