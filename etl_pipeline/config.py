import os
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    IntegerType, 
    DoubleType
)

# Load GCP bucket name and service key
GS_BUCKET = os.environ["GS_BUCKET_NAME"]
GS_SERVICE_KEY = os.environ["GS_SERVICE_ACCOUNT_KEY_FILE"]

# Load MongoDB username and password
MONGO_USERNAME = os.environ["MONGO_USERNAME"]
MONGO_PASSWORD = os.environ["MONGO_PASSWORD"]

# Load MongoDB database variables
MONGO_IP = os.environ["MONGO_IP"]
MONGO_DB = os.environ["MONGO_DB_NAME"]
MONGO_COLLECTION = os.environ["MONGO_COLLECTION_NAME"]

# Load Visual crossing weather API key
WEATHER_API_KEY = os.environ['WEATHER_API_KEY']

# Define how many months of weather data to collect per airflow job
NUM_MONTHS_TO_PROCESS_PER_JOB = 1

# Define the time period to collect weather data for each location
START_DATE = "2022-01-01"
END_DATE = "2022-12-31"

# Define the location of the csv containing the lat / lon coordinates
LOCATION_DATA_FILE_NAME = "locations.csv"

# Define the collection cache GCP filename
COLLECTION_CACHE_FILE_NAME = "collection_cache.json"

# Define the spark cache GCP filename
SPARK_CACHE_FILE_NAME = "spark_cache.json"

# Define weather data GCP folder name
GCP_DATA_FOLDER = "data"

# Define airflow job id / start date / schedule (every 3 hours)
AIRFLOW_DAG_ID = "weather_collection_pipeline"
AIRFLOW_START_DATE = "2023-02-26"
AIRFLOW_SCHEDULE = "0 */3 * * *"

# Define the project date format
DATE_FORMAT = "%Y-%m-%d"

# Define columns to be dropped in spark processing
DROP_COLUMNS = ["tzoffset", "icon", "stations"]

# Define columns to be rename in spark processing
COLUMN_RENAME_DICT = {
    "cloudcover": "cloud_cover_perc",
    "uvindex": "uv_index",
    "solarradiation": "solar_radiation",
    "solarenergy": "solar_energy",
}

# Define the schema for each data field
WEATHER_DATA_SCHEMA = StructType([
    StructField('datetimeEpoch', IntegerType(), False),
    StructField('temp', DoubleType(), False),
    StructField('feelslike', DoubleType(), False),
    StructField('humidity', DoubleType(), False),
    StructField('dew', DoubleType(), False),
    StructField('precip', DoubleType(), False),
    StructField('precipprob', DoubleType(), False),
    StructField('snow', DoubleType(), False),
    StructField('snowdepth', DoubleType(), False),
    StructField('preciptype', StringType(), False),
    StructField('windgust', DoubleType(), False),
    StructField('windspeed', DoubleType(), False),
    StructField('winddir', DoubleType(), False),
    StructField('pressure', DoubleType(), False),
    StructField('visibility', DoubleType(), False),
    StructField('cloudcover', DoubleType(), False),
    StructField('solarradiation', DoubleType(), False),
    StructField('solarenergy', DoubleType(), False),
    StructField('uvindex', DoubleType(), False),
    StructField('severerisk', DoubleType(), False),
    StructField('conditions', StringType(), False),
    StructField('icon', StringType(), False),
    StructField('stations', StringType(), False),
    StructField('source', StringType(), False),
    StructField('time', StringType(), False),
    StructField('lat', DoubleType(), False),
    StructField('lon', DoubleType(), False),
    StructField('timezone', StringType(), False),
    StructField('tzoffset', DoubleType(), False),
    StructField('date', StringType(), False),
    StructField('day_agg_tempmax', DoubleType(), False),
    StructField('day_agg_tempmin', DoubleType(), False),
    StructField('day_agg_temp', DoubleType(), False),
    StructField('day_agg_feelslikemax', DoubleType(), False),
    StructField('day_agg_feelslikemin', DoubleType(), False),
    StructField('day_agg_feelslike', DoubleType(), False),
    StructField('day_agg_dew', DoubleType(), False),
    StructField('day_agg_humidity', DoubleType(), False),
    StructField('day_agg_precip', DoubleType(), False),
    StructField('day_agg_precipprob', DoubleType(), False),
    StructField('day_agg_precipcover', DoubleType(), False),
    StructField('day_agg_preciptype', StringType(), False),
    StructField('day_agg_snow', DoubleType(), False),
    StructField('day_agg_snowdepth', DoubleType(), False),
    StructField('day_agg_windgust', DoubleType(), False),
    StructField('day_agg_windspeed', DoubleType(), False),
    StructField('day_agg_winddir', DoubleType(), False),
    StructField('day_agg_pressure', DoubleType(), False),
    StructField('day_agg_cloudcover', DoubleType(), False),
    StructField('day_agg_visibility', DoubleType(), False),
    StructField('day_agg_uvindex', DoubleType(), False),
    StructField('day_agg_severerisk', DoubleType(), False),
    StructField('day_agg_sunrise', StringType(), False),
    StructField('day_agg_sunset', StringType(), False),
    StructField('day_agg_moonphase', DoubleType(), False),
    StructField('day_agg_conditions', StringType(), False),
    StructField('day_agg_description', StringType(), False),
    StructField('day_agg_source', StringType(), False),
])
