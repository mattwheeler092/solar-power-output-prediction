import os

# Load GCP bucket name and service key
GS_BUCKET = os.environ["GS_BUCKET"]
GS_SERVICE_KEY = os.environ["GS_SERVICE_KEY"]

# Load MongoDB username and password
MONGO_USERNAME = os.environ["MONGO_USERNAME"]
MONGO_PASSWORD = os.environ["MONGO_PASSWORD"]

# Load MongoDB database variables
MONGO_IP = os.environ["MONGO_IP"]
MONGO_DB = os.environ["MONGO_DB"]
MONGO_COLLECTION = os.environ["MONGO_COLLECTION"]

# Load Visual crossing weather API key
WEATHER_API_KEY = os.environ['WEATHER_API_KEY']

# Define the time period to collect weather data
START_DATE = "2022-01-01"
END_DATE = "2022-12-31"

# Define the project date format
DATE_FORMAT = "%Y-%m-%d"

# Define the collection cache filename
COLLECTION_CACHE_FILE_NAME = "collection_cache.json"

# Define the spark cache filename
SPARK_CACHE_FILE_NAME = "spark_cache.json"

# Define how many weather months to process per airflow job
NUM_MONTHS_TO_PROCESS_PER_JOB = 400
