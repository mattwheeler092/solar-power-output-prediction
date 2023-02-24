import os
from dotenv import load_dotenv
import pandas as pd


# Env variables ------------------------------------------------------
load_dotenv()
OW_API_KEY = os.getenv("OW_API_KEY")
VC_API_KEY = os.getenv("VC_API_KEY")
GS_BUCKET_NAME = os.getenv("GS_BUCKET_NAME")
GS_SERVICE_ACCOUNT_KEY_FILE = os.getenv("GS_SERVICE_ACCOUNT_KEY_FILE")
MONGO_IP = os.getenv("MONGO_IP")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")
MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")

# Airflow variables ------------------------------------------------------
SCHEDULE_INTERVAL = "0 */3 * * *"


# API call variables ----------------------------------------------------

# (latitude, longitude) pairs
LIMIT = 5 #! how many locations to fetch data of
dir_path = os.path.dirname(os.path.realpath(__file__))
try:
    locations = pd.read_csv(os.path.join(dir_path, "locations.csv"))
    LAT_LON_TUPLE = [tuple(i) for i in locations[['lat', 'lat']].values][:LIMIT]
except:
    LAT_LON_TUPLE = [(34.1139, 34.1139),
                    (37.7562, 37.7562),
                    (32.8312, 32.8312),
                    (33.9381, 33.9381),
                    (38.5667, 38.5667)
    ]

# GCS variables -------------------------------------------------------

# regex pattern for matching VC historical weather data csv files on GCS
VC_DATA_FILENAME_REGEX = "^VC.+.csv"


# SPARK variables ----------------------------------------------------
COLUMN_RENAME_DICT = {
    "feelslike": "feels_like",
    "precip": "precipitation",
    "severerisk": "severe_risk",
    "uvindex": "uv_index",
}

DROP_COLUMNS = ["weather_id", "weather_icon", "source"]
