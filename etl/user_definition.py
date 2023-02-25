import os
from dotenv import load_dotenv


# load .env variables
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

# API call variables ----------------------------------------------------

# (latitude, longitude) pairs
# with open("locations.csv", "r") as f:
#     data = f.read().split("\n")
#     LAT_LON_TUPLE = [tuple(float(j) for j in i.split(",")) for i in data[1:]]
LAT_LON_TUPLE = [
    (34.582769, -117.409214),
    (37.765206, -122.241636),
    (41.487114, -120.542456),
    (38.419356, -120.824103),
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
