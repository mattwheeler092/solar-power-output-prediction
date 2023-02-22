# API call variables ----------------------------------------------------

# (latitude, longitude) pairs
with open("locations.csv", "r") as f:
    data = f.read().split("\n")
    LAT_LON_TUPLE = [tuple(float(j) for j in i.split(",")) for i in data[1:]]


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

DROP_COLUMNS = ['weather_id', 'weather_icon', 'source']
