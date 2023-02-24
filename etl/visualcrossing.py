import requests
from datetime import datetime as dt
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import storage
import io
import json
from user_definition import *


def fetch_visualcrossing_history(last_fetch_date_map, api_key, test=True): # test mode so we don't over-fetch during testing
    """Fetch historical weather data by month for each (lat, lon) pair.
    Return:
    1. a list of tuples, each containing the (lat, lon) pair, the last fetch date, and the corresponding dataFrame.
    2. a list of (lat, lon) pairs for which an error occurred during fetching.
    3. a mapping of (lat, lon) pair to the last fetch date"""
    out_df = []  # tuple of ((lat,lon), df)
    errors = []
    for (lat, lon), last_date in last_fetch_date_map.items():
        try:
            last_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
            if last_date >= datetime.strptime(DEFAULT_LAST_DATE, "%Y-%m-%d"):
                continue
            end_date = (
                _last_day_of_month(last_date)
                if not test
                else last_date + timedelta(days=1)
            )
            endpoint = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{lat}%2C{lon}/{str(last_date.date())}/{str(end_date.date())}?unitGroup=us&key={api_key}&contentType=json"
            response = requests.get(endpoint)
            data = response.json()

            # convert json to dataframe
            df = json_to_df(data, lat, lon)

            # update last fetch date for the location
            last_fetch_date_dict = _get_last_fetch_date(df, lat, lon)
            last_fetch_date_map.update(last_fetch_date_dict)

            # save dataframe
            out_df.append(((lat, lon), str(last_date.date()), df))
        except:
            errors.append((lat, lon))
    return out_df, errors, last_fetch_date_map


def _last_day_of_month(date):
    """Given a date, return the last day of the month."""
    next_month = date.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)


def _get_last_fetch_date(df, lat, lon):
    """Given a DataFrame of weather data, return a dictionary mapping the (lat, lon) pair to the maximum date in the dataFrame."""
    try:
        timestamp = df["datetimeEpoch"].max()
        date = str(dt.fromtimestamp(timestamp).date())
        return {(lat, lon): date}
    except:
        return {}


def read_last_fetch_date_map(
    bucket_name, service_account_key_file, file_name="VC_last_fetch.json"
):
    """Read a mapping of (lat, lon) pair to last fetch date from GCS.
    If such file doesn't already exist, create one where the last fetch dates are initialized as 2022-01-01"""
    client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = client.get_bucket(bucket_name)
    print("reading last fetch mpa")
    blob = bucket.blob(file_name)
    if blob.exists():
        json_str = blob.download_as_string()
        last_fetch_date_map = json.loads(json_str)
        new_last_fetch_date_map = {
            eval(k): v for k, v in last_fetch_date_map.items()}
        new_last_fetch_date_map.update(
            _check_new_locations(new_last_fetch_date_map))
        return new_last_fetch_date_map
    else:
        # if the file does not exist, create one
        locations = LAT_LON_TUPLE
        last_fetch_date = {l: DEFAULT_START_DATE for l in locations}
        update_last_fetch_date_map(
            last_fetch_date, bucket_name, service_account_key_file, file_name
        )
        return last_fetch_date

def _check_new_locations(last_fetch_date_map):
    """Check whether new (lat, lon) pairs are added in user_definition.py."""
    new_loc_dict = {}
    for pair in LAT_LON_TUPLE:
        if pair not in last_fetch_date_map:
            new_loc_dict[pair] = DEFAULT_START_DATE
    return new_loc_dict

def update_last_fetch_date_map(
    last_fetch_date_map,
    bucket_name,
    service_account_key_file,
    file_name="VC_last_fetch.json",
):
    """Write a mapping of (lat, lon) pairs to last fetch dates to a JSON file to GCS."""
    client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    new_last_fetch_date_map = {
        str(k): v for k, v in last_fetch_date_map.items()}
    blob.upload_from_string(
        data=json.dumps(new_last_fetch_date_map), content_type="application/json"
    )


def flatten_json(json_data, lat, lon):
    """Flatten the JSON API response."""
    for k in ["preciptype", "stations"]:
        if type(json_data[k]) is list:
            json_data.update({k: ",".join(json_data[k])})
    json_data.update({"lat": lat, "lon": lon})
    return json_data


def json_to_df(data, lat, lon):
    """Convert JSON API response of a location to a dataframe."""
    result = []
    for d in data["days"]:
        for h in d["hours"]:
            result.append(flatten_json(h, lat, lon))
    return pd.DataFrame(result)


def write_csv_to_gcs(bucket_name, blob_name, service_account_key_file, df):
    """Write and read a blob from GCS using file-like IO"""
    client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    # write df to a temporary file object
    temp_buffer = io.StringIO()
    df.to_csv(temp_buffer, index=False)
    # upload temporary file object
    temp_buffer.seek(0)
    blob.upload_from_file(temp_buffer, content_type="text/csv")
