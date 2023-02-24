import os
import calendar
import pandas as pd

from datetime import datetime, timedelta
from config import (
    DATE_FORMAT, 
    WEATHER_API_KEY, 
    LOCATION_DATA_FILE_NAME,
    GCP_DATA_FOLDER
)

def load_location_data():
    """ Function to lat / lon values from locations csv """
    locations_df = pd.read_csv(LOCATION_DATA_FILE_NAME)
    return locations_df[['lat', 'lon']].values

def generate_gcp_filename(lat, lon, start):
    """ Function to construct the GCP file name 
    for a given lat /lon location and date """
    date = datetime.strptime(start, DATE_FORMAT)
    month = str(date.month).rjust(2, '0')
    year = str(date.year)
    file_name = f'vc_{lat}_{lon}_{year}_{month}.csv'
    return os.path.join(GCP_DATA_FOLDER, file_name)


def increment_date(date, num_days):
    """ Function to increment the provided str date 
        by a specified number of days. Returns date 
        in str format """
    date = datetime.strptime(date, DATE_FORMAT)
    incremented_date = date + timedelta(days=num_days)
    return incremented_date.strftime(DATE_FORMAT)


def month_end_date(date):
    """ Function to find the end of month date where the 
        month in question is the month of the input date """
    date = datetime.strptime(date, DATE_FORMAT)
    # Define year / month / day for month end 
    year = date.year
    month = date.month
    day = calendar.monthrange(year, month)[1]
    # Return month end date in str format
    return datetime(year, month, day).strftime(DATE_FORMAT)


def min_date(date1, date2):
    """ Function to determine which str date 
        is the earlier date """
    datetime1 = datetime.strptime(date1, DATE_FORMAT)
    datetime2 = datetime.strptime(date2, DATE_FORMAT)
    if datetime1 <= datetime2:
        return date1
    else:
        return date2
    

def flatten_json(data):
    """ Function to combine any list fields 
        within the data dict into a concat str"""
    for key, value in data.items():
        if isinstance(value, list):
            data[key] = ','.join([str(v) for v in value])
    return data


def generate_api_query(lat, lon, start, end):
    """ Function to generate the api query for a specific 
        lat / lon position and a specific start / end 
        date period. """
    return f"""https://weather.visualcrossing.com/VisualCrossingWebServices
/rest/services/timeline/{lat}%2C%20{lon}/{start}/{end}?unitGroup=metric&
key={WEATHER_API_KEY}&contentType=json""".replace("\n", "")


