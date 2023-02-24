import pandas as pd
import requests

from utils import flatten_json, generate_api_query


class FailedApiRequest(Exception):
    """ Class to capture cases when API requests to 
    the Visual Crossing weather service fail """


def collect_api_data(lat, lon, start, end):
    """ Function to make API requestion for specific 
    lat /lon location and specific date range. Also 
    process API response and returns a pandas dataframe """
    # Generate API query and make request
    api_query = generate_api_query(lat, lon, start, end)
    response = requests.get(api_query)
    # Raise error if API request fails
    if response.status_code != 200:
        raise FailedApiRequest
    # Else extract, process, and return API data
    data = response.json()
    return process_response_json(data)


def process_response_json(json):
    """ Function to process the JSON response data 
    from a visual crossing API call """
    result = []
    # Extract high level location stats
    location_stats = extract_location_stats(json)
    # Loop through each day and extract day stats
    for day_json in json["days"]:
        day_stats = extract_day_stats(day_json)
        # Loop through each day and extract day stats
        for hour_json in day_json["hours"]:
            hour_json = extract_hour_stats(hour_json)
            # Combine day and location stats and append result
            hour_json.update(location_stats)
            hour_json.update(day_stats)
            result.append(hour_json)
    # Return formatted 
    return pd.DataFrame(result)


def extract_location_stats(data):
    """ Function to extract location level 
    fields from API response """
    return flatten_json({
        'lat': data['latitude'], 
        'lon': data['longitude'],
        'timezone': data['timezone'],
        'tzoffset': data['tzoffset']
    })

def extract_day_stats(data):
    """ Function to pull relevant day level 
    fields from data """
    # Remove unwanted fields
    day = data.copy()
    del day['sunriseEpoch']
    del day['sunsetEpoch']
    del day['icon']
    del day['solarradiation']
    del day['solarenergy']
    del day['hours']
    del day['stations']
    del day['datetimeEpoch']
    # Loop through remaining day fields
    for key in list(day.keys()):
        # Set None precipitation type to mean no rain
        if "preciptype" in key and day[key] is None:
            day[key] = 'No rain'
        # Rename datetime field to date
        if key == "datetime":
            day['date'] = day.pop(key) 
        # Add 'day_agg_' prefix to all otherkey names
        else:
            day[f'day_agg_{key}'] = day.pop(key)
    return flatten_json(day)


def extract_hour_stats(data):
    """ Function to pull relevant hour level 
    fields from data"""
    # Loop through remaining day fields
    for key in list(data.keys()):
        # Set None precipitation type to mean no rain
        if "preciptype" in key and data[key] is None:
            data[key] = 'No rain'
        # Rename datetime field to date
        elif key == "datetime":
            data['time'] = data.pop(key) 
    return flatten_json(data)
