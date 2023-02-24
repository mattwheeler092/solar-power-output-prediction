import requests
from datetime import datetime as dt
import pandas as pd
from google.cloud import storage
import io
import json


def fetch_openweather_locations(locations, api_key):
    """Fetch location information from Open Weather given a list of (lat, lon) pairs."""
    result = []
    errors = []
    for lat, lon in locations:
        try:
            endpoint = f"https://api.openweathermap.org/geo/1.0/reverse?lat={lat}&lon={lon}&appid={api_key}"
            response = requests.get(endpoint)
            res = response.json()[0]
            data = {"lat": lat, "lon": lon, 
                    "name": res.get('name', "UNK"),
                    "country": res.get('country', "UNK"),
                    "state": res.get('state', "UNK")}
            result.append(data)
        except:
            errors.append((lat, lon))
    return pd.DataFrame(result), errors
