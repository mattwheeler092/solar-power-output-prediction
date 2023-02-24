import json

from datetime import datetime, timedelta
from gcp_bucket import GCP_Bucket
from user_definision import (
    CACHE_FILE_NAME, 
    END_DATE, 
    START_DATE, 
    DATE_FORMAT
)
from utils import increment_date

class LocationCache:
    """Class to load and update location cache that keeps
    track of which latitude / longitude coordinates have
    had weather information collected. Holds information
    of the most recent data collection date"""

    def __init__(self):
        """ """
        # Initialise GCP bucket class
        self.bucket = GCP_Bucket()

        # If cache file exists load from GCP
        if self.bucket.check_file_exists(CACHE_FILE_NAME):
            self.cache = self.bucket.load_file(CACHE_FILE_NAME)

        else:  # If cache doesn't exist upload empty json to GCP
            self.cache = {}
            self.bucket.upload_file(json.dumps(self.cache), CACHE_FILE_NAME)

    def gen_cache_key(self, lat, lon):
        """ Function to produce cache dict key from lat / lon"""
        return f"({lat}:{lon})"

    def collection_complete(self, lat, lon):
        """Function to check if all data has been collected
        for a specific lat / lon location. Checks if the most 
        recent collection date is equal to the end date """
        key = self.gen_cache_key(lat, lon)
        return self.cache.get(key, START_DATE) == END_DATE

    def get_next_collection_dates(self, lat, lon):
        """ Function to get the start / end dates of the next """
        key = self.gen_cache_key(lat, lon)
        last_date = self.cache.get(key)
        if last_date is None:
            start = START_DATE
        else:
            start = increment_date(last_date, num_days=1)
        end = self.increment_date(start)
        return start, end

    def update_cache(self, lat, lon, date):
        """Function to update the cache to the most recent
        collection date for a given lat / lon location.
        Function also uploads file to GCP"""
        key = self.gen_cache_key(lat, lon)
        self.cache[key] = date
        self.bucket.upload_file(
            json.dumps(self.cache), 
            CACHE_FILE_NAME, 
            overwrite_file=True
        )
