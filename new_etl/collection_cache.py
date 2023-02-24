import json

from gcp_bucket import GCP_Bucket
from utils import increment_date, month_end_date, min_date
from user_definision import (
    COLLECTION_CACHE_FILE_NAME, 
    NUM_MONTHS_TO_PROCESS_PER_JOB,
    END_DATE, 
    START_DATE
)

class CollectionCache:
    """ Class to load and update collection cache that keeps
        track of which latitude / longitude coordinates have
        had weather information collected. Holds information
        of the most recent data collection date """

    def __init__(self):
        """ """
        # Initialise GCP bucket class
        self.bucket = GCP_Bucket()
        # If cache file exists load from GCP
        if self.bucket.check_file_exists(COLLECTION_CACHE_FILE_NAME):
            self.cache = self.bucket.load_file(COLLECTION_CACHE_FILE_NAME)
        # If cache doesn't exist upload empty json to GCP
        else:  
            self.cache = {}
            self.update_cache()

    def interate_cache(self, coordinates):
        """ """
        processed_months = 0
        # Loop through each of the provided coordinates
        for lat, lon in coordinates:
            # Yield start / end dates until collection complete for lat / lon
            while not self.collection_complete(lat, lon):
                start, end = self.get_next_collection_dates(lat, lon)
                yield lat, lon, start, end
                # Update the cache to reflect the newest collection date
                self.update_cache(lat, lon, end)
                # Increment processed month count end if limit reached
                processed_months += 1
                if processed_months == NUM_MONTHS_TO_PROCESS_PER_JOB:
                    return

    def gen_cache_key(self, lat, lon):
        """ Function to produce cache dict key from lat / lon"""
        return f"({lat}:{lon})"

    def collection_complete(self, lat, lon):
        """ Function to check if all data has been collected
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
        end = min_date(month_end_date(start), END_DATE)
        return start, end
    
    def update_collection_date(self, lat, lon, date):
        """ Function to update the cache to the most recent
        collection date for a given lat / lon location.
        Function also uploads file to GCP """
        key = self.gen_cache_key(lat, lon)
        self.cache[key] = date
        self.update_cache()

    def update_cache(self, lat, lon, date):
        """ Function to upload the newly updated 
        cache to GCP """
        self.bucket.upload_file(
            self.cache, 
            COLLECTION_CACHE_FILE_NAME, 
            file_type='json',
            overwrite=True
        )
