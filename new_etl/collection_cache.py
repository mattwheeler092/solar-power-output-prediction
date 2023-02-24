from gcp_bucket import GCP_Bucket
from utils import increment_date, month_end_date, min_date
from config import (
    COLLECTION_CACHE_FILE_NAME, 
    NUM_MONTHS_TO_PROCESS_PER_JOB,
    END_DATE, 
    START_DATE
)

class CollectionCache:
    """ Class to load and update data collection cache that keeps
        track of which latitude / longitude locations have
        had weather information collected. Holds information
        of the most recent data collection date """

    def __init__(self):
        """ Initialiser loads collection cache file from GCP if 
        it exists. If not then it creates an empty cache that 
        is then uploaded to GCP """
        # Initialise GCP bucket object
        self.bucket = GCP_Bucket()
        # If cache file exists load from GCP
        if self.bucket.check_file_exists(COLLECTION_CACHE_FILE_NAME):
            self.cache = self.bucket.load_file(COLLECTION_CACHE_FILE_NAME)
        # If cache doesn't exist upload empty json to GCP
        else:  
            self.cache = {}
            self.update_cache()


    def generator(self, locations):
        """ Generator that yields (lat, lon, start, end) combinations. 
        Once a combination has been successfully yielded, it updates 
        the cache which is then uploaded to GCP. Function to limited 
        to return a maximum of 'NUM_MONTHS_TO_PROCESS_PER_JOB' 
        combinations. """
        months_processed = 0
        # Loop through each of the provided locations
        for lat, lon in locations:
            # Yield start / end dates until collection is complete
            while not self.collection_complete(lat, lon):
                start, end = self.next_collection_dates(lat, lon)
                yield lat, lon, start, end
                # Update cache with newest collection date
                self.update_collection_date(lat, lon, end)
                # Increment processed count end if limit reached
                months_processed += 1
                if months_processed == NUM_MONTHS_TO_PROCESS_PER_JOB:
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
    

    def full_collection_complete(self, locations):
        """ Function to check if full collection process is 
        complete. Loops through each location and see's if 
        the cached collection date is equal to the end date """
        for lat, lon in locations:
            key = self.gen_cache_key(lat, lon)
            if self.cache.get(key, START_DATE) != END_DATE:
                return False
        return True


    def next_collection_dates(self, lat, lon):
        """ Function to get the start / end dates of the next 
        data collection period for a specified lat / lon location. 
        'start' will always be the start of a month and 'end' 
        with be the end of the same month """
        # Extract the most recent collection date
        key = self.gen_cache_key(lat, lon)
        latest_date = self.cache.get(key)
        # If it doesn't exist set to start date
        if latest_date is None:
            start = START_DATE
        # Else set to start to one day after latest
        else:
            start = increment_date(latest_date, num_days=1)
        # Set end date to be min of end_date or month end date
        end = min_date(month_end_date(start), END_DATE)
        return start, end
    

    def update_collection_date(self, lat, lon, date):
        """ Function to update the cache to the most recent
        collection date for a given lat / lon location.
        Function also uploads file to GCP """
        key = self.gen_cache_key(lat, lon)
        self.cache[key] = date
        self.update_cache()


    def update_cache(self):
        """ Function to upload the newly updated 
        cache to GCP """
        self.bucket.upload_file(
            self.cache, 
            COLLECTION_CACHE_FILE_NAME, 
            file_type='json',
            overwrite=True
        )
