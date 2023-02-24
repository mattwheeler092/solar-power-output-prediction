import json

from gcp_bucket import GCP_Bucket
from user_definision import SPARK_CACHE_FILE_NAME

class SparkCache:
    """ Class to load and update the spark cache which 
        keeps track of which GCP weather data files need 
        to be processed by PySpark """

    def __init__(self):
        """ Initialiser loads spark cache file from GCP if it 
        exists. If not then it creates an empty cache that 
        is then uploaded to GCP """
        # Initialise GCP bucket class
        self.bucket = GCP_Bucket()
        # If cache file exists load from GCP
        if self.bucket.check_file_exists(SPARK_CACHE_FILE_NAME):
            self.cache = self.bucket.load_file(SPARK_CACHE_FILE_NAME)
        # If cache doesn't exist upload empty json to GCP
        else:  
            self.cache = {"files": []}
            self.update_cache()

    def list_cached_files(self):
        """ Generator function that lists the current files 
        to be processed by spark. Once the file has been 
        processed it is removed from the cache and the 
        updated cache is uploaded to GCP """
        file_names = self.cache["files"].copy()
        # Yield each file in the cache
        for file_name in file_names:
            yield file_name
            # Remove file from cache once processed
            self.remove_file(file_name)

    def add_file(self, file_name):
        """ Function to add a new file to the cached 
        file list. Once added, the updated cache is 
        uploaded to GCP """
        # Add file to cached file list + update cache
        self.cache["files"].append(file_name)
        self.update_cache()

    def remove_file(self, file_name):
        """ Function to remove file from cached 
        file list. Once removed, the updated cache 
        is uploaded to GCP """
        idx = self.cache["files"].index(file_name)
        self.cache["files"].pop(idx)
        self.update_cache()

    def update_cache(self):
        """ Function to upload the newly updated 
        cache to GCP """
        self.bucket.upload_file(
            self.cache, 
            SPARK_CACHE_FILE_NAME, 
            file_type='json',
            overwrite=True
        )
