import json
import os

from google.cloud import storage
from user_definision import GS_BUCKET, GS_SERVICE_KEY


class GCP_Bucket:
    """ Class to handle all connections to the project GCP 
        bucket. Provides functionality to check if a file
        exists plus load / upload files """

    def __init__(self):
        """Function to initialise GCP bucket class"""
        self.client = storage.Client.from_service_account_json(GS_SERVICE_KEY)
        self.bucket = self.client.get_bucket(GS_BUCKET)

    def check_file_exists(self, file_name):
        """Function to check if a file exists
        within the GCP bucket"""
        blob = self.bucket.blob(file_name)
        return blob.exists()

    def load_file(self, file_name):
        """Function to load a JSON file from GCP"""
        blob = self.bucket.blob(file_name)
        json_str = blob.download_as_string()
        return json.loads(json_str)

    def upload_file(self, data, file_name, overwrite_file=False):
        """Function to upload a JSON file to GCP. The
        'overwrite_file' flag specifies if the upload
        is able to overwrite existing GCP files with
        the same file_name"""
        if overwrite_file:
            params = {"if_generation_match": None}
        else:
            params = {"if_generation_not_match": None}
        blob = self.bucket.blob(file_name)
        blob.upload_from_string(
            data=json.dumps(data), 
            content_type="application/json", 
            **params
        )
