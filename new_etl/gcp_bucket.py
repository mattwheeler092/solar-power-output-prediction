import json
import io
import pandas as pd

from google.cloud import storage
from user_definision import GS_BUCKET, GS_SERVICE_KEY


class FileTypeError(Exception):
    pass


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

    def load_file(self, file_name, file_type):
        """Function to load either a JSON or CSV file to 
        GCP. Returns dict for JSON files and pd.DataFrame 
        for CSV files """
        # Load file string from GCP bucket
        blob = self.bucket.blob(file_name)
        file_str = blob.download_as_string()
        # Construct dict or pd.DataFrame from file str
        if file_type == 'json':
            return json.loads(file_str)
        elif file_type == 'csv':
            return pd.read_csv(io.StringIO(file_str))
        else:
            msg = "'file_type' != 'csv' or 'json"
            raise FileTypeError(msg)

    def upload_file(self, data, file_name, file_type, overwrite=False):
        """Function to upload either a JSON or CSV file to 
        GCP. The 'overwrite' flag specifies if the upload
        is able to overwrite existing GCP files with the 
        same file_name """
        # Set the upload params to either enable or diable overwiting
        if overwrite:
            params = {"if_generation_match": None}
        else:
            params = {"if_generation_not_match": None}
        # Convert the input data to string format
        if file_type == 'json':
            data = json.dumps(data)
            content_type = "application/json"
        elif file_type == 'csv':
            data = data.to_csv(index=False)
            content_type = "text/csv"
        else:
            msg = "'file_type' != 'csv' or 'json"
            raise FileTypeError(msg)
        # Initialise blob obj and upload the data to GCP
        blob = self.bucket.blob(file_name)
        blob.upload_from_string(
            data, content_type=content_type, **params
        )
