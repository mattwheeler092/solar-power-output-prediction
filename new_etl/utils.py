import os
import calendar
import pandas as pd
import time

# from airflow.models import DagRun, DagModel
# from airflow import settings
from airflow.api.client.local_client import Client
from google.api_core.exceptions import TooManyRequests
from datetime import datetime, timedelta
from config import (
    DATE_FORMAT, 
    WEATHER_API_KEY, 
    LOCATION_DATA_FILE_NAME,
    GCP_DATA_FOLDER,
    AIRFLOW_DAG_ID
)


def kill_airflow_job():
    """ Function to perminently kill the pipelines airflow 
    job. To be used once all data has been collected """
    # Define client and loop through each dag run
    client = Client(None, None)
    for dag_run in client.get_dag_runs(AIRFLOW_DAG_ID):
        # kill all running dags with project dag id
        if dag_run.state == 'running':
            client.trigger_dag(
                dag_id=AIRFLOW_DAG_ID, 
                run_id=dag_run.run_id, 
                conf={'kill_signal': 'SIGINT'}
            )


# def kill_airflow_job():
#     """ Function to perminently kill the pipelines airflow 
#     job. To be used once all data has been collected """
#     # Get the DAG object by its dag_id
#     dag = DagModel.get_dagmodel(AIRFLOW_DAG_ID)
#     # Set the end_date of the DagRun to stop the DAG
#     for dag_run in DagRun.find(dag_id=AIRFLOW_DAG_ID):
#         dag_run.end_date = datetime.now()
#         dag_run.state = "failed"
#         dag_run.external_trigger = False
#         session = settings.Session()
#         session.merge(dag_run)
#         session.commit()


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
    """ Function to combine any list fields within the data 
    dict into a concat str with '|' seperator """
    for key, value in data.items():
        if isinstance(value, list):
            data[key] = '|'.join([str(v) for v in value])
    return data


def generate_api_query(lat, lon, start, end):
    """ Function to generate the api query for a specific 
    lat / lon position and a specific start / end 
    date period. """
    return f"""https://weather.visualcrossing.com/VisualCrossingWebServices
/rest/services/timeline/{lat}%2C%20{lon}/{start}/{end}?unitGroup=metric&
key={WEATHER_API_KEY}&contentType=json""".replace("\n", "")


class FailedApiRequest(Exception):
    """ Class to capture cases when API requests to 
    the Visual Crossing weather service fail """


class ApiRequestTimeoutError(Exception):
    """ Class to capture cases when API requests to 
    the Visual Crossing weather service fail """


class FileTypeError(Exception):
    """ """


class BaseTimeoutDecoratorClass:
    """ """
    def __init__(self, retries, wait, error):
        """ """
        self.retries = retries
        self.wait = wait
        self.error = error

    def __call__(self, func):
        """ """
        def wrapped(*args, **kwargs):
            for i in range(self.retries + 1):
                try:
                    result = func(*args, **kwargs)
                    return result
                except self.error as err:
                    time.sleep(self.wait)
                    if i == self.retries:
                        raise err
        return wrapped


class RetryOnGcpTimeoutError(BaseTimeoutDecoratorClass):
    """ """
    def __init__(self, retries, wait):
        super().__init__(retries, wait, TooManyRequests)


class RetryOnApiTimeoutError(BaseTimeoutDecoratorClass):
    """ """
    def __init__(self, retries, wait):
        super().__init__(retries, wait, ApiRequestTimeoutError)

