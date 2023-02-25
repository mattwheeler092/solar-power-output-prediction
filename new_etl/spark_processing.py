import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime

from config import GS_SERVICE_KEY, GS_BUCKET

def create_spark_session():
    """ Function to initialse a spark session and to configure 
    that spark session to access the projects GCP bucket """
    # Initialise the spark session and add GCP bucket config
    spark = SparkSession.builder.getOrCreate()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set(
        "google.cloud.auth.service.account.json.keyfile", GS_SERVICE_KEY
    )
    conf.set(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    conf.set(
        "fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )  
    return spark


def read_file_from_gcp(spark_session, gcp_filename):
    """ Function to load csv file from GCP into spark """
    df = (spark_session.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(os.path.join(f'gs://{GS_BUCKET}', gcp_filename)))
    return df


# def convert_datetime(df, column_name="datetimeEpoch"):
#     """Convert unix timestamps to Pacific Time"""
#     df = df.withColumn(column_name, from_unixtime(column_name).cast("timestamp"))
#     return df


# def drop_columns(df, columns=DROP_COLUMNS):
#     """Drop specified columns of the given Spark dataframe."""
#     drop_cols = [col for col in columns if col in df.columns]
#     df = df.drop(*drop_cols)
#     return df


# def rename_columns(df, rename_dict=COLUMN_RENAME_DICT):
#     """Rename columns of the given Spark dataframe."""
#     for k, v in rename_dict.items():
#         df = df.withColumnRenamed(k, v)
#     return df


# ['datetimeEpoch',
#  'temp',
#  'feelslike',
#  'humidity',
#  'dew',
#  'precip',
#  'precipprob',
#  'snow',
#  'snowdepth',
#  'preciptype',
#  'windgust',
#  'windspeed',
#  'winddir',
#  'pressure',
#  'visibility',
#  'cloudcover',
#  'solarradiation',
#  'solarenergy',
#  'uvindex',
#  'severerisk',
#  'conditions',
#  'icon',
#  'stations',
#  'source',
#  'lat',
#  'lon',
#  'timezone',
#  'tzoffset',
#  'day_agg_tempmax',
#  'day_agg_tempmin',
#  'day_agg_temp',
#  'day_agg_feelslikemax',
#  'day_agg_feelslikemin',
#  'day_agg_feelslike',
#  'day_agg_dew',
#  'day_agg_humidity',
#  'day_agg_precip',
#  'day_agg_precipprob',
#  'day_agg_precipcover',
#  'day_agg_preciptype',
#  'day_agg_snow',
#  'day_agg_snowdepth',
#  'day_agg_windgust',
#  'day_agg_windspeed',
#  'day_agg_winddir',
#  'day_agg_pressure',
#  'day_agg_cloudcover',
#  'day_agg_visibility',
#  'day_agg_uvindex',
#  'day_agg_severerisk',
#  'day_agg_sunrise',
#  'day_agg_sunset',
#  'day_agg_moonphase',
#  'day_agg_conditions',
#  'day_agg_description',
#  'day_agg_source',
#  'time',
#  'date']
