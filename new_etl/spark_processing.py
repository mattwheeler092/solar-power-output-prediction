import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime

from mongo_db import MongoDB
from config import GS_SERVICE_KEY, GS_BUCKET

from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    IntegerType, 
    DoubleType
)

def create_spark_df_from_gcp_file(gcp_filename, spark_session):
    """ Function to load csv file from GCP into spark """
    df = (spark_session.read.format("csv")
          .option("header", "true")
          .schema(WEATHER_DATA_SCHEMA)
          .load(os.path.join(f'gs://{GS_BUCKET}', gcp_filename)))
    return df


def insert_spark_data_to_mongo(spark_df):
    """ Function to insert each spark 
    dataframe rows into mongo database """
    mongo = MongoDB()
    records = []
    for row in spark_df.collect():
        row = row.asDict()
        date = row['date']
        time = row['time']
        lat = row['lat']
        lon = row['lon']
        id_str = f'{lat}_{lon}_{date}_{time}'
        row['_id'] = id_str
        records.append(row)
    mongo.insert(records)


def process_spark_df(spark_df):
    """ Function to drop / rename specified 
    column within provided spark dataframe """
    df = drop_columns(spark_df)
    df = rename_columns(spark_df)
    return df


def create_spark_session():
    """ Function to initialse a spark session and to configure 
    that spark session to access the projects GCP bucket """
    # Initialise the spark session and add GCP bucket config
    spark = SparkSession.builder.config(
        "spark.jars","gcs-connector-hadoop1-latest.jar"
    ).getOrCreate()
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


def drop_columns(spark_df):
    """Drop specified columns of the given Spark dataframe."""
    drop_cols = [col for col in DROP_COLUMNS if col in spark_df.columns]
    spark_df = spark_df.drop(*drop_cols)
    return spark_df


def rename_columns(spark_df):
    """Rename columns of the given Spark dataframe."""
    for k, v in COLUMN_RENAME_DICT.items():
        if k in spark_df.columns:
            spark_df = spark_df.withColumnRenamed(k, v)
    return spark_df


# Define columns to be droppes
DROP_COLUMNS = ["timezone", "tzoffset", "icon", "stations"]

# Define columns to be renames
COLUMN_RENAME_DICT = {
    "cloudcover": "cloud_cover_perc",
    "uvindex": "uv_index",
    "solarradiation": "solar_radiation",
    "solarenergy": "solar_energy",
}
# Define the schema for each data field
WEATHER_DATA_SCHEMA = StructType([
    StructField('datetimeEpoch', IntegerType(), False),
    StructField('temp', DoubleType(), False),
    StructField('feelslike', DoubleType(), False),
    StructField('humidity', DoubleType(), False),
    StructField('dew', DoubleType(), False),
    StructField('precip', DoubleType(), False),
    StructField('precipprob', DoubleType(), False),
    StructField('snow', DoubleType(), False),
    StructField('snowdepth', DoubleType(), False),
    StructField('preciptype', StringType(), False),
    StructField('windgust', DoubleType(), False),
    StructField('windspeed', DoubleType(), False),
    StructField('winddir', DoubleType(), False),
    StructField('pressure', DoubleType(), False),
    StructField('visibility', DoubleType(), False),
    StructField('cloudcover', DoubleType(), False),
    StructField('solarradiation', DoubleType(), False),
    StructField('solarenergy', DoubleType(), False),
    StructField('uvindex', DoubleType(), False),
    StructField('severerisk', DoubleType(), False),
    StructField('conditions', StringType(), False),
    StructField('icon', StringType(), False),
    StructField('stations', StringType(), False),
    StructField('source', StringType(), False),
    StructField('time', StringType(), False),
    StructField('lat', DoubleType(), False),
    StructField('lon', DoubleType(), False),
    StructField('timezone', StringType(), False),
    StructField('tzoffset', DoubleType(), False),
    StructField('date', StringType(), False),
    StructField('day_agg_tempmax', DoubleType(), False),
    StructField('day_agg_tempmin', DoubleType(), False),
    StructField('day_agg_temp', DoubleType(), False),
    StructField('day_agg_feelslikemax', DoubleType(), False),
    StructField('day_agg_feelslikemin', DoubleType(), False),
    StructField('day_agg_feelslike', DoubleType(), False),
    StructField('day_agg_dew', DoubleType(), False),
    StructField('day_agg_humidity', DoubleType(), False),
    StructField('day_agg_precip', DoubleType(), False),
    StructField('day_agg_precipprob', DoubleType(), False),
    StructField('day_agg_precipcover', DoubleType(), False),
    StructField('day_agg_preciptype', StringType(), False),
    StructField('day_agg_snow', DoubleType(), False),
    StructField('day_agg_snowdepth', DoubleType(), False),
    StructField('day_agg_windgust', DoubleType(), False),
    StructField('day_agg_windspeed', DoubleType(), False),
    StructField('day_agg_winddir', DoubleType(), False),
    StructField('day_agg_pressure', DoubleType(), False),
    StructField('day_agg_cloudcover', DoubleType(), False),
    StructField('day_agg_visibility', DoubleType(), False),
    StructField('day_agg_uvindex', DoubleType(), False),
    StructField('day_agg_severerisk', DoubleType(), False),
    StructField('day_agg_sunrise', StringType(), False),
    StructField('day_agg_sunset', StringType(), False),
    StructField('day_agg_moonphase', DoubleType(), False),
    StructField('day_agg_conditions', StringType(), False),
    StructField('day_agg_description', StringType(), False),
    StructField('day_agg_source', StringType(), False),
])
