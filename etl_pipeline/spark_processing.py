import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime

from mongo_db import MongoDB
from config import (
    WEATHER_DATA_SCHEMA,
    GS_SERVICE_KEY, 
    DROP_COLUMNS,
    COLUMN_RENAME_DICT,
    GS_BUCKET
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


