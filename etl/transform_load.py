import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime
from google.cloud import storage
from mongodb import *
from dotenv import load_dotenv
from pyspark.sql.functions import col
import os
from user_definition import *


def read_weather_history_data(
    spark, folder="", path="gs://msds697-solar", file_name_regex=VC_DATA_FILENAME_REGEX
):
    """Read weather history CSV from GCS into spark"""
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(os.path.join(path, folder, file_name_regex))
    )
    return df


def convert_datetime(df, column_name="datetimeEpoch"):
    """Convert unix timestamps to Pacific Time"""
    df = df.withColumn(column_name, from_unixtime(column_name).cast("timestamp"))
    return df


def drop_columns(df, columns=DROP_COLUMNS):
    """Drop specified columns of the given Spark dataframe."""
    drop_cols = [col for col in columns if col in df.columns]
    df = df.drop(*drop_cols)
    return df


def rename_columns(df, rename_dict=COLUMN_RENAME_DICT):
    """Rename columns of the given Spark dataframe."""
    for k, v in rename_dict.items():
        df = df.withColumnRenamed(k, v)
    return df


def read_location_data(spark, path="gs://msds697-solar"):
    """Read Open Weather location data from GCS into a Spark dataframe."""
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(os.path.join(path, "OW_coordinate_location.csv"))
    )
    return df


def transform_load_to_mongo(folder=""):
    """Main function for transforming the data and loading the resulting aggregates to MongoDB."""
    spark = SparkSession.builder.getOrCreate()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set(
        "google.cloud.auth.service.account.json.keyfile", GS_SERVICE_ACCOUNT_KEY_FILE
    )
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set(
        "fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )

    # transform
    weather_df = read_weather_history_data(spark, folder)
    weather_df = convert_datetime(weather_df)
    weather_df = drop_columns(weather_df, columns=["weather_id", "weather_icon"])
    weather_df = rename_columns(
        weather_df,
        COLUMN_RENAME_DICT,
    )
    location_df = read_location_data(spark)

    # join weather_df with location_df
    final_df = weather_df.join(location_df, ["lat", "lon"], "left")

    # load to mongodb
    mongodb = MongoDBCollection(
        MONGO_USERNAME, MONGO_PASSWORD, MONGO_IP, MONGO_DB_NAME, MONGO_COLLECTION_NAME
    )

    # convert each row of the dataframe to a dictionary and then insert into mongodb
    documents = [r.asDict() for r in final_df.select("*").collect()]
    mongodb.insert_many(documents)
    print("MongoDB insertion completed!")


if __name__ == "__main__":
    transform_load_to_mongo()
