from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, lit, sha2


# Configuration
SOURCE_PATH = "s3://taxirides-ale21/Taxi_Rides_Datasets/Trips_Data"

@dp.table(
    name = "taxirides2025.bronze.trips",
    comment = "Streaming ingestion of raw trips data with Auto Loader",
    table_properties = {
        "quality" : "bronze",
        "layer" : "bronze",
        "source_format" : "parquet",
        "delta.enableChangeDataFeed" : "true",
        "delta.autoOptimize.optimizeWrite" : "true",
        "delta.autoOptimize.autoCompact" : "true",
        "delta.feature.timestampNtz": "supported" 
    }      
)


def trips_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudfiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger","100")
        .load(SOURCE_PATH)
    )

    #rename columns 
    df = df.withColumnRenamed(
        "trip_distance",
        "trip_distance_miles"
    )

    df = (
        df.withColumn("file_name", col("_metadata.file_path"))
          .withColumn("ingest_timestamp", current_timestamp())
    )

    return df

    

