from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, lit, sha2


# Configuration
SOURCE_PATH = "s3://taxirides-ale21/Taxi_Rides_Datasets/zone_lookup_table"


@dp.materialized_view(
    name = "taxirides2025.bronze.zone",
    comment = "Zone Raw Data Processing",
    table_properties = {
        "quality" : "bronze",
        "layer" : "bronze",
        "source_format" : "csv",
        "delta.enableChangeDataFeed" : "true",
        "delta.autoOptimize.optimizeWrite" : "true",
        "delta.autoOptimize.autoCompact" : "true"
    }      
)


def zone_bronze():
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(SOURCE_PATH)
    )

    df = (
        df.withColumn("file_name", col("_metadata.file_path"))
          .withColumn("ingest_timestamp", current_timestamp())
    )

    return df




