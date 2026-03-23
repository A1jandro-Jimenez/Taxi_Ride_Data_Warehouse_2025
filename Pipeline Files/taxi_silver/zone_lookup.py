from pyspark import pipelines as dp
from pyspark.sql import functions as F



@dp.materialized_view(
    name="taxirides2025.silver.zone",
    comment="CLeaned and standardized taxi zone data",
    table_properties= { "quality" : "silver", 
        "layer": "silver", 
        "delta.enableChangeDataFeed": "true", 
        "delta.autoOptimize.optimizeWrite": "true", 
        "delta.autoOptimize.autoCompact": "true"
        }
        
    )
def zone_silver():
    df_bronze = spark.read.table("taxirides2025.bronze.zone")
    df_silver = df_bronze.select(
        F.col("LocationID").alias("LocationID"),
        F.col("Borough").alias("Borough"),
        F.col("Zone").alias("Zone"),
        F.col("service_zone").alias("service_zone"),
        F.col("ingest_timestamp").alias("bronze_ingest_timestamp")
    )

    df_silver = df_silver.withColumn(
        "silver_ingest_timestamp", F.current_timestamp()
        )
    
    return df_silver

    

