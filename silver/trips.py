# import libraries
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date
from pyspark.sql.functions import date_format
from pyspark.sql.functions import expr
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql.functions import col
from pyspark.sql.functions import hour
# define expectations
cols = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance_miles",
    "RatecodeID",
    "store_and_fwd_flag", 
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
    "file_name",
    "ingest_timestamp",
    "pickup_date",
    "DateID",
    "trip_duration_minutes",
    "silver_processed_timestamp"
]

condition = " AND ".join([f"{c} IS NOT NULL" for c in cols])

valid_cols = {
    "fare_amount_non_negative":"fare_amount >= 0",
              "extra_non_negative":"extra >= 0",
              "mta_tax_non_negative":"mta_tax >= 0",
              "tip_amount_non_negative":"tip_amount >= 0",
              "tolls_amount_non_negative" :"tolls_amount >= 0",
              "improvement_surcharge_non_negative":"improvement_surcharge >= 0",
              "congestion_surcharge_non_negative":"congestion_surcharge >= 0",
              "Airport_fee_non_negative":"Airport_fee >= 0",
              "cbd_congestion_fee_non_negative":"cbd_congestion_fee >= 0",
              "passenger_count_non_zero":"passenger_count > 0",
             
}

valid_ids = {
    "RatecodeID_valid":"RatecodeID != 99",
    "RatecodeID_valid2":"RatecodeID != 0",
    "payment_type_valid":"payment_type != 6",
    "payment_type_valid2":"payment_type != 5",
}



# create staging view 
@dp.view(
    name = "trips_silver_staging",
    comment = "Transformed trips data ready for CDC upsert"
)
@dp.expect_or_drop("no_nulls_except_rescued", condition)
@dp.expect_or_drop("non-zero_postive_distances","trip_distance_miles > 0")
@dp.expect_or_drop("total_amount_non-negative","total_amount >= 0")
@dp.expect_all_or_drop(valid_cols)
@dp.expect_all_or_drop(valid_ids)
@dp.expect_or_drop(
    "valid_2025_pickup",
    "tpep_pickup_datetime >= '2025-01-01' AND tpep_pickup_datetime < '2026-01-01'"
)
@dp.expect_or_drop(
    "valid_2025_dropoff",
    "tpep_dropoff_datetime >= '2025-01-01' AND tpep_dropoff_datetime < '2026-01-01'"
)



#function to do transformaion
def trips_silver():
    df_bronze = spark.readStream.table("taxirides2025.bronze.trips")

    df_silver = df_bronze.withColumn(
        "tpep_pickup_datetime",
        col("tpep_pickup_datetime").cast("timestamp")
        ).withColumn(
        "tpep_dropoff_datetime",
        col("tpep_dropoff_datetime").cast("timestamp")
        )
    
    df_silver = df_bronze.withColumn("pickup_date", to_date(F.col("tpep_pickup_datetime"))) 
    df_silver = df_silver.withColumn("DateID",F.date_format(F.col("pickup_date"),"yyyyMMdd").cast("int"))
    
    df_silver = df_silver.withColumn(
    "trip_duration_minutes",
    expr("timestampdiff(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)")
    
    )


    df_silver = df_silver.withColumn(
        "hour_of_day", hour("tpep_pickup_datetime")
        )


    df_silver = df_silver.withColumn(
        "TripID",
        sha2(concat_ws("||",
        col("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime"),
        col("PULocationID"),
        col("DOLocationID")
        ), 256)
    )
 



    df_silver = df_silver.withColumn(
        "silver_processed_timestamp",F.current_timestamp(),
    )

   

    return df_silver



#creat silver table as streaming table
dp.create_streaming_table(
    name = "taxirides2025.silver.trips",
    comment = "Cleaned and validated trips with CDC upsert capabilities",
    table_properties = {
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed" : "true",
        "delta.autoOptimize.optimizeWrite" : "true",
        "delta.autoOptimize.autoCompact" : "true",
        "delta.feature.timestampNtz": "supported" ,

    }
    
)



# create auto CDC flow to get staging to silver table
dp.create_auto_cdc_flow(
    target = "taxirides2025.silver.trips",
    source = "trips_silver_staging",
    keys = ["TripID"],
    sequence_by=F.col ("silver_processed_timestamp"),
    stored_as_scd_type = 1
)
    