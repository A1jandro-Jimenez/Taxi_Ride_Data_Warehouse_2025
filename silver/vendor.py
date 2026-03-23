from pyspark import pipelines as dp
from pyspark.sql import functions as F




@dp.materialized_view(
    name = "taxirides2025.silver.vendor",
    comment = "vendor dimension with data attributes (2025)",
    table_properties = {
        "quality" : "taxirides2025.silver.vendor",
        "layer" : "silver",
        "source_format" : "csv",
        "delta.enableChangeDataFeed" : "true",
        "delta.autoOptimize.optimizeWrite" : "true",
        "delta.autoOptimize.autoCompact" : "true",
    },
)

def vendor():
    data = [(1, "Creative Mobile Technologies"), (2, "Curb Mobility, LLC"), (6, "Myle Technologies Inc"), (7, "Helix")]

    df = spark.createDataFrame(data, ["VendorID", "vendor_name"])

    df = df.select(
        "VendorID",
        "vendor_name",
    )
    
    return df

