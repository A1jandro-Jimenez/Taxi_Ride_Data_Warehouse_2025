from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name = "taxirides2025.silver.ratecode",
    comment = "Ratecode dimension with data attributes (2025)",
    table_properties = {
        "quality" : "taxirides2025.silver.ratecode",
        "layer" : "silver",
        "source_format" : "csv",
        "delta.enableChangeDataFeed" : "true",
        "delta.autoOptimize.optimizeWrite" : "true",
        "delta.autoOptimize.autoCompact" : "true",
    },
)

def vendor():
    data = [(1, "Standard rate"), (2, "JFK"), (3, "Newark"),(4,"Nassau or Westchester"), (5, "Negotiated fare"), (6, "Group ride"), (99, "Null/unknown")]

    df = spark.createDataFrame(data, ["RateCodeID", "Rate"])

    df = df.select(
        "RateCodeID",
        "Rate",
    )
    
    return df
