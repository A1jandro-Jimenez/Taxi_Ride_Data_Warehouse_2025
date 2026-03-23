from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name = "taxirides2025.silver.payment_type",
    comment = "Payment type dimension with data attributes (2025)",
    table_properties = {
        "quality" : "taxirides2025.silver.payment_type",
        "layer" : "silver",
        "source_format" : "csv",
        "delta.enableChangeDataFeed" : "true",
        "delta.autoOptimize.optimizeWrite" : "true",
        "delta.autoOptimize.autoCompact" : "true",
    },
)

def vendor():
    data = [(0, "Flex Fare trip"), 
            (1, "Credit card"),
            (2, "Cash"),
            (3,"No charge"), 
            (4, "Dispute"), 
            (5, "Unknown"), 
            (6, "Voided trip")]

    df = spark.createDataFrame(data, ["PaymentTypeID", "payment_type"])

    df = df.select(
        "PaymentTypeID",
        "payment_type",
    )
    
    return df
