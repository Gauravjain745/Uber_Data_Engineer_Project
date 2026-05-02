# Cell 1 - Set up ADLS connection
# PASTE YOUR STORAGE ACCOUNT KEY BELOW
spark.conf.set(
    "fs.azure.account.key.uberdlgjain.dfs.core.windows.net",
    "PASTE_YOUR_STORAGE_KEY_HERE"
)

# Cell 2 - Load mapping files from ADLS bronze/ingestion into Delta tables
import pandas as pd

files = [
    {"file": "map_cities"},
    {"file": "map_cancellation_reasons"},
    {"file": "map_payment_methods"},
    {"file": "map_ride_statuses"},
    {"file": "map_vehicle_makes"},
    {"file": "map_vehicle_types"}
]

for file in files:
    url = f"abfss://bronze@uberdlgjain.dfs.core.windows.net/ingestion/{file['file']}.json"
    df = spark.read.option("multiline", "true").json(url)
    df.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .saveAsTable(f"uber.bronze.{file['file']}")
    print(f"Loaded {file['file']} successfully")

# Cell 3 - Load bulk rides (only once)
bulk_url = "abfss://bronze@uberdlgjain.dfs.core.windows.net/ingestion/bulk_rides.json"
df = spark.read.option("multiline", "true").json(bulk_url)
df_spark = spark.createDataFrame(df.toPandas())
if not spark.catalog.tableExists("uber.bronze.bulk_rides"):
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .saveAsTable("uber.bronze.bulk_rides")
    print("bulk_rides loaded - will not run more than 1 time")
else:
    print("bulk_rides already exists - skipping")

# Cell 4 - Verify data loaded
display(spark.sql("SELECT * FROM uber.bronze.map_cities LIMIT 5"))
