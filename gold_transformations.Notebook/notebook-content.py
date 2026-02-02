# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dd55c76e-39a6-4901-bd1d-80e052337b74",
# META       "default_lakehouse_name": "superstore_silver",
# META       "default_lakehouse_workspace_id": "79978027-ed02-401b-8c3f-d91bb5c9a294",
# META       "known_lakehouses": [
# META         {
# META           "id": "dd55c76e-39a6-4901-bd1d-80e052337b74"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "27c88d7d-4268-a72a-499b-c0a91f1ab2e6",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Import libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import date, datetime, timedelta
from delta.tables import DeltaTable


# Configuration
SILVER_PATH = "Files/superstore_silver/cleaned_data"
GOLD_PATH = "Files/superstore_gold/"
WATERMARK_PATH = "Files/superstore_gold/watermarks/"

print("Gold Layer - Star Schema Implementation")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if Silver data exists using Spark
print("Searching for Silver data...")
print("=" * 70)

# Try different possible paths
paths_to_check = [
    "Files/superstore_silver/cleaned_data",
    "Files/cleaned_data",
    "Tables/superstore_silver_cleaned"
]

for path in paths_to_check:
    try:
        df = spark.read.format("delta").load(path)
        count = df.count()
        cols = len(df.columns)
        print(f"\nFOUND at: {path}")
        print(f"  Records: {count:,}")
        print(f"  Columns: {cols}")
        print(f"\nUse this path in Gold pipeline!")
        break
    except Exception as e:
        print(f"Not found at: {path}")

# Also check if there are any tables in the lakehouse
print("\n" + "=" * 70)
print("Checking for tables...")
try:
    tables = spark.sql("SHOW TABLES").collect()
    if tables:
        print("Available tables:")
        for table in tables:
            print(f"  - {table.namespace}.{table.tableName}")
    else:
        print("No tables found")
except:
    print("No tables found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate date dimension for 10 years (2014-2024)
# This only needs to run once or when extending date range

def generate_date_dimension(start_year, end_year):
    """Generate complete date dimension"""
    dates = []
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)
    
    current_date = start_date
    while current_date <= end_date:
        dates.append({
            'date_key': int(current_date.strftime('%Y%m%d')),
            'full_date': current_date,
            'year': current_date.year,
            'quarter': (current_date.month - 1) // 3 + 1,
            'month': current_date.month,
            'month_name': current_date.strftime('%B'),
            'week_of_year': current_date.isocalendar()[1],
            'day_of_month': current_date.day,
            'day_of_week': current_date.isoweekday(),
            'day_name': current_date.strftime('%A'),
            'is_weekend': current_date.isoweekday() in [6, 7]
        })
        current_date += timedelta(days=1)
    
    return dates

# Generate date dimension
date_data = generate_date_dimension(2014, 2024)
dim_date_df = spark.createDataFrame(date_data)

# Write to Gold layer
dim_date_path = GOLD_PATH + "dim_date"
dim_date_df.write.format("delta").mode("overwrite").save(dim_date_path)

print(f"dim_date created: {dim_date_df.count()} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create ship mode dimension - static reference table

ship_modes = [
    (1, 'Standard Class', 'Economy', 5),
    (2, 'Second Class', 'Standard', 3),
    (3, 'First Class', 'Express', 2),
    (4, 'Same Day', 'Premium', 0)
]

dim_ship_mode_df = spark.createDataFrame(
    ship_modes,
    ['ship_mode_key', 'ship_mode', 'shipping_category', 'expected_delivery_days']
)

# Write to Gold layer
dim_ship_mode_path = GOLD_PATH + "dim_ship_mode"
dim_ship_mode_df.write.format("delta").mode("overwrite").save(dim_ship_mode_path)

print(f"dim_ship_mode created: {dim_ship_mode_df.count()} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get watermark for dim_geography
def get_watermark(table_name):
    try:
        watermark_df = spark.read.format("delta").load(f"{WATERMARK_PATH}{table_name}")
        return watermark_df.agg(F.max("last_processed_timestamp")).collect()[0][0]
    except:
        return None

last_watermark = get_watermark("dim_geography")

# Read new records from Silver
silver_df = spark.read.format("delta").load(SILVER_PATH)

if last_watermark:
    new_records = silver_df.filter(F.col("silver_processing_timestamp") > last_watermark)
    print(f"Incremental: {new_records.count()} new silver records")
else:
    new_records = silver_df
    print(f"Full load: {new_records.count()} silver records")

# Extract unique geographies from new records
new_geographies = new_records.select(
    "City", "State", "Postal_Code", "Region", "Country"
).distinct()

# Check if dim_geography exists
dim_geography_path = GOLD_PATH + "dim_geography"
try:
    existing_geography = spark.read.format("delta").load(dim_geography_path)
    
    # Find truly new geographies (not already in dimension)
    new_only = new_geographies.join(
        existing_geography,
        (new_geographies.City == existing_geography.city) &
        (new_geographies.State == existing_geography.state) &
        (new_geographies.Postal_Code == existing_geography.postal_code),
        "left_anti"
    )
    
    if new_only.count() > 0:
        # Generate new surrogate keys
        max_key = existing_geography.agg(F.max("geography_key")).collect()[0][0] or 0
        
        new_geography_dim = new_only \
            .withColumn("row_num", F.row_number().over(Window.orderBy("City", "State"))) \
            .withColumn("geography_key", F.col("row_num") + max_key) \
            .select(
                "geography_key",
                F.col("City").alias("city"),
                F.col("State").alias("state"),
                F.col("Postal_Code").alias("postal_code"),
                F.col("Region").alias("region"),
                F.col("Country").alias("country")
            )
        
        # Append new geographies
        new_geography_dim.write.format("delta").mode("append").save(dim_geography_path)
        print(f"dim_geography: {new_geography_dim.count()} new locations added")
    else:
        print("dim_geography: No new locations to add")
        
except:
    # First load - create dimension
    new_geography_dim = new_geographies \
        .withColumn("geography_key", F.row_number().over(Window.orderBy("City", "State"))) \
        .select(
            "geography_key",
            F.col("City").alias("city"),
            F.col("State").alias("state"),
            F.col("Postal_Code").alias("postal_code"),
            F.col("Region").alias("region"),
            F.col("Country").alias("country")
        )
    
    new_geography_dim.write.format("delta").mode("overwrite").save(dim_geography_path)
    print(f"dim_geography created: {new_geography_dim.count()} records")

# Update watermark
if new_records.count() > 0:
    new_watermark = new_records.agg(F.max("silver_processing_timestamp")).collect()[0][0]
    watermark_df = spark.createDataFrame(
        [(new_watermark, datetime.now())],
        ["last_processed_timestamp", "updated_at"]
    )
    watermark_df.write.format("delta").mode("overwrite").save(f"{WATERMARK_PATH}dim_geography")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
