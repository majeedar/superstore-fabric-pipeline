# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a109ee49-a219-4882-afc7-f836c70d955c",
# META       "default_lakehouse_name": "superstore_bronze",
# META       "default_lakehouse_workspace_id": "79978027-ed02-401b-8c3f-d91bb5c9a294",
# META       "known_lakehouses": [
# META         {
# META           "id": "a109ee49-a219-4882-afc7-f836c70d955c"
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
from datetime import datetime
from delta.tables import DeltaTable

# Configuration
BRONZE_PATH = "Files/superstore_bronze/raw_data"
SILVER_PATH = "Files/superstore_silver/cleaned_data"
WATERMARK_PATH = "Files/superstore_silver/watermark/"

print("Silver Layer Transformation Pipeline")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for existing watermark to determine load type
def get_last_watermark(watermark_path):
    try:
        watermark_df = spark.read.format("delta").load(watermark_path)
        last_timestamp = watermark_df.agg(F.max("last_processed_timestamp")).collect()[0][0]
        return last_timestamp
    except:
        return None

last_watermark = get_last_watermark(WATERMARK_PATH)

if last_watermark:
    print(f"Incremental Load Mode - Last watermark: {last_watermark}")
else:
    print("Full Load Mode - No existing watermark found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read bronze layer
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

# Filter based on watermark (incremental) or load all (full)
if last_watermark:
    new_records_df = bronze_df.filter(F.col("ingestion_timestamp") > last_watermark)
else:
    new_records_df = bronze_df

record_count = new_records_df.count()
print(f"Records to process: {record_count:,}")

# Set flag for downstream processing
HAS_NEW_RECORDS = record_count > 0

if not HAS_NEW_RECORDS:
    print("No new records to process - pipeline will skip transformations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Only transform if there are new records
if HAS_NEW_RECORDS:
    # Data type conversions
    silver_df = new_records_df \
        .withColumn("Order_Date", F.to_date(F.col("Order_Date"), "M/d/yyyy")) \
        .withColumn("Ship_Date", F.to_date(F.col("Ship_Date"), "M/d/yyyy")) \
        .withColumn("Sales", F.col("Sales").cast(DecimalType(10, 2))) \
        .withColumn("Quantity", F.col("Quantity").cast(IntegerType())) \
        .withColumn("Discount", F.col("Discount").cast(DecimalType(5, 4))) \
        .withColumn("Profit", F.col("Profit").cast(DecimalType(10, 2)))

    # Text cleaning
    silver_df = silver_df \
        .withColumn("Customer_Name", F.trim(F.initcap(F.col("Customer_Name")))) \
        .withColumn("Product_Name", F.trim(F.col("Product_Name"))) \
        .withColumn("City", F.trim(F.upper(F.col("City")))) \
        .withColumn("State", F.trim(F.upper(F.col("State")))) \
        .withColumn("Country", F.trim(F.upper(F.col("Country")))) \
        .withColumn("Region", F.trim(F.col("Region"))) \
        .withColumn("Ship_Mode", F.trim(F.col("Ship_Mode"))) \
        .withColumn("Segment", F.trim(F.col("Segment"))) \
        .withColumn("Category", F.trim(F.col("Category"))) \
        .withColumn("Sub_Category", F.trim(F.col("Sub_Category"))) \
        .withColumn("Postal_Code", F.lpad(F.trim(F.col("Postal_Code")), 5, "0"))

    # Derived columns
    silver_df = silver_df \
        .withColumn("order_year", F.year(F.col("Order_Date"))) \
        .withColumn("order_month", F.month(F.col("Order_Date"))) \
        .withColumn("order_quarter", F.quarter(F.col("Order_Date"))) \
        .withColumn("order_year_month", F.date_format(F.col("Order_Date"), "yyyy-MM")) \
        .withColumn("shipping_days", F.datediff(F.col("Ship_Date"), F.col("Order_Date"))) \
        .withColumn("profit_margin", F.when(F.col("Sales") > 0, (F.col("Profit") / F.col("Sales")) * 100).otherwise(0)) \
        .withColumn("revenue_per_quantity", F.when(F.col("Quantity") > 0, F.col("Sales") / F.col("Quantity")).otherwise(0)) \
        .withColumn("is_profitable", F.when(F.col("Profit") > 0, "Yes").otherwise("No")) \
        .withColumn("has_discount", F.col("Discount") > 0) \
        .withColumn("discount_category",
                    F.when(F.col("Discount") == 0, "No Discount")
                    .when(F.col("Discount") <= 0.1, "Low (0-10%)")
                    .when(F.col("Discount") <= 0.2, "Medium (10-20%)")
                    .otherwise("High (>20%)"))

    # Data quality
    silver_df = silver_df.fillna({"Discount": 0, "Postal_Code": "UNKNOWN"})

    critical_columns = ['Row_ID', 'Order_ID', 'Customer_ID', 'Product_ID', 'Order_Date', 'Sales', 'Quantity', 'Profit']
    for col in critical_columns:
        silver_df = silver_df.filter(F.col(col).isNotNull())

    silver_df = silver_df \
        .withColumn("is_valid_sale", F.col("Sales") > 0) \
        .withColumn("is_valid_quantity", F.col("Quantity") > 0) \
        .withColumn("is_valid_dates", F.col("Ship_Date") >= F.col("Order_Date")) \
        .withColumn("is_valid_discount", (F.col("Discount") >= 0) & (F.col("Discount") <= 1)) \
        .withColumn("data_quality_score",
                    (F.col("is_valid_sale").cast("int") + F.col("is_valid_quantity").cast("int") +
                     F.col("is_valid_dates").cast("int") + F.col("is_valid_discount").cast("int")) / 4.0 * 100) \
        .withColumn("record_status",
                    F.when(F.col("data_quality_score") == 100, "Valid")
                    .when(F.col("data_quality_score") >= 75, "Warning")
                    .otherwise("Invalid")) \
        .withColumn("silver_processing_timestamp", F.current_timestamp()) \
        .withColumn("silver_layer_version", F.lit("v1.0"))

    print(f"Transformations complete: {silver_df.count():,} records")
else:
    print("Skipping transformations - no new records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Only write if there are new records
if HAS_NEW_RECORDS:
    # Check if silver table exists
    try:
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        table_exists = True
    except:
        table_exists = False

    # Write logic based on whether table exists
    if table_exists:
        print("Performing MERGE (upsert)...")
        silver_table.alias("target").merge(
            silver_df.alias("source"),
            "target.Row_ID = source.Row_ID"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        print("Performing initial load (overwrite)...")
        silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)

    print("Silver layer updated successfully")
else:
    print("Skipping silver update - no new records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Only update watermark if there are new records
if HAS_NEW_RECORDS:
    new_watermark = new_records_df.agg(F.max("ingestion_timestamp")).collect()[0][0]
    
    watermark_df = spark.createDataFrame(
        [(new_watermark, datetime.now())],
        ["last_processed_timestamp", "updated_at"]
    )
    
    watermark_df.write.format("delta").mode("overwrite").save(WATERMARK_PATH)
    
    print(f"Watermark updated: {new_watermark}")
else:
    print("No new records - watermark unchanged")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Final verification
silver_final = spark.read.format("delta").load(SILVER_PATH)

print("\n" + "=" * 70)
print("PIPELINE EXECUTION SUMMARY")
print("=" * 70)
print(f"Total records in Silver: {silver_final.count():,}")
print(f"Records processed this run: {record_count:,}")
print(f"Load type: {'Incremental' if last_watermark else 'Full'}")

if HAS_NEW_RECORDS:
    print("\nQuality Distribution:")
    silver_final.groupBy("record_status").count().show()

print("\nPipeline completed successfully")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
