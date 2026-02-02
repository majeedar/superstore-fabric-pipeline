# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "db097b4f-cccf-4967-9222-1daf1746a05f",
# META       "default_lakehouse_name": "superstore_project",
# META       "default_lakehouse_workspace_id": "79978027-ed02-401b-8c3f-d91bb5c9a294",
# META       "known_lakehouses": [
# META         {
# META           "id": "db097b4f-cccf-4967-9222-1daf1746a05f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Import Libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
from delta.tables import DeltaTable

BRONZE_PATH = "Files/bronze_layer/raw_data"
SILVER_PATH = "Files/silver_layer/cleaned_data"
WATERMARK_PATH = "Files/silver_layer/watermarks/silver_watermark/"

print("=" * 70)
print("SILVER LAYER - TRANSFORMATION & VALIDATION")
print("=" * 70)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Watermark Check
def get_last_watermark(watermark_path):
    try:
        watermark_df = spark.read.format("delta").load(watermark_path)
        return watermark_df.agg(F.max("last_processed_timestamp")).collect()[0][0]
    except:
        return None

last_watermark = get_last_watermark(WATERMARK_PATH)

if last_watermark:
    print(f"Incremental Mode - Last watermark: {last_watermark}")
else:
    print("Full Load Mode - No watermark found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Bronze & Filter
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

if last_watermark:
    new_records_df = bronze_df.filter(F.col("ingestion_timestamp") > last_watermark)
    print(f"Incremental: {new_records_df.count():,} new records")
else:
    new_records_df = bronze_df
    print(f"Full load: {new_records_df.count():,} records")

HAS_NEW_RECORDS = new_records_df.count() > 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transformations
if HAS_NEW_RECORDS:
    silver_df = new_records_df \
        .withColumn("Order_Date", F.to_date(F.col("Order_Date"), "M/d/yyyy")) \
        .withColumn("Ship_Date", F.to_date(F.col("Ship_Date"), "M/d/yyyy")) \
        .withColumn("Sales", F.col("Sales").cast(DecimalType(10, 2))) \
        .withColumn("Quantity", F.col("Quantity").cast(IntegerType())) \
        .withColumn("Discount", F.col("Discount").cast(DecimalType(5, 4))) \
        .withColumn("Profit", F.col("Profit").cast(DecimalType(10, 2))) \
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
        .withColumn("Postal_Code", F.lpad(F.trim(F.col("Postal_Code").cast("string")), 5, "0")) \
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
                    .otherwise("High (>20%)")) \
        .fillna({"Discount": 0, "Postal_Code": "UNKNOWN"})

    # Remove nulls in critical columns
    critical_columns = ['Row_ID', 'Order_ID', 'Customer_ID', 'Product_ID', 'Order_Date', 'Sales', 'Quantity', 'Profit']
    for col in critical_columns:
        silver_df = silver_df.filter(F.col(col).isNotNull())

    # Quality flags
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
                    .otherwise("Invalid"))

    # Deduplicate
    window_spec = Window.partitionBy("Order_ID", "Product_ID").orderBy(F.col("ingestion_timestamp").desc())
    silver_df = silver_df \
        .withColumn("row_num", F.row_number().over(window_spec)) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")

    # Metadata
    silver_df = silver_df \
        .withColumn("silver_processing_timestamp", F.current_timestamp()) \
        .withColumn("silver_layer_version", F.lit("v1.0"))

    print(f"Transformations complete: {silver_df.count():,} records")
else:
    print("No new records to transform")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write Silver & Update Watermark
if HAS_NEW_RECORDS:
    try:
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        silver_table.alias("target").merge(
            silver_df.alias("source"),
            "target.Row_ID = source.Row_ID"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("MERGE completed")
    except:
        silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
        print("Initial load completed")

    # Update watermark
    new_watermark = new_records_df.agg(F.max("ingestion_timestamp")).collect()[0][0]
    watermark_df = spark.createDataFrame(
        [(new_watermark, datetime.now())],
        ["last_processed_timestamp", "updated_at"]
    )
    watermark_df.write.format("delta").mode("overwrite").save(WATERMARK_PATH)
    print(f"Watermark updated: {new_watermark}")
else:
    print("No new records to write")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verification
silver_verify = spark.read.format("delta").load(SILVER_PATH)

print("\n" + "=" * 70)
print("SILVER LAYER COMPLETE")
print("=" * 70)
print(f"Records: {silver_verify.count():,}")
print(f"Columns: {len(silver_verify.columns)}")
print("\nQuality Distribution:")
silver_verify.groupBy("record_status").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
