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

# Import libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
from delta.tables import DeltaTable

SILVER_PATH = "Files/silver_layer/cleaned_data"
WATERMARK_PATH = "Files/silver_layer/watermarks/gold_watermarks/"

print("=" * 70)
print("GOLD LAYER - STAR SCHEMA")
print("=" * 70)

# Verify Silver
silver_test = spark.read.format("delta").load(SILVER_PATH)
print(f"Silver accessible: {silver_test.count():,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Helper Functions
def get_watermark(table_name):
    try:
        watermark_df = spark.read.format("delta").load(f"{WATERMARK_PATH}{table_name}")
        return watermark_df.agg(F.max("last_processed_timestamp")).collect()[0][0]
    except:
        return None

def save_watermark(table_name, watermark_value):
    watermark_df = spark.createDataFrame(
        [(watermark_value, datetime.now())],
        ["last_processed_timestamp", "updated_at"]
    )
    watermark_df.write.format("delta").mode("overwrite").save(f"{WATERMARK_PATH}{table_name}")

print("Helper functions loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dim_date - Derived from Silver
print("Creating dim_date...")

silver_df = spark.read.format("delta").load(SILVER_PATH)

# Extract dates from both Order_Date and Ship_Date
order_dates = silver_df.select(F.col("Order_Date").alias("full_date"))
ship_dates = silver_df.select(F.col("Ship_Date").alias("full_date"))

# Union both, remove nulls and duplicates
all_dates = order_dates.union(ship_dates) \
    .filter(F.col("full_date").isNotNull()) \
    .distinct()

# Build date dimension
dim_date_df = all_dates \
    .withColumn("date_key", F.date_format(F.col("full_date"), "yyyyMMdd").cast(IntegerType())) \
    .withColumn("year", F.year(F.col("full_date"))) \
    .withColumn("quarter", F.quarter(F.col("full_date"))) \
    .withColumn("month", F.month(F.col("full_date"))) \
    .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM")) \
    .withColumn("week_of_year", F.weekofyear(F.col("full_date"))) \
    .withColumn("day_of_month", F.dayofmonth(F.col("full_date"))) \
    .withColumn("day_of_week", F.dayofweek(F.col("full_date"))) \
    .withColumn("day_name", F.date_format(F.col("full_date"), "EEEE")) \
    .withColumn("is_weekend", F.dayofweek(F.col("full_date")).isin([1, 7])) \
    .select(
        "date_key", "full_date", "year", "quarter", "month",
        "month_name", "week_of_year", "day_of_month",
        "day_of_week", "day_name", "is_weekend"
    )

# Drop existing table first, then write
spark.sql("DROP TABLE IF EXISTS dim_date")
dim_date_df.write.format("delta").saveAsTable("dim_date")

print(f"dim_date created: {dim_date_df.count():,} records")
print(f"Date range: {dim_date_df.agg(F.min('full_date')).collect()[0][0]} to {dim_date_df.agg(F.max('full_date')).collect()[0][0]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dim_ship_mode - Derived from Silver
print("Creating dim_ship_mode...")

silver_df = spark.read.format("delta").load(SILVER_PATH)

# Extract ship modes and calculate actual avg shipping days
ship_mode_stats = silver_df \
    .groupBy("Ship_Mode") \
    .agg(
        F.round(F.avg("shipping_days")).cast(IntegerType()).alias("expected_delivery_days"),
        F.min("shipping_days").alias("min_days"),
        F.max("shipping_days").alias("max_days")
    )

# Derive shipping_category from actual avg shipping days
dim_ship_mode_df = ship_mode_stats \
    .withColumn("shipping_category",
                F.when(F.col("expected_delivery_days") == 0, "Premium")
                .when(F.col("expected_delivery_days") <= 2, "Express")
                .when(F.col("expected_delivery_days") <= 4, "Standard")
                .otherwise("Economy")) \
    .withColumn("ship_mode_key", F.row_number().over(Window.orderBy("Ship_Mode"))) \
    .select(
        "ship_mode_key",
        F.col("Ship_Mode").alias("ship_mode"),
        "shipping_category",
        "expected_delivery_days"
    )

# Drop and recreate
spark.sql("DROP TABLE IF EXISTS dim_ship_mode")
dim_ship_mode_df.write.format("delta").saveAsTable("dim_ship_mode")

# Show results
print(f"dim_ship_mode created: {dim_ship_mode_df.count()} records")
dim_ship_mode_df.orderBy("expected_delivery_days").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dim_customer - Incremental MERGE (SCD Type 1)
print("Processing dim_customer...")

last_watermark = get_watermark("dim_customer")
silver_df = spark.read.format("delta").load(SILVER_PATH)

if last_watermark:
    new_records = silver_df.filter(F.col("silver_processing_timestamp") > last_watermark)
    print(f"Incremental: {new_records.count():,} new records")
else:
    new_records = silver_df
    print(f"Full load: {new_records.count():,} records")

if new_records.count() > 0:
    new_customers = new_records.select(
        "Customer_ID", "Customer_Name", "Segment"
    ).distinct()

    try:
        existing = spark.read.table("dim_customer")
        max_key = existing.agg(F.max("customer_key")).collect()[0][0] or 0

        customers_with_keys = new_customers \
            .withColumn("row_num", F.row_number().over(Window.orderBy("Customer_ID"))) \
            .withColumn("customer_key", (F.col("row_num") + max_key).cast(IntegerType())) \
            .withColumn("effective_date", F.current_date()) \
            .withColumn("is_current", F.lit(True)) \
            .select(
                "customer_key",
                F.col("Customer_ID").alias("customer_id"),
                F.col("Customer_Name").alias("customer_name"),
                F.col("Segment").alias("segment"),
                "effective_date",
                "is_current"
            )

        # MERGE - Update existing, insert new
        DeltaTable.forName(spark, "dim_customer").alias("target").merge(
            customers_with_keys.alias("source"),
            "target.customer_id = source.customer_id"
        ).whenMatchedUpdate(set={
            "customer_name": "source.customer_name",
            "segment": "source.segment",
            "effective_date": "source.effective_date"
        }).whenNotMatchedInsertAll().execute()

        print("dim_customer: MERGE completed")

    except:
        # First load
        customers_dim = new_customers \
            .withColumn("customer_key", F.row_number().over(Window.orderBy("Customer_ID")).cast(IntegerType())) \
            .withColumn("effective_date", F.current_date()) \
            .withColumn("is_current", F.lit(True)) \
            .select(
                "customer_key",
                F.col("Customer_ID").alias("customer_id"),
                F.col("Customer_Name").alias("customer_name"),
                F.col("Segment").alias("segment"),
                "effective_date",
                "is_current"
            )

        spark.sql("DROP TABLE IF EXISTS dim_customer")
        customers_dim.write.format("delta").saveAsTable("dim_customer")
        print(f"dim_customer created: {customers_dim.count()} records")

    # Save watermark
    save_watermark("dim_customer", new_records.agg(F.max("silver_processing_timestamp")).collect()[0][0])
else:
    print("dim_customer: No new records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dim_product - Incremental MERGE (SCD Type 1)
print("Processing dim_product...")

last_watermark = get_watermark("dim_product")
silver_df = spark.read.format("delta").load(SILVER_PATH)

if last_watermark:
    new_records = silver_df.filter(F.col("silver_processing_timestamp") > last_watermark)
    print(f"Incremental: {new_records.count():,} new records")
else:
    new_records = silver_df
    print(f"Full load: {new_records.count():,} records")

if new_records.count() > 0:
    new_products = new_records.select(
        "Product_ID", "Product_Name", "Category", "Sub_Category"
    ).distinct()

    try:
        existing = spark.read.table("dim_product")
        max_key = existing.agg(F.max("product_key")).collect()[0][0] or 0

        products_with_keys = new_products \
            .withColumn("row_num", F.row_number().over(Window.orderBy("Product_ID"))) \
            .withColumn("product_key", (F.col("row_num") + max_key).cast(IntegerType())) \
            .withColumn("effective_date", F.current_date()) \
            .withColumn("is_current", F.lit(True)) \
            .select(
                "product_key",
                F.col("Product_ID").alias("product_id"),
                F.col("Product_Name").alias("product_name"),
                F.col("Category").alias("category"),
                F.col("Sub_Category").alias("sub_category"),
                "effective_date",
                "is_current"
            )

        # MERGE - Update existing, insert new
        DeltaTable.forName(spark, "dim_product").alias("target").merge(
            products_with_keys.alias("source"),
            "target.product_id = source.product_id"
        ).whenMatchedUpdate(set={
            "product_name": "source.product_name",
            "category": "source.category",
            "sub_category": "source.sub_category",
            "effective_date": "source.effective_date"
        }).whenNotMatchedInsertAll().execute()

        print("dim_product: MERGE completed")

    except:
        # First load
        products_dim = new_products \
            .withColumn("product_key", F.row_number().over(Window.orderBy("Product_ID")).cast(IntegerType())) \
            .withColumn("effective_date", F.current_date()) \
            .withColumn("is_current", F.lit(True)) \
            .select(
                "product_key",
                F.col("Product_ID").alias("product_id"),
                F.col("Product_Name").alias("product_name"),
                F.col("Category").alias("category"),
                F.col("Sub_Category").alias("sub_category"),
                "effective_date",
                "is_current"
            )

        spark.sql("DROP TABLE IF EXISTS dim_product")
        products_dim.write.format("delta").saveAsTable("dim_product")
        print(f"dim_product created: {products_dim.count()} records")

    # Save watermark
    save_watermark("dim_product", new_records.agg(F.max("silver_processing_timestamp")).collect()[0][0])
else:
    print("dim_product: No new records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dim_geography 
print("Processing dim_geography...")

silver_df = spark.read.format("delta").load(SILVER_PATH)

# Extract unique geographies from Silver
new_geo = silver_df.select(
    "City", "State", "Postal_Code", "Region", "Country"
).distinct()

print(f"Unique geographies found: {new_geo.count()}")

# Add surrogate key
dim_geography_df = new_geo \
    .withColumn("geography_key", F.row_number().over(Window.orderBy("City", "State")).cast(IntegerType())) \
    .select(
        "geography_key",
        F.col("City").alias("city"),
        F.col("State").alias("state"),
        F.col("Postal_Code").alias("postal_code"),
        F.col("Region").alias("region"),
        F.col("Country").alias("country")
    )

# Drop and recreate clean
spark.sql("DROP TABLE IF EXISTS dim_geography")
dim_geography_df.write.format("delta").saveAsTable("dim_geography")

# Verify
verify = spark.read.table("dim_geography")
print(f"dim_geography created: {verify.count()} records")
verify.show(10, truncate=False)

# Save watermark
save_watermark("dim_geography", silver_df.agg(F.max("silver_processing_timestamp")).collect()[0][0])
print("Watermark saved")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creat fact_sales - Incremental MERGE with Role-Playing dim_date
print("Processing fact_sales...")

last_watermark = get_watermark("fact_sales")
silver_df = spark.read.format("delta").load(SILVER_PATH)

if last_watermark:
    new_records = silver_df.filter(F.col("silver_processing_timestamp") > last_watermark)
    print(f"Incremental: {new_records.count():,} new records")
else:
    new_records = silver_df
    print(f"Full load: {new_records.count():,} records")

if new_records.count() > 0:
    # Load all dimensions
    dim_customer = spark.read.table("dim_customer")
    dim_product = spark.read.table("dim_product")
    dim_geography = spark.read.table("dim_geography")
    dim_date = spark.read.table("dim_date")
    dim_ship_mode = spark.read.table("dim_ship_mode")

    # Role-Playing: Order_Date → dim_date
    fact_df = new_records.join(
        dim_date.alias("order_dim_date"),
        F.date_format(new_records.Order_Date, "yyyyMMdd").cast(IntegerType()) == F.col("order_dim_date.date_key"),
        "left"
    )

    # Role-Playing: Ship_Date → dim_date
    fact_df = fact_df.join(
        dim_date.alias("ship_dim_date"),
        F.date_format(new_records.Ship_Date, "yyyyMMdd").cast(IntegerType()) == F.col("ship_dim_date.date_key"),
        "left"
    )

    # Join other dimensions
    fact_df = fact_df \
        .join(dim_customer, new_records.Customer_ID == dim_customer.customer_id, "left") \
        .join(dim_product, new_records.Product_ID == dim_product.product_id, "left") \
        .join(dim_geography,
              (new_records.City == dim_geography.city) &
              (new_records.State == dim_geography.state) &
              (new_records.Postal_Code == dim_geography.postal_code), "left") \
        .join(dim_ship_mode, new_records.Ship_Mode == dim_ship_mode.ship_mode, "left")

    # Select fact columns - TWO date keys
    fact_sales_df = fact_df.select(
        F.col("Row_ID").cast(IntegerType()).alias("row_id"),
        F.col("Order_ID").alias("order_id"),
        F.col("order_dim_date.date_key").alias("order_date_key"),
        F.col("ship_dim_date.date_key").alias("ship_date_key"),
        "customer_key",
        "product_key",
        "geography_key",
        "ship_mode_key",
        F.col("Sales").alias("sales"),
        F.col("Quantity").cast(IntegerType()).alias("quantity"),
        F.col("Discount").alias("discount"),
        F.col("Profit").alias("profit"),
        F.col("shipping_days").cast(IntegerType()).alias("shipping_days"),
        F.col("profit_margin").alias("profit_margin"),
        F.col("revenue_per_quantity").alias("revenue_per_quantity"),
        F.col("is_profitable").cast("boolean").alias("is_profitable"),
        F.col("has_discount").cast("boolean").alias("has_discount"),
        "silver_processing_timestamp"
    )

    try:
        # MERGE into existing fact table
        DeltaTable.forName(spark, "fact_sales").alias("target").merge(
            fact_sales_df.alias("source"),
            "target.row_id = source.row_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"fact_sales: MERGE completed")

    except:
        # First load
        spark.sql("DROP TABLE IF EXISTS fact_sales")
        fact_sales_df.write.format("delta").saveAsTable("fact_sales")
        print(f"fact_sales created: {fact_sales_df.count():,} records")

    # Save watermark
    save_watermark("fact_sales", new_records.agg(F.max("silver_processing_timestamp")).collect()[0][0])
else:
    print("fact_sales: No new records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find which dimension is causing duplicates
print("Duplicate Check on Dimensions")
print("=" * 70)

# dim_geography - check duplicate natural keys
geo = spark.read.table("dim_geography")
geo_dupes = geo.groupBy("city", "state", "postal_code").count().filter(F.col("count") > 1)
print(f"dim_geography duplicates: {geo_dupes.count()}")
if geo_dupes.count() > 0:
    geo_dupes.show(10, truncate=False)

# dim_customer - check duplicate natural keys
cust = spark.read.table("dim_customer")
cust_dupes = cust.groupBy("customer_id").count().filter(F.col("count") > 1)
print(f"\ndim_customer duplicates: {cust_dupes.count()}")
if cust_dupes.count() > 0:
    cust_dupes.show(10, truncate=False)

# dim_product - check duplicate natural keys
prod = spark.read.table("dim_product")
prod_dupes = prod.groupBy("product_id").count().filter(F.col("count") > 1)
print(f"\ndim_product duplicates: {prod_dupes.count()}")
if prod_dupes.count() > 0:
    prod_dupes.show(10, truncate=False)

# dim_date - check duplicate date_key
dt = spark.read.table("dim_date")
dt_dupes = dt.groupBy("date_key").count().filter(F.col("count") > 1)
print(f"\ndim_date duplicates: {dt_dupes.count()}")
if dt_dupes.count() > 0:
    dt_dupes.show(10, truncate=False)

# dim_ship_mode - check duplicate ship_mode
ship = spark.read.table("dim_ship_mode")
ship_dupes = ship.groupBy("ship_mode").count().filter(F.col("count") > 1)
print(f"\ndim_ship_mode duplicates: {ship_dupes.count()}")
if ship_dupes.count() > 0:
    ship_dupes.show(10, truncate=False)

print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Show all duplicate products with full details
print("All Duplicate Products in dim_product")
print("=" * 70)

dim_product = spark.read.table("dim_product")

# Get all duplicate product_ids
duplicate_ids = dim_product \
    .groupBy("product_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .select("product_id")

print(f"Total duplicate product_ids: {duplicate_ids.count()}\n")

# Show ALL rows for duplicate product_ids with full details
duplicate_rows = dim_product.join(duplicate_ids, "product_id") \
    .orderBy("product_id", "product_key")

print(f"Total duplicate rows: {duplicate_rows.count()}\n")
duplicate_rows.show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
