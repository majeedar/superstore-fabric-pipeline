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
# META     },
# META     "environment": {
# META       "environmentId": "27c88d7d-4268-a72a-499b-c0a91f1ab2e6",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Import Libraries
import kagglehub
import pandas as pd
import os
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit

BRONZE_PATH = "Files/bronze_layer/raw_data"

print("=" * 70)
print("BRONZE LAYER - DATA INGESTION")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Download from Kaggle
dataset_path = kagglehub.dataset_download("vivek468/superstore-dataset-final")
csv_file = os.path.join(dataset_path, "Sample - Superstore.csv")

df = pd.read_csv(csv_file, encoding='latin-1', encoding_errors='ignore',
                 engine='python', on_bad_lines='skip')

print(f"Records loaded: {len(df):,}")
print(f"Columns: {list(df.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(df)

# Clean column names
for col_name in spark_df.columns:
    clean_name = col_name.strip().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "")
    if col_name != clean_name:
        spark_df = spark_df.withColumnRenamed(col_name, clean_name)

# Add metadata
spark_df = spark_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("kaggle")) \
    .withColumn("source_file", lit("Sample - Superstore.csv"))

print(f"Columns after cleaning: {spark_df.columns}")
spark_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the data to Bronze
spark_df.write.format("delta").mode("overwrite").save(BRONZE_PATH)

# Verify
verify = spark.read.format("delta").load(BRONZE_PATH)
print("\n" + "=" * 70)
print("BRONZE LAYER COMPLETE")
print("=" * 70)
print(f"Records: {verify.count():,}")
print(f"Columns: {len(verify.columns)}")
print(f"Location: {BRONZE_PATH}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
