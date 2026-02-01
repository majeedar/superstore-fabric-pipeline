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

# Install kagglehub
#%pip install kagglehub

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import libraries
import kagglehub
import pandas as pd
import os
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import datetime
import json


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Download the dataset from Kaggle
print("Downloading dataset from Kaggle...")
dataset_path = kagglehub.dataset_download("vivek468/superstore-dataset-final")
print(f"✅ Dataset downloaded to: {dataset_path}")

# List all files in the dataset
print("\nFiles in dataset:")
for file in os.listdir(dataset_path):
    file_path = os.path.join(dataset_path, file)
    file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert to MB
    print(f"  - {file} ({file_size:.2f} MB)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the CSV 
csv_file = os.path.join(dataset_path, "Sample - Superstore.csv")

print("\nReading CSV file with flexible options...")

# Try multiple approaches
try:
    # Approach 1: Use Python engine with encoding errors ignored
    df = pd.read_csv(
        csv_file,
        encoding='latin-1',
        encoding_errors='ignore',
        engine='python',
        on_bad_lines='skip'  # Skip problematic lines
    )
    print("✅ Dataset loaded successfully!")
except Exception as e:
    print(f"Approach 1 failed: {e}")
    
    try:
        # Approach 2: Try UTF-8 with error handling
        df = pd.read_csv(
            csv_file,
            encoding='utf-8',
            encoding_errors='replace',
            engine='python',
            on_bad_lines='skip'
        )
        print("✅ Dataset loaded successfully with UTF-8!")
    except Exception as e2:
        print(f"Approach 2 failed: {e2}")
        
        # Approach 3: Try ISO-8859-1
        df = pd.read_csv(
            csv_file,
            encoding='iso-8859-1',
            engine='python',
            on_bad_lines='skip',
            quoting=3  # QUOTE_NONE
        )
        print("✅ Dataset loaded successfully with ISO-8859-1!")

print(f"\nShape: {df.shape[0]} rows × {df.shape[1]} columns")
print(f"Columns: {list(df.columns)}")
print("\nFirst 5 records:")
print(df.head())
print(f"\nData types:\n{df.dtypes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert to Spark DataFrame
print("\nConverting to Spark DataFrame...")
spark_df = spark.createDataFrame(df)

# Add bronze layer metadata
spark_df = spark_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("kaggle")) \
    .withColumn("source_file", lit("Sample - Superstore.csv"))

print(f"Total records to ingest: {spark_df.count()}")

# Function to clean column names
def clean_column_name(col_name):
    """Remove spaces and special characters from column names"""
    return col_name.replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "")

# Rename all columns
for col in spark_df.columns:
    clean_col = clean_column_name(col)
    if col != clean_col:
        spark_df = spark_df.withColumnRenamed(col, clean_col)
        print(f"  Renamed: '{col}' -> '{clean_col}'")

print("\n✅ Column names cleaned!")
print(f"New columns: {spark_df.columns}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write to Bronze Layer Files
print("\nWriting to bronze layer files...")

# Define the output path in Files section
bronze_path = "Files/superstore_bronze/raw_data"

# Option A: Write as Delta format (RECOMMENDED for bronze layer)
spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_path)

print(f"✅ Data successfully written to {bronze_path}")
print(f"   Format: Delta")
print(f"   Records: {spark_df.count()}")

# Cell 7: Verify the data
print("\n" + "="*60)
print("VERIFICATION")
print("="*60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Quality Monitoring

# MARKDOWN ********************

# ## 

# CELL ********************

# Complete Data Quality Monitoring Framework for Bronze Layer

class BronzeDataQuality:
    """Data Quality Monitoring for Bronze Layer"""
    
    def __init__(self, df: DataFrame, dataset_name: str):
        self.df = df
        self.dataset_name = dataset_name
        self.timestamp = datetime.now()
        self.quality_results = {}
    
    def check_row_count(self, min_expected=1):
        """Check if dataset has minimum expected rows"""
        row_count = self.df.count()
        status = "PASS" if row_count >= min_expected else "FAIL"
        
        self.quality_results['row_count'] = {
            'metric': 'Row Count',
            'value': row_count,
            'threshold': min_expected,
            'status': status
        }
        print(f"✓ Row Count: {row_count} (Min Expected: {min_expected}) - {status}")
        return status == "PASS"
    
    def check_null_counts(self, critical_columns=None):
        """Check null counts for all columns"""
        print("\n📊 Null Value Analysis:")
        null_counts = []
        
        for col in self.df.columns:
            null_count = self.df.filter(F.col(col).isNull()).count()
            null_pct = (null_count / self.df.count()) * 100
            
            status = "PASS"
            if critical_columns and col in critical_columns and null_count > 0:
                status = "FAIL"
            
            null_counts.append({
                'column': col,
                'null_count': null_count,
                'null_percentage': round(null_pct, 2),
                'status': status
            })
            
            symbol = "✓" if status == "PASS" else "✗"
            print(f"{symbol} {col}: {null_count} nulls ({null_pct:.2f}%)")
        
        self.quality_results['null_analysis'] = null_counts
        return null_counts
    
    def check_duplicates(self, key_columns=None):
        """Check for duplicate records"""
        if key_columns:
            duplicate_count = self.df.groupBy(key_columns).count().filter(F.col("count") > 1).count()
            total_duplicates = self.df.groupBy(key_columns).count().filter(F.col("count") > 1).agg(F.sum("count")).collect()[0][0] or 0
        else:
            duplicate_count = self.df.count() - self.df.distinct().count()
            total_duplicates = duplicate_count
        
        status = "PASS" if duplicate_count == 0 else "WARNING"
        
        self.quality_results['duplicates'] = {
            'duplicate_groups': duplicate_count,
            'total_duplicate_rows': total_duplicates,
            'status': status
        }
        
        print(f"\n📋 Duplicate Analysis:")
        print(f"{'✓' if status == 'PASS' else '⚠'} Duplicate groups: {duplicate_count}")
        print(f"{'✓' if status == 'PASS' else '⚠'} Total duplicate rows: {total_duplicates} - {status}")
        
        return status == "PASS"
    
    def check_data_types(self):
        """Validate data types and identify potential issues"""
        print("\n🔍 Data Type Analysis:")
        type_issues = []
        
        for col, dtype in self.df.dtypes:
            print(f"  {col}: {dtype}")
            
            # Check for potential type mismatches
            if dtype == "string":
                # Check if column should be numeric
                numeric_count = self.df.filter(F.col(col).cast("double").isNotNull()).count()
                if numeric_count == self.df.count() and self.df.count() > 0:
                    type_issues.append({
                        'column': col,
                        'current_type': dtype,
                        'suggested_type': 'numeric',
                        'confidence': 'high'
                    })
        
        self.quality_results['data_types'] = {
            'schema': self.df.dtypes,
            'potential_issues': type_issues
        }
        
        if type_issues:
            print("\n⚠ Potential Type Issues Found:")
            for issue in type_issues:
                print(f"  - {issue['column']}: {issue['current_type']} → {issue['suggested_type']}")
        
        return type_issues
    
    def check_value_ranges(self, numeric_columns=None):
        """Check min/max values for numeric columns"""
        if not numeric_columns:
            numeric_columns = [col for col, dtype in self.df.dtypes if dtype in ['int', 'bigint', 'double', 'float']]
        
        if not numeric_columns:
            print("\n📈 No numeric columns to analyze")
            return
        
        print("\n📈 Numeric Value Ranges:")
        range_stats = []
        
        for col in numeric_columns:
            stats = self.df.select(
                F.min(col).alias('min'),
                F.max(col).alias('max'),
                F.avg(col).alias('avg'),
                F.stddev(col).alias('stddev')
            ).collect()[0]
            
            range_stats.append({
                'column': col,
                'min': stats['min'],
                'max': stats['max'],
                'avg': stats['avg'],
                'stddev': stats['stddev']
            })
            
            print(f"  {col}:")
            print(f"    Min: {stats['min']}, Max: {stats['max']}")
            print(f"    Avg: {stats['avg']:.2f}" if stats['avg'] else "    Avg: None")
        
        self.quality_results['value_ranges'] = range_stats
        return range_stats
    
    def check_column_count(self, expected_columns=None):
        """Verify expected number of columns"""
        actual_count = len(self.df.columns)
        status = "PASS"
        
        if expected_columns:
            status = "PASS" if actual_count == expected_columns else "FAIL"
            print(f"\n📋 Column Count: {actual_count} (Expected: {expected_columns}) - {status}")
        else:
            print(f"\n📋 Column Count: {actual_count}")
        
        self.quality_results['column_count'] = {
            'actual': actual_count,
            'expected': expected_columns,
            'status': status
        }
        
        return status == "PASS"
    
    def generate_report(self):
        """Generate comprehensive quality report"""
        print("\n" + "="*70)
        print("DATA QUALITY REPORT")
        print("="*70)
        print(f"Dataset: {self.dataset_name}")
        print(f"Timestamp: {self.timestamp}")
        print(f"Total Records: {self.df.count()}")
        print(f"Total Columns: {len(self.df.columns)}")
        
        # Summary
        total_checks = len(self.quality_results)
        passed_checks = sum(1 for v in self.quality_results.values() 
                          if isinstance(v, dict) and v.get('status') == 'PASS')
        
        print(f"\n✅ Quality Checks Passed: {passed_checks}/{total_checks}")
        print("="*70)
        
        return self.quality_results
    
    def save_quality_metrics(self, output_path="Files/superstore_bronze/quality_metrics/"):
        """Save quality metrics to Delta table"""
        # Convert results to DataFrame
        metrics_data = [{
            'dataset_name': self.dataset_name,
            'check_timestamp': self.timestamp,
            'total_records': self.df.count(),
            'total_columns': len(self.df.columns),
            'quality_results': json.dumps(self.quality_results)
        }]
        
        metrics_df = spark.createDataFrame(metrics_data)
        
        # Write to Delta table
        metrics_df.write \
            .format("delta") \
            .mode("append") \
            .save(output_path)
        
        print(f"\n💾 Quality metrics saved to: {output_path}")
        return output_path


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run Data Quality Checks on Bronze Layer
# Load bronze data
bronze_path = "Files/superstore_bronze/raw_data"

bronze_df = spark.read.format("delta").load(bronze_path)

dq = BronzeDataQuality(bronze_df, "superstore_bronze")
dq.check_row_count(min_expected=1000)
dq.check_null_counts(critical_columns=['Row_ID', 'Order_ID'])
dq.check_duplicates(key_columns=['Row_ID'])
quality_results = dq.generate_report()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quality Alert System
def check_quality_thresholds(quality_results):
    """Check if quality metrics meet thresholds"""
    alerts = []
    
    # Check row count
    if quality_results.get('row_count', {}).get('status') == 'FAIL':
        alerts.append({
            'severity': 'CRITICAL',
            'check': 'Row Count',
            'message': 'Dataset has fewer rows than expected'
        })
    
    # Check for high null percentages
    null_analysis = quality_results.get('null_analysis', [])
    for col_info in null_analysis:
        if col_info['null_percentage'] > 50:
            alerts.append({
                'severity': 'WARNING',
                'check': 'Null Values',
                'message': f"Column '{col_info['column']}' has {col_info['null_percentage']}% nulls"
            })
    
    # Check duplicates
    if quality_results.get('duplicates', {}).get('status') == 'WARNING':
        alerts.append({
            'severity': 'WARNING',
            'check': 'Duplicates',
            'message': f"Found {quality_results['duplicates']['duplicate_groups']} duplicate groups"
        })
    
    # Display alerts
    if alerts:
        print("\n🚨 QUALITY ALERTS:")
        print("="*70)
        for alert in alerts:
            print(f"[{alert['severity']}] {alert['check']}: {alert['message']}")
    else:
        print("\n✅ No quality alerts - all checks passed!")
    
    return alerts

# Run alert checks
alerts = check_quality_thresholds(quality_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
