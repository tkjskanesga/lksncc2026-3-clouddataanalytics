"""
etl_shipments.py
AWS Glue ETL Job — NusaCommerce Analytics Platform
Transform: shipments CSV → Parquet (partitioned by year/month/day)

Job Parameters:
  --JOB_NAME       : Glue job name (auto-provided by Glue)
  --input_path     : s3://nusa-raw-data-{accountid}/shipments/
  --output_path    : s3://nusa-processed-data-{accountid}/shipments/
  --database_name  : nusa-database
  --table_name     : shipments
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth,
    trim, upper, current_timestamp, lit
)
from pyspark.sql.types import DecimalType

# ---------------------------------------------------------------------------
# Initialize Glue Job
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'input_path',
    'output_path',
    'database_name',
    'table_name'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"[INFO] Starting ETL job: {args['JOB_NAME']}")
print(f"[INFO] Input  : {args['input_path']}")
print(f"[INFO] Output : {args['output_path']}")

# ---------------------------------------------------------------------------
# EXTRACT — Read CSV from S3
# ---------------------------------------------------------------------------
print("[INFO] Reading CSV data from S3...")
raw_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("multiLine", "false") \
    .option("encoding", "UTF-8") \
    .csv(args['input_path'])

raw_count = raw_df.count()
print(f"[INFO] Total raw rows: {raw_count}")

# ---------------------------------------------------------------------------
# TRANSFORM — Step 1: Clean whitespace & standardize strings
# ---------------------------------------------------------------------------
print("[INFO] Cleaning whitespace and standardizing strings...")
cleaned_df = raw_df \
    .withColumn("shipment_id",      trim(col("shipment_id"))) \
    .withColumn("transaction_id",   trim(col("transaction_id"))) \
    .withColumn("courier",          upper(trim(col("courier")))) \
    .withColumn("origin_province",  upper(trim(col("origin_province")))) \
    .withColumn("dest_province",    upper(trim(col("dest_province")))) \
    .withColumn("status",           upper(trim(col("status"))))

# ---------------------------------------------------------------------------
# TRANSFORM — Step 2: Cast data types
# ---------------------------------------------------------------------------
print("[INFO] Casting data types...")
typed_df = cleaned_df \
    .withColumn("pickup_date",         to_date(col("pickup_date"), "yyyy-MM-dd")) \
    .withColumn("estimated_arrival",   to_date(col("estimated_arrival"), "yyyy-MM-dd")) \
    .withColumn("actual_arrival",      to_date(col("actual_arrival"), "yyyy-MM-dd")) \
    .withColumn("weight_kg",           col("weight_kg").cast(DecimalType(6, 2))) \
    .withColumn("shipping_cost",       col("shipping_cost").cast(DecimalType(10, 2)))

# ---------------------------------------------------------------------------
# TRANSFORM — Step 3: Validation — filter invalid rows
# ---------------------------------------------------------------------------
print("[INFO] Filtering invalid rows...")
VALID_STATUSES = ["PICKED_UP", "IN_TRANSIT", "DELIVERED", "RETURNED"]

valid_df = typed_df \
    .filter(col("shipment_id").isNotNull()) \
    .filter(col("shipment_id") != "") \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("transaction_id") != "") \
    .filter(col("shipping_cost").isNotNull()) \
    .filter(col("shipping_cost") > 0) \
    .filter(col("status").isin(VALID_STATUSES))

valid_count = valid_df.count()
print(f"[INFO] Valid rows after filter: {valid_count} (dropped: {raw_count - valid_count})")

# ---------------------------------------------------------------------------
# TRANSFORM — Step 4: Deduplicate by shipment_id
# ---------------------------------------------------------------------------
print("[INFO] Deduplicating by shipment_id...")
deduped_df = valid_df.dropDuplicates(["shipment_id"])

deduped_count = deduped_df.count()
print(f"[INFO] Rows after deduplication: {deduped_count} (duplicates removed: {valid_count - deduped_count})")

# ---------------------------------------------------------------------------
# TRANSFORM — Step 5: Add audit columns (without partition columns)
# Partition columns (_year/_month/_day) are NOT added to the DataFrame schema.
# If they were included in the Parquet, Redshift COPY would fail because those
# columns do not exist in the staging table DDL. Partitions are only used as
# S3 folder paths.
# ---------------------------------------------------------------------------
print("[INFO] Adding audit columns...")
final_df = deduped_df \
    .withColumn("etl_timestamp", current_timestamp()) \
    .withColumn("data_source",   lit("batch_upload"))

# Compute partition values from date column (for partitionBy only, not persisted as columns)
part_df = final_df \
    .withColumn("_year",  year(col("pickup_date"))) \
    .withColumn("_month", month(col("pickup_date"))) \
    .withColumn("_day",   dayofmonth(col("pickup_date")))

# ---------------------------------------------------------------------------
# LOAD — Write Parquet to S3 with partitioning year/month/day
# Use part_df (with _year/_month/_day columns) for partitionBy
# Columns prefixed with _ are partition columns only, not loaded into staging tables
# ---------------------------------------------------------------------------
print(f"[INFO] Writing Parquet to: {args['output_path']}")
part_df.write \
    .mode("overwrite") \
    .partitionBy("_year", "_month", "_day") \
    .option("compression", "snappy") \
    .parquet(args['output_path'])

final_count = final_df.count()
print(f"[INFO] ETL complete. Total rows written: {final_count}")
print(f"[INFO] Output: {args['output_path']}")

job.commit()
print("[INFO] Job committed successfully.")
