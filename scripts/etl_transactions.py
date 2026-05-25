"""
etl_transactions.py
AWS Glue ETL Job — NusaCommerce Analytics Platform
Transform: transactions CSV → Parquet (partitioned by year/month/day)

Job Parameters:
  --JOB_NAME       : Glue job name (auto-provided by Glue)
  --input_path     : s3://nusa-raw-data-{accountid}/transactions/
  --output_path    : s3://nusa-processed-data-{accountid}/transactions/
  --database_name  : nusa-database
  --table_name     : transactions
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, to_date, to_timestamp, year, month, dayofmonth,
    trim, upper, current_timestamp, lit
)
from pyspark.sql.types import DecimalType, IntegerType

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
    .withColumn("transaction_id",  trim(col("transaction_id"))) \
    .withColumn("seller_id",       trim(col("seller_id"))) \
    .withColumn("buyer_id",        trim(col("buyer_id"))) \
    .withColumn("product_id",      trim(col("product_id"))) \
    .withColumn("category",        upper(trim(col("category")))) \
    .withColumn("province",        upper(trim(col("province")))) \
    .withColumn("payment_method",  upper(trim(col("payment_method")))) \
    .withColumn("status",          upper(trim(col("status"))))

# ---------------------------------------------------------------------------
# TRANSFORM — Step 2: Cast data types
# ---------------------------------------------------------------------------
print("[INFO] Casting data types...")
typed_df = cleaned_df \
    .withColumn("order_date",    to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn("created_at",    to_timestamp(col("created_at"))) \
    .withColumn("quantity",      col("quantity").cast(IntegerType())) \
    .withColumn("unit_price",    col("unit_price").cast(DecimalType(12, 2))) \
    .withColumn("total_amount",  col("total_amount").cast(DecimalType(12, 2)))

# ---------------------------------------------------------------------------
# TRANSFORM — Step 3: Validation — filter invalid rows
# ---------------------------------------------------------------------------
print("[INFO] Filtering invalid rows...")
VALID_STATUSES = ["COMPLETED", "CANCELLED", "PENDING", "REFUNDED"]

valid_df = typed_df \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("transaction_id") != "") \
    .filter(col("total_amount").isNotNull()) \
    .filter(col("total_amount") > 0) \
    .filter(col("status").isin(VALID_STATUSES))

valid_count = valid_df.count()
print(f"[INFO] Valid rows after filter: {valid_count} (dropped: {raw_count - valid_count})")

# ---------------------------------------------------------------------------
# TRANSFORM — Step 4: Deduplicate by transaction_id
# ---------------------------------------------------------------------------
print("[INFO] Deduplicating by transaction_id...")
deduped_df = valid_df.dropDuplicates(["transaction_id"])

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
    .withColumn("_year",  year(col("order_date"))) \
    .withColumn("_month", month(col("order_date"))) \
    .withColumn("_day",   dayofmonth(col("order_date")))

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
