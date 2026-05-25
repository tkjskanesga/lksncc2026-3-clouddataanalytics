# NusaCommerce Analytics Platform — Lambda Functions

This directory contains six Lambda functions that power the NusaCommerce Analytics Platform.

---

## Deployment Summary

| Function Name | File | Runtime | Memory | Timeout |
|---------------|------|---------|--------|---------|
| `nusa-stream-processor` | `stream_processor.py` | Python 3.12 | 256 MB | 60 sec |
| `nusa-api-handler` | `api_handler.py` | Python 3.12 | 512 MB | 29 sec |
| `nusa-redshift-loader` | `redshift_loader.py` | Python 3.12 | 256 MB | 900 sec |
| `nusa-view-refresher` | `view_refresher.py` | Python 3.12 | 256 MB | 120 sec |
| `nusa-validate-input` | `validate_input.py` | Python 3.12 | 128 MB | 30 sec |
| `nusa-ml-loader` | `ml_loader.py` | Python 3.12 | 256 MB | 300 sec |

All functions use the **LabRole** IAM role and handler format `{filename}.lambda_handler`.

---

## Environment Variables

### nusa-stream-processor

| Variable | Required | Value |
|----------|----------|-------|
| `DYNAMODB_TABLE` | ✅ | `nusa-realtime-metrics` |

**Trigger**: Kinesis event source mapping on `nusa-events-stream` (batch size 100, starting position LATEST).

---

### nusa-api-handler

| Variable | Required | Value |
|----------|----------|-------|
| `DYNAMODB_TABLE` | ✅ | `nusa-realtime-metrics` |
| `REDSHIFT_CLUSTER_ID` | ✅ | `nusa-warehouse` |
| `REDSHIFT_DATABASE` | ✅ | `nusacommerce` |
| `RAW_BUCKET` | ✅ | `nusa-raw-data-{accountid}` |
| `PROCESSED_BUCKET` | ✅ | `nusa-processed-data-{accountid}` |
| `CURATED_BUCKET` | ✅ | `nusa-curated-data-{accountid}` |
| `ATHENA_WORKGROUP` | ✅ | `nusa-workgroup` |
| `SNS_TOPIC_ARN` | ✅ | `arn:aws:sns:us-east-1:{accountid}:nusa-alerts` |

**Note**: This function also retrieves Redshift credentials from Secrets Manager (`nusa/redshift/credentials`) at runtime.

---

### nusa-redshift-loader

| Variable | Required | Value |
|----------|----------|-------|
| `REDSHIFT_CLUSTER_ID` | ✅ | `nusa-warehouse` |
| `REDSHIFT_DATABASE` | ✅ | `nusacommerce` |
| `REDSHIFT_DB_USER` | ✅ | `nusa_admin` |
| `PROCESSED_BUCKET` | ✅ | `nusa-processed-data-{accountid}` |
| `REDSHIFT_IAM_ROLE` | ✅ | `arn:aws:iam::{accountid}:role/LabRole` |

**Purpose**: Runs COPY commands to load Parquet data from S3 processed bucket into Redshift staging tables (`staging.transactions`, `staging.shipments`, `staging.sellers`).

---

### nusa-view-refresher

| Variable | Required | Value |
|----------|----------|-------|
| `REDSHIFT_CLUSTER_ID` | ✅ | `nusa-warehouse` |
| `REDSHIFT_DATABASE` | ✅ | `nusacommerce` |
| `REDSHIFT_DB_USER` | ✅ | `nusa_admin` |

**Purpose**: Refreshes materialized view `reporting.mv_daily_summary` after new data is loaded.

---

### nusa-validate-input

| Variable | Required | Value |
|----------|----------|-------|
| `RAW_BUCKET` | ❌ (optional) | `nusa-raw-data-{accountid}` |

**Purpose**: Validates that the pipeline input event contains valid `bucket`, `key`, and `timestamp` fields before the Step Functions pipeline proceeds.

---

### nusa-ml-loader

| Variable | Required | Value |
|----------|----------|-------|
| `REDSHIFT_CLUSTER_ID` | ✅ | `nusa-warehouse` |
| `REDSHIFT_DATABASE` | ✅ | `nusacommerce` |
| `REDSHIFT_DB_USER` | ✅ | `nusa_admin` |
| `REDSHIFT_IAM_ROLE` | ✅ | `arn:aws:iam::{accountid}:role/LabRole` |
| `CURATED_BUCKET` | ✅ | `nusa-curated-data-{accountid}` |

**Purpose**: Loads EMR ML feature engineering output from S3 curated bucket into Redshift analytics tables (`analytics.seller_scores`, `analytics.user_segments`).

---

## Dead Letter Queues

| Function | DLQ Name | Max Receive Count |
|----------|----------|-------------------|
| `nusa-api-handler` | `nusa-api-handler-dlq` | 3 |
| `nusa-stream-processor` | `nusa-stream-processor-dlq` | 3 |

---
