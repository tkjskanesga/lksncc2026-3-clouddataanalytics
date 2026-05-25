# NusaCommerce Analytics Platform

**LKS National Competition 2026 — Cloud Computing**

End-to-End Cloud Data & Analytics Pipeline for E-Commerce Intelligence on AWS.

![Architecture](arsitektur%20diagram.png)

---

## Project Structure

```
├── data/                   # CSV datasets (transactions, user_events, shipments, sellers)
├── lambda/                 # 6 Lambda function source code (Python 3.12)
├── scripts/                # Glue ETL scripts + EMR PySpark ML scripts
├── stepfunctions/          # Step Functions state machine definition (JSON)
├── frontend/               # Analytics dashboard (static HTML)
├── sql/                    # Athena queries + Redshift setup SQL
└── NusaCommerce_LKS_Module_Final.docx  # Competition module document
```

---

## Quick Start

- **Platform**: AWS Academy Learner Lab
- **Region**: `us-east-1`
- **IAM Role**: `LabRole` (do not create new roles)
- **Prefix**: All resources must use `nusa-` prefix
- **Duration**: 5 hours

Refer to `NusaCommerce_LKS_Module_Final.docx` for complete task instructions.

---

## API Endpoints

Base URL: `https://{api-id}.execute-api.us-east-1.amazonaws.com/prod`

All endpoints (except `/health`) require header `x-api-key: {your-api-key}`.

### Core Analytics

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check (no auth required) |
| GET | `/metrics/realtime?date=YYYY-MM-DD` | Real-time GMV, orders, active users from DynamoDB |
| GET | `/analytics/sales?start_date=...&end_date=...&group_by=category` | Sales report from Redshift |
| GET | `/analytics/funnel?date=YYYY-MM-DD` | Conversion funnel from Athena |
| GET | `/analytics/recommendations?type=sellers&limit=20` | ML seller scores from Redshift |
| GET | `/analytics/recommendations?type=users&limit=20` | ML user segments from Redshift |

### Pipeline Control

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/pipeline/execute` | Start a new Step Functions execution |
| GET | `/pipeline/status?executionArn=...` | Get live execution status with node states |
| GET | `/pipeline/executions?limit=10` | List recent pipeline executions |

---

## Manual API Testing (curl)

Replace `{base}` with your API Gateway URL and `{key}` with your API key.

```bash
# Health check (no auth)
curl https://{base}/health

# Real-time metrics
curl -H "x-api-key: {key}" "https://{base}/metrics/realtime?date=2024-06-15"

# Sales by category
curl -H "x-api-key: {key}" "https://{base}/analytics/sales?start_date=2024-01-01&end_date=2024-12-31&group_by=category"

# Sales by province
curl -H "x-api-key: {key}" "https://{base}/analytics/sales?start_date=2024-01-01&end_date=2024-12-31&group_by=province"

# Conversion funnel
curl -H "x-api-key: {key}" "https://{base}/analytics/funnel?date=2024-06-15"

# Seller recommendations (ML)
curl -H "x-api-key: {key}" "https://{base}/analytics/recommendations?type=sellers&limit=10"

# User segments (ML)
curl -H "x-api-key: {key}" "https://{base}/analytics/recommendations?type=users&limit=10"

# Filter by segment
curl -H "x-api-key: {key}" "https://{base}/analytics/recommendations?type=sellers&segment=TOP"
curl -H "x-api-key: {key}" "https://{base}/analytics/recommendations?type=users&segment=CHAMPION"

# Start pipeline execution
curl -X POST -H "x-api-key: {key}" -H "Content-Type: application/json" \
  -d '{"key":"transactions/transactions.csv","emr_cluster_id":"j-XXXXX"}' \
  "https://{base}/pipeline/execute"

# Check pipeline status (use executionArn from above response)
curl -H "x-api-key: {key}" "https://{base}/pipeline/status?executionArn=arn:aws:states:us-east-1:123456789012:execution:nusa-pipeline-orchestrator:abc-123"

# List recent executions
curl -H "x-api-key: {key}" "https://{base}/pipeline/executions?limit=5"
```

---

## Expected API Responses

### GET /health
```json
{"status": "healthy", "timestamp": "2026-05-25T10:00:00Z", "service": "nusa-analytics-api", "version": "2.0.0"}
```

### GET /metrics/realtime
```json
{"date": "2024-06-15", "hour": "10", "gmv": 15000000, "order_count": 42, "active_users": 18, "timestamp": "..."}
```

### GET /analytics/sales
```json
{"start_date": "2024-01-01", "end_date": "2024-12-31", "group_by": "category", "results": [{"dimension": "ELECTRONICS", "order_count": 1200, "gmv": 5000000, "aov": 4166.67}], "timestamp": "..."}
```

### GET /analytics/funnel
```json
{"date": "2024-06-15", "platform": "ALL", "funnel": {"page_views": 5000, "add_to_cart": 1200, "checkouts": 400, "purchases": 150, "view_to_cart_pct": 24.0, "cart_to_checkout_pct": 33.33, "checkout_to_purchase_pct": 37.5, "overall_conversion_pct": 3.0}, "timestamp": "..."}
```

### POST /pipeline/execute
```json
{"executionArn": "arn:aws:states:us-east-1:123456789012:execution:nusa-pipeline-orchestrator:abc-123", "startDate": "2026-05-25T10:00:00Z", "status": "RUNNING"}
```

### GET /pipeline/status
```json
{"executionArn": "...", "status": "RUNNING", "nodeStates": {"pn-validate": "success", "pn-etl-txn": "running", "pn-etl-ship": "running", "pn-etl-sell": "running"}, "currentState": "ParallelETL"}
```

---

## AWS Services Used

| Layer | Services |
|-------|----------|
| Ingestion | S3, Kinesis Data Streams, Kinesis Firehose |
| Processing | Glue ETL, Glue Crawler, EMR (Spark), Step Functions, EventBridge |
| Storage | Redshift, DynamoDB, S3 (data lake), Secrets Manager |
| Serving | API Gateway, Lambda, SQS (DLQ), SNS, CloudWatch |
| Security | WAF, Secrets Manager, API Key authentication |
| Frontend | AWS Amplify |
