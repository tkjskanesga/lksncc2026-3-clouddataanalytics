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

Refer to `PDF file` for complete task instructions.

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
