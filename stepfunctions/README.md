# NusaCommerce Pipeline Orchestrator — Step Functions

State machine definition for the NusaCommerce batch pipeline.

## File

| File | Description |
|------|-------------|
| `pipeline_orchestrator.json` | State machine definition (contains intentional bugs — must be fixed) |

## Required Input

The state machine expects the following JSON input when executed:

```json
{
  "bucket": "nusa-raw-data-{accountid}",
  "key": "transactions/transactions.csv",
  "timestamp": "2026-05-25T10:00:00Z",
  "sns_topic_arn": "arn:aws:sns:us-east-1:{accountid}:nusa-alerts",
  "processed_bucket": "nusa-processed-data-{accountid}",
  "curated_bucket": "nusa-curated-data-{accountid}",
  "emr_cluster_id": "j-XXXXXXXXXXXXX"
}
```

Replace `{accountid}` with your AWS account ID and `j-XXXXXXXXXXXXX` with your EMR cluster ID.

## Manual Test Execution

### Via AWS Console

1. Open **Step Functions** → select `nusa-pipeline-orchestrator`
2. Click **Start execution**
3. Paste the input JSON above (with your values)
4. Click **Start execution**

### Via AWS CLI

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
SNS_ARN="arn:aws:sns:us-east-1:${ACCOUNT_ID}:nusa-alerts"
EMR_CLUSTER_ID=$(aws emr list-clusters --active --region us-east-1 \
  --query "Clusters[?Name=='nusa-emr-cluster']|[0].Id" --output text)

aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:${ACCOUNT_ID}:stateMachine:nusa-pipeline-orchestrator" \
  --input "{
    \"bucket\": \"nusa-raw-data-${ACCOUNT_ID}\",
    \"key\": \"transactions/transactions.csv\",
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
    \"sns_topic_arn\": \"${SNS_ARN}\",
    \"processed_bucket\": \"nusa-processed-data-${ACCOUNT_ID}\",
    \"curated_bucket\": \"nusa-curated-data-${ACCOUNT_ID}\",
    \"emr_cluster_id\": \"${EMR_CLUSTER_ID}\"
  }" \
  --region us-east-1
```

## Pipeline States (9 total)

```
ValidateInput → ParallelETL → RunCrawler → ParallelPostProcessing → LoadMLFeatures → NotifySuccess → Success
                                                                                    ↘ NotifyFailure → Failure
```

| State | Type | Description |
|-------|------|-------------|
| ValidateInput | Task (Lambda) | Validates input fields: bucket, key, timestamp |
| ParallelETL | Parallel | Runs 3 Glue ETL jobs simultaneously |
| RunCrawler | Task (SDK) | Starts Glue Crawler `nusa-crawler-raw` |
| ParallelPostProcessing | Parallel | Branch 1: Redshift load + refresh. Branch 2: EMR ML jobs |
| LoadMLFeatures | Task (Lambda) | Loads EMR output into Redshift analytics tables |
| NotifySuccess | Task (SNS) | Publishes success notification |
| NotifyFailure | Task (SNS) | Publishes failure notification (Catch handler) |
| Success | Succeed | Terminal success state |
| Failure | Fail | Terminal failure state |

## Expected Execution Time

| Phase | Approximate Duration |
|-------|---------------------|
| ValidateInput | < 5 sec |
| ParallelETL (3 Glue jobs) | 3–8 min |
| RunCrawler | < 30 sec (async start) |
| LoadToRedshift + RefreshViews | 3–10 min |
| EMR SellerScoring + UserSegmentation | 5–15 min |
| LoadMLFeatures | 1–3 min |
| **Total** | **~15–30 min** |

## Troubleshooting

- **State machine fails to create**: Check for invalid `Next` references pointing to non-existent states
- **ParallelETL fails**: Verify Glue job names match exactly (check for typos)
- **EMR steps fail**: Verify S3 script path is correct and EMR cluster is in WAITING state
- **LoadMLFeatures fails**: Verify Lambda function name is correct
- **NotifyFailure loops**: Ensure `ResultPath` is set on Catch blocks to preserve `$.sns_topic_arn`
