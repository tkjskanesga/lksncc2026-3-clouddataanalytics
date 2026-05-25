"""
Lambda Function: nusa-ml-loader
Loads EMR ML Feature Engineering results from S3 into Redshift.

Called by Step Functions state: LoadMLFeatures
Runs two COPY commands:
  1. curated/ml-features/seller-scores/ → analytics.seller_scores
  2. curated/ml-features/user-segments/ → analytics.user_segments
"""

import json
import os
import boto3
import time
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

redshift = boto3.client("redshift-data")


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return value


def execute_statement(sql: str, cluster_id: str, database: str) -> str:
    """Submit SQL to Redshift Cluster and return execution ID."""
    db_user = os.environ.get("REDSHIFT_DB_USER", "nusa_admin")
    response = redshift.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=sql,
    )
    return response["Id"]


def wait_for_statement(execution_id: str, timeout_seconds: int = 240) -> dict:
    """Poll SQL execution status until finished or timeout."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        resp = redshift.describe_statement(Id=execution_id)
        status = resp["Status"]
        if status == "FINISHED":
            return resp
        if status in ("FAILED", "ABORTED"):
            error = resp.get("Error", "Unknown error")
            raise RuntimeError(f"Redshift statement {execution_id} {status}: {error}")
        time.sleep(5)
    raise TimeoutError(f"Redshift statement {execution_id} timed out after {timeout_seconds}s")


def run_sql(sql: str, cluster_id: str, database: str, label: str) -> None:
    """Execute SQL and wait until finished."""
    logger.info(f"[{label}] Executing SQL...")
    exec_id = execute_statement(sql, cluster_id, database)
    logger.info(f"[{label}] Execution ID: {exec_id}")
    result = wait_for_statement(exec_id)
    rows = result.get("ResultRows", 0)
    logger.info(f"[{label}] Completed. Rows affected: {rows}")


# ── DDL ───────────────────────────────────────────────────────────────────────

DDL_SELLER_SCORES_DROP = "DROP TABLE IF EXISTS analytics.seller_scores;"

DDL_SELLER_SCORES_CREATE = """
CREATE TABLE analytics.seller_scores (
    seller_id        VARCHAR(256)   NOT NULL,
    seller_name      VARCHAR(256),
    tier             VARCHAR(50),
    composite_score  FLOAT8,
    gmv_score        FLOAT8,
    completion_score FLOAT8,
    rating_score     FLOAT8,
    growth_score     FLOAT8,
    segment_label    VARCHAR(50)
)
DISTSTYLE KEY
DISTKEY (seller_id)
SORTKEY (composite_score);
"""

DDL_USER_SEGMENTS_DROP = "DROP TABLE IF EXISTS analytics.user_segments;"

DDL_USER_SEGMENTS_CREATE = """
CREATE TABLE analytics.user_segments (
    user_id        VARCHAR(256)   NOT NULL,
    recency_days   BIGINT,
    frequency      BIGINT,
    monetary       FLOAT8,
    cluster_id     BIGINT,
    segment_label  VARCHAR(50)
)
DISTSTYLE KEY
DISTKEY (user_id)
SORTKEY (segment_label);
"""


# ── COPY Commands ─────────────────────────────────────────────────────────────

def build_copy_sql(table: str, s3_path: str, iam_role: str) -> str:
    return f"""
    TRUNCATE TABLE {table};
    COPY {table} (
        {get_columns(table)}
    )
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    FORMAT AS PARQUET
    SERIALIZETOJSON;
    """


def get_columns(table: str) -> str:
    cols = {
        "analytics.seller_scores": (
            "seller_id, seller_name, tier, composite_score, "
            "gmv_score, completion_score, rating_score, growth_score, segment_label"
        ),
        "analytics.user_segments": (
            "user_id, recency_days, frequency, monetary, cluster_id, segment_label"
        ),
    }
    return cols.get(table, "*")


# ── Main Handler ──────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")

    try:
        # FIX: use REDSHIFT_CLUSTER_ID consistently with other Lambda functions
        cluster_id     = get_env("REDSHIFT_CLUSTER_ID")
        database       = get_env("REDSHIFT_DATABASE")
        iam_role       = get_env("REDSHIFT_IAM_ROLE")
        curated_bucket = get_env("CURATED_BUCKET")

        seller_scores_path = f"s3://{curated_bucket}/ml-features/seller-scores/"
        user_segments_path = f"s3://{curated_bucket}/ml-features/user-segments/"

        # 1. Drop and recreate tables
        run_sql(DDL_SELLER_SCORES_DROP, cluster_id, database, "DROP:seller_scores")
        run_sql(DDL_SELLER_SCORES_CREATE, cluster_id, database, "CREATE:seller_scores")
        run_sql(DDL_USER_SEGMENTS_DROP, cluster_id, database, "DROP:user_segments")
        run_sql(DDL_USER_SEGMENTS_CREATE, cluster_id, database, "CREATE:user_segments")

        # 2. COPY seller scores
        run_sql(f"""
            COPY analytics.seller_scores
            FROM '{seller_scores_path}'
            IAM_ROLE '{iam_role}'
            FORMAT AS PARQUET;
        """, cluster_id, database, "COPY:seller_scores")

        # 3. COPY user segments
        run_sql(f"""
            COPY analytics.user_segments
            FROM '{user_segments_path}'
            IAM_ROLE '{iam_role}'
            FORMAT AS PARQUET;
        """, cluster_id, database, "COPY:user_segments")

        # 4. Verify row counts
        verify_sql = """
            SELECT 'seller_scores' AS tbl, COUNT(*) AS cnt FROM analytics.seller_scores
            UNION ALL
            SELECT 'user_segments', COUNT(*) FROM analytics.user_segments;
        """
        exec_id = execute_statement(verify_sql, cluster_id, database)
        result  = wait_for_statement(exec_id)
        logger.info(f"Verification result: {result}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "ML features loaded successfully",
                "seller_scores_path": seller_scores_path,
                "user_segments_path": user_segments_path,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }),
        }

    except Exception as e:
        logger.error(f"ml_loader failed: {str(e)}", exc_info=True)
        raise
