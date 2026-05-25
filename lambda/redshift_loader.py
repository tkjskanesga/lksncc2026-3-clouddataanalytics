"""
Lambda Redshift Loader: nusa-redshift-loader
Executes COPY commands to load data from S3 to Redshift Cluster.
Waits for each COPY to finish before returning (prevents race condition with
view_refresher which runs immediately after in Step Functions).
"""

import json
import boto3
import os
import time
from datetime import datetime

redshift_data = boto3.client('redshift-data')


def wait_for_statement(exec_id: str, timeout_seconds: int = 240) -> None:
    """Poll Redshift Data API until statement finishes or raises on failure."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        resp   = redshift_data.describe_statement(Id=exec_id)
        status = resp['Status']
        if status == 'FINISHED':
            return
        if status in ('FAILED', 'ABORTED'):
            raise RuntimeError(f"Redshift statement {exec_id} {status}: {resp.get('Error')}")
        time.sleep(5)
    raise TimeoutError(f"Redshift statement {exec_id} timed out after {timeout_seconds}s")


def lambda_handler(event, context):
    try:
        cluster_id      = os.environ['REDSHIFT_CLUSTER_ID']
        database        = os.environ['REDSHIFT_DATABASE']
        db_user         = os.environ.get('REDSHIFT_DB_USER', 'nusa_admin')
        processed_bucket = os.environ['PROCESSED_BUCKET']
        iam_role        = os.environ['REDSHIFT_IAM_ROLE']

        tables  = ['transactions', 'shipments', 'sellers']
        results = {}

        for table in tables:
            try:
                query = f"""
                COPY staging.{table}
                FROM 's3://{processed_bucket}/{table}/'
                IAM_ROLE '{iam_role}'
                FORMAT AS PARQUET
                """
                response = redshift_data.execute_statement(
                    ClusterIdentifier=cluster_id,
                    Database=database,
                    DbUser=db_user,
                    Sql=query
                )
                exec_id = response['Id']
                print(f"COPY submitted for {table}: {exec_id}")

                # Wait for COPY to finish before returning
                wait_for_statement(exec_id)
                results[table] = {'status': 'FINISHED', 'query_id': exec_id}
                print(f"COPY completed for {table}")

            except Exception as e:
                results[table] = {'status': 'FAILED', 'error': str(e)}
                print(f"Error loading {table}: {str(e)}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'COPY commands completed',
                'results': results,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
        }
    except Exception as e:
        print(f"Error in redshift_loader: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
        }
