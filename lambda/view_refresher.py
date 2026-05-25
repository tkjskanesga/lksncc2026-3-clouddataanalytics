"""
Lambda View Refresher: nusa-view-refresher
Refreshes materialized views in Redshift Cluster.
"""

import json
import boto3
import os
import time
from datetime import datetime

redshift_data = boto3.client('redshift-data')


def wait_for_statement(exec_id: str, timeout_seconds: int = 240) -> None:
    """Poll until statement finishes."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        resp   = redshift_data.describe_statement(Id=exec_id)
        status = resp['Status']
        if status == 'FINISHED':
            return
        if status in ('FAILED', 'ABORTED'):
            raise RuntimeError(f"Statement {exec_id} {status}: {resp.get('Error')}")
        time.sleep(5)
    raise TimeoutError(f"Statement {exec_id} timed out after {timeout_seconds}s")


def lambda_handler(event, context):
    try:
        cluster_id = os.environ['REDSHIFT_CLUSTER_ID']
        database   = os.environ['REDSHIFT_DATABASE']
        db_user    = os.environ.get('REDSHIFT_DB_USER', 'nusa_admin')

        views   = ['reporting.mv_daily_summary']
        results = {}

        for view in views:
            try:
                response = redshift_data.execute_statement(
                    ClusterIdentifier=cluster_id,
                    Database=database,
                    DbUser=db_user,
                    Sql=f"REFRESH MATERIALIZED VIEW {view}"
                )
                exec_id = response['Id']
                print(f"Refresh submitted for {view}: {exec_id}")

                wait_for_statement(exec_id)
                results[view] = {'status': 'FINISHED', 'query_id': exec_id}
                print(f"Refresh completed for {view}")

            except Exception as e:
                results[view] = {'status': 'FAILED', 'error': str(e)}
                print(f"Error refreshing {view}: {str(e)}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Materialized views refresh completed',
                'results': results,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
        }

    except Exception as e:
        print(f"Error in view_refresher: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
        }
