"""
Lambda API Handler: nusa-api-handler
Handles REST API requests for analytics endpoints.

Endpoints:
  GET /health                   — health check (no auth)
  GET /metrics/realtime         — real-time GMV from DynamoDB
  GET /analytics/sales          — sales report from Redshift
  GET /analytics/funnel         — conversion funnel from Athena
  GET /analytics/recommendations — seller scores & user segments from Redshift

Environment variables:
  DYNAMODB_TABLE      — nusa-realtime-metrics
  REDSHIFT_WORKGROUP  — nusa-warehouse
  REDSHIFT_DATABASE   — nusacommerce
  RAW_BUCKET          — nusa-raw-data-{accountid}
  PROCESSED_BUCKET    — nusa-processed-data-{accountid}
  SNS_TOPIC_ARN       — ARN of SNS topic nusa-alerts
"""

import json
import boto3
import os
import time
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
redshift = boto3.client('redshift-data')
athena   = boto3.client('athena')
s3       = boto3.client('s3')
secretsmanager = boto3.client('secretsmanager')

# Cache Redshift credentials to avoid calling Secrets Manager on every request
_redshift_creds_cache = None

def get_redshift_creds() -> dict:
    """Retrieve Redshift credentials from Secrets Manager (Task 22)."""
    global _redshift_creds_cache
    if _redshift_creds_cache:
        return _redshift_creds_cache
    secret = secretsmanager.get_secret_value(SecretId='nusa/redshift/credentials')
    _redshift_creds_cache = json.loads(secret['SecretString'])
    return _redshift_creds_cache


def lambda_handler(event, context):
    """Route API requests to appropriate handlers."""
    try:
        path         = event.get('path', '')
        http_method  = event.get('httpMethod', 'GET')
        query_params = event.get('queryStringParameters') or {}

        logger.info(f"Request: {http_method} {path} params={query_params}")

        if path == '/health':
            return handle_health()
        elif path == '/metrics/realtime':
            return handle_realtime_metrics(query_params)
        elif path == '/analytics/sales':
            return handle_sales_analytics(query_params)
        elif path == '/analytics/funnel':
            return handle_funnel_analytics(query_params)
        elif path == '/analytics/recommendations':
            return handle_recommendations(query_params)
        elif path == '/pipeline/execute' and http_method == 'POST':
            body = json.loads(event.get('body') or '{}')
            return handle_pipeline_execute(body)
        elif path == '/pipeline/status':
            return handle_pipeline_status(query_params)
        elif path == '/pipeline/executions':
            return handle_pipeline_executions(query_params)
        else:
            return error_response(404, f'Endpoint not found: {path}')

    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}", exc_info=True)
        return error_response(500, f'Internal server error: {str(e)}')

def handle_health():
    """Health check endpoint — no auth required."""
    return success_response({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'service': 'nusa-analytics-api',
        'version': '2.0.0',
    })


def handle_realtime_metrics(params):
    """GET /metrics/realtime — read from DynamoDB."""
    try:
        date = params.get('date', datetime.utcnow().strftime('%Y-%m-%d'))
        hour = params.get('hour', datetime.utcnow().strftime('%H'))

        table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])
        pk    = f"METRIC#{date}#{hour}"

        response = table.query(
            KeyConditionExpression=Key('pk').eq(pk),
        )

        metrics = {}
        for item in response.get('Items', []):
            sk    = item.get('sk', '').lower()
            value = item.get('value', 0)
            metrics[sk] = int(value) if isinstance(value, Decimal) else value

        return success_response({
            'date':         date,
            'hour':         hour,
            'gmv':          metrics.get('gmv', 0),
            'order_count':  metrics.get('order_count', 0),
            'active_users': metrics.get('active_users', 0),
            'timestamp':    datetime.utcnow().isoformat() + 'Z',
        })

    except Exception as e:
        logger.error(f"handle_realtime_metrics error: {e}", exc_info=True)
        return error_response(500, str(e))


def handle_sales_analytics(params):
    """GET /analytics/sales — query Redshift mv_daily_summary."""
    try:
        start_date = params.get('start_date',
                                (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date   = params.get('end_date', datetime.utcnow().strftime('%Y-%m-%d'))
        group_by   = params.get('group_by', 'category')

        # Map group_by to actual column names in mv_daily_summary
        # mv_daily_summary columns: order_date, category, buyer_province, payment_method,
        #   order_count, unique_buyers, gmv, aov, last_updated
        col_map = {'category': 'category', 'province': 'buyer_province', 'buyer_province': 'buyer_province'}
        if group_by not in {'category', 'province', 'buyer_province'}:
            return error_response(400, f'group_by must be one of: category, province')

        actual_col = col_map.get(group_by, 'category')

        if group_by == 'seller_id':
            # seller_id not in mv_daily_summary — query staging directly
            sql = f"""
                SELECT seller_id AS dimension,
                    COUNT(DISTINCT transaction_id) AS order_count,
                    SUM(total_amount) AS gmv,
                    AVG(total_amount) AS aov
                FROM staging.transactions
                WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
                  AND status = 'COMPLETED'
                GROUP BY seller_id ORDER BY gmv DESC LIMIT 50;
            """
        else:
            sql = f"""
                SELECT {actual_col} AS dimension,
                    SUM(order_count) AS order_count,
                    SUM(gmv) AS gmv,
                    AVG(aov) AS aov
                FROM reporting.mv_daily_summary
                WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY {actual_col}
                ORDER BY gmv DESC LIMIT 50;
            """

        rows = run_redshift_query(sql)
        return success_response({
            'start_date': start_date,
            'end_date':   end_date,
            'group_by':   group_by,
            'results':    rows,
            'timestamp':  datetime.utcnow().isoformat() + 'Z',
        })

    except Exception as e:
        logger.error(f"handle_sales_analytics error: {e}", exc_info=True)
        return error_response(500, str(e))


def handle_funnel_analytics(params):
    """GET /analytics/funnel — run Athena named query nusa-funnel-analysis."""
    try:
        date     = params.get('date', datetime.utcnow().strftime('%Y-%m-%d'))
        platform = params.get('platform', 'ALL')

        workgroup   = os.environ.get('ATHENA_WORKGROUP', 'nusa-workgroup')
        curated_bucket = os.environ['CURATED_BUCKET']
        output_loc  = f"s3://{curated_bucket}/athena-results/"

        sql = f"""
            SELECT
                event_type,
                COUNT(DISTINCT user_id)  AS users,
                COUNT(*)                 AS events
            FROM "nusa-database"."user_events"
            WHERE substr(event_timestamp, 1, 10) = '{date}'
              {f"AND platform = '{platform}'" if platform != 'ALL' else ''}
            GROUP BY event_type
            ORDER BY
                CASE event_type
                    WHEN 'PAGE_VIEW'    THEN 1
                    WHEN 'ADD_TO_CART'  THEN 2
                    WHEN 'CHECKOUT'     THEN 3
                    WHEN 'PURCHASE'     THEN 4
                    ELSE 5
                END;
        """

        exec_id = athena.start_query_execution(
            QueryString=sql,
            WorkGroup=workgroup,
            ResultConfiguration={'OutputLocation': output_loc},
        )['QueryExecutionId']

        rows = wait_athena_and_fetch(exec_id)

        # Calculate conversion rates
        funnel = build_funnel(rows)

        return success_response({
            'date':      date,
            'platform':  platform,
            'funnel':    funnel,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        })

    except Exception as e:
        logger.error(f"handle_funnel_analytics error: {e}", exc_info=True)
        return error_response(500, str(e))


def handle_recommendations(params):
    """GET /analytics/recommendations — seller scores & user segments from Redshift."""
    try:
        rec_type = params.get('type', 'sellers')   # 'sellers' | 'users'
        limit    = min(int(params.get('limit', 20)), 100)
        segment  = params.get('segment')           # filter opsional

        if rec_type == 'sellers':
            where = f"WHERE segment_label = '{segment}'" if segment else ""
            sql = f"""
                SELECT
                    seller_id,
                    seller_name,
                    tier,
                    composite_score,
                    gmv_score,
                    completion_score,
                    rating_score,
                    growth_score,
                    segment_label
                FROM analytics.seller_scores
                {where}
                ORDER BY composite_score DESC
                LIMIT {limit};
            """
        elif rec_type == 'users':
            where = f"WHERE segment_label = '{segment}'" if segment else ""
            sql = f"""
                SELECT
                    user_id,
                    recency_days,
                    frequency,
                    monetary,
                    cluster_id,
                    segment_label
                FROM analytics.user_segments
                {where}
                ORDER BY monetary DESC
                LIMIT {limit};
            """
        else:
            return error_response(400, "type must be 'sellers' or 'users'")

        rows = run_redshift_query(sql)

        # Hitung summary per segmen
        summary_sql = (
            "SELECT segment_label, COUNT(*) AS count, "
            "AVG(composite_score) AS avg_score "
            "FROM analytics.seller_scores GROUP BY segment_label ORDER BY avg_score DESC;"
            if rec_type == 'sellers' else
            "SELECT segment_label, COUNT(*) AS count, "
            "AVG(monetary) AS avg_monetary "
            "FROM analytics.user_segments GROUP BY segment_label ORDER BY avg_monetary DESC;"
        )
        summary = run_redshift_query(summary_sql)

        return success_response({
            'type':      rec_type,
            'segment':   segment,
            'count':     len(rows),
            'results':   rows,
            'summary':   summary,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        })

    except Exception as e:
        logger.error(f"handle_recommendations error: {e}", exc_info=True)
        return error_response(500, str(e))


# ── Pipeline (Step Functions) Handlers ─────────────────────────────────────────

sfn_client = boto3.client('stepfunctions')

def handle_pipeline_execute(body):
    """POST /pipeline/execute — Start a new Step Functions execution."""
    try:
        account_id = boto3.client('sts').get_caller_identity()['Account']
        region = os.environ.get('AWS_REGION', 'us-east-1')
        sm_arn = f"arn:aws:states:{region}:{account_id}:stateMachine:nusa-pipeline-orchestrator"

        raw_bucket = os.environ.get('RAW_BUCKET', f'nusa-raw-data-{account_id}')
        processed_bucket = os.environ.get('PROCESSED_BUCKET', f'nusa-processed-data-{account_id}')
        curated_bucket = os.environ.get('CURATED_BUCKET', f'nusa-curated-data-{account_id}')
        sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', f'arn:aws:sns:{region}:{account_id}:nusa-alerts')
        emr_cluster_id = body.get('emr_cluster_id', 'none')

        sfn_input = json.dumps({
            'bucket': raw_bucket,
            'key': body.get('key', 'transactions/transactions.csv'),
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'sns_topic_arn': sns_topic_arn,
            'processed_bucket': processed_bucket,
            'curated_bucket': curated_bucket,
            'emr_cluster_id': emr_cluster_id,
        })

        resp = sfn_client.start_execution(
            stateMachineArn=sm_arn,
            input=sfn_input,
        )

        return success_response({
            'executionArn': resp['executionArn'],
            'startDate': resp['startDate'].isoformat(),
            'status': 'RUNNING',
        })

    except Exception as e:
        logger.error(f"handle_pipeline_execute error: {e}", exc_info=True)
        return error_response(500, str(e))


def handle_pipeline_status(params):
    """GET /pipeline/status?executionArn=... — Get execution status with state details."""
    try:
        execution_arn = params.get('executionArn', '')
        if not execution_arn:
            return error_response(400, 'Missing required parameter: executionArn')

        resp = sfn_client.describe_execution(executionArn=execution_arn)
        status = resp['status']

        # Get execution history to determine which states completed
        history_resp = sfn_client.get_execution_history(
            executionArn=execution_arn,
            maxResults=100,
            reverseOrder=False,
        )

        states = {}
        current_state = None
        for event in history_resp.get('events', []):
            etype = event['type']
            if etype == 'TaskStateEntered' or etype == 'ParallelStateEntered':
                name = event.get('stateEnteredEventDetails', {}).get('name', '')
                states[name] = 'running'
                current_state = name
            elif etype == 'TaskStateExited' or etype == 'ParallelStateExited':
                name = event.get('stateExitedEventDetails', {}).get('name', '')
                states[name] = 'success'
            elif etype in ('TaskFailed', 'ExecutionFailed'):
                if current_state:
                    states[current_state] = 'failed'

        # Map state names to frontend node IDs
        state_to_node = {
            'ValidateInput': 'pn-validate',
            'ParallelETL': 'pn-parallel-etl',
            'ETLTransactions': 'pn-etl-txn',
            'ETLShipments': 'pn-etl-ship',
            'ETLSellers': 'pn-etl-sell',
            'RunCrawler': 'pn-crawler',
            'ParallelPostProcessing': 'pn-parallel-post',
            'LoadToRedshift': 'pn-redshift',
            'RefreshViews': 'pn-refresh',
            'RunEMRSellerScoring': 'pn-emr-seller',
            'RunEMRUserSegmentation': 'pn-emr-user',
            'LoadMLFeatures': 'pn-ml-load',
            'NotifySuccess': 'pn-success',
            'NotifyFailure': 'pn-failure',
            'Success': 'pn-success',
            'Failure': 'pn-failure',
        }

        node_states = {}
        for state_name, state_status in states.items():
            node_id = state_to_node.get(state_name)
            if node_id:
                node_states[node_id] = state_status

        return success_response({
            'executionArn': execution_arn,
            'status': status,
            'startDate': resp['startDate'].isoformat(),
            'stopDate': resp.get('stopDate', datetime.utcnow()).isoformat() if status != 'RUNNING' else None,
            'nodeStates': node_states,
            'currentState': current_state,
            'stateDetails': states,
        })

    except Exception as e:
        logger.error(f"handle_pipeline_status error: {e}", exc_info=True)
        return error_response(500, str(e))


def handle_pipeline_executions(params):
    """GET /pipeline/executions — List recent executions."""
    try:
        account_id = boto3.client('sts').get_caller_identity()['Account']
        region = os.environ.get('AWS_REGION', 'us-east-1')
        sm_arn = f"arn:aws:states:{region}:{account_id}:stateMachine:nusa-pipeline-orchestrator"

        limit = min(int(params.get('limit', '10')), 20)

        resp = sfn_client.list_executions(
            stateMachineArn=sm_arn,
            maxResults=limit,
        )

        executions = []
        for ex in resp.get('executions', []):
            executions.append({
                'executionArn': ex['executionArn'],
                'name': ex['name'],
                'status': ex['status'],
                'startDate': ex['startDate'].isoformat(),
                'stopDate': ex.get('stopDate', '').isoformat() if ex.get('stopDate') else None,
            })

        return success_response({
            'executions': executions,
            'count': len(executions),
        })

    except Exception as e:
        logger.error(f"handle_pipeline_executions error: {e}", exc_info=True)
        return error_response(500, str(e))


# ── Redshift Helper ───────────────────────────────────────────────────────────

def run_redshift_query(sql: str) -> list:
    """Execute SQL on Redshift Cluster and return rows as list of dict."""
    cluster_id = os.environ['REDSHIFT_CLUSTER_ID']
    database   = os.environ['REDSHIFT_DATABASE']
    try:
        creds   = get_redshift_creds()
        db_user = creds.get('username', 'nusa_admin')
    except Exception:
        db_user = os.environ.get('REDSHIFT_DB_USER', 'nusa_admin')

    resp = redshift.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=sql,
    )
    exec_id = resp['Id']

    # Poll until finished (max 25 sec to stay within Lambda 29 sec timeout)
    deadline = __import__('time').time() + 25
    while __import__('time').time() < deadline:
        status_resp = redshift.describe_statement(Id=exec_id)
        status = status_resp['Status']
        if status == 'FINISHED':
            break
        if status in ('FAILED', 'ABORTED'):
            raise RuntimeError(f"Redshift query failed: {status_resp.get('Error')}")
        __import__('time').sleep(1)
    else:
        raise TimeoutError("Redshift query timed out")

    result = redshift.get_statement_result(Id=exec_id)
    columns = [col['label'] for col in result.get('ColumnMetadata', [])]
    rows = []
    for record in result.get('Records', []):
        row = {}
        for col, field in zip(columns, record):
            val = list(field.values())[0] if field else None
            row[col] = val
        rows.append(row)
    return rows


# ── Athena Helper ─────────────────────────────────────────────────────────────

def wait_athena_and_fetch(exec_id: str) -> list:
    """Poll Athena query until finished and return rows."""
    deadline = __import__('time').time() + 25
    while __import__('time').time() < deadline:
        resp   = athena.get_query_execution(QueryExecutionId=exec_id)
        state  = resp['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            break
        if state in ('FAILED', 'CANCELLED'):
            reason = resp['QueryExecution']['Status'].get('StateChangeReason', '')
            raise RuntimeError(f"Athena query {state}: {reason}")
        __import__('time').sleep(1)
    else:
        raise TimeoutError("Athena query timed out")

    result  = athena.get_query_results(QueryExecutionId=exec_id)
    headers = [col['Label'] for col in
               result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    rows = []
    for row in result['ResultSet']['Rows'][1:]:   # skip header row
        rows.append({h: d.get('VarCharValue', '')
                     for h, d in zip(headers, row['Data'])})
    return rows


def build_funnel(rows: list) -> dict:
    """Convert Athena rows to funnel structure with conversion rates."""
    counts = {r['event_type']: int(r.get('users', 0)) for r in rows}
    pv  = counts.get('PAGE_VIEW',    0)
    atc = counts.get('ADD_TO_CART',  0)
    co  = counts.get('CHECKOUT',     0)
    pu  = counts.get('PURCHASE',     0)

    def pct(num, den):
        return round(num / den * 100, 2) if den > 0 else 0.0

    return {
        'page_views':                  pv,
        'add_to_cart':                 atc,
        'checkouts':                   co,
        'purchases':                   pu,
        'view_to_cart_pct':            pct(atc, pv),
        'cart_to_checkout_pct':        pct(co,  atc),
        'checkout_to_purchase_pct':    pct(pu,  co),
        'overall_conversion_pct':      pct(pu,  pv),
    }


# ── Response Helpers ──────────────────────────────────────────────────────────

def success_response(body: dict) -> dict:
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(body, default=str),
    }


def error_response(status_code: int, message: str) -> dict:
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({
            'error':     message,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        }),
    }
