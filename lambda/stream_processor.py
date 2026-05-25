"""
Lambda Stream Processor: nusa-stream-processor
Processes Kinesis events and aggregates metrics to DynamoDB
"""

import base64
import json
import boto3
import os
from datetime import datetime
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])

def lambda_handler(event, context):
    """
    Process Kinesis records and aggregate metrics to DynamoDB
    """
    try:
        for record in event['Records']:
            # Decode Kinesis data (base64-encoded by AWS)
            payload = json.loads(
                base64.b64decode(record['kinesis']['data']).decode('utf-8')
            )
            
            # Extract event details
            event_type = payload.get('event_type', 'UNKNOWN')
            timestamp = payload.get('event_timestamp', datetime.utcnow().isoformat())
            amount = Decimal(str(payload.get('amount', 0)))
            
            # Parse timestamp to get date and hour
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            date_str = dt.strftime('%Y-%m-%d')
            hour_str = dt.strftime('%H')
            
            # Aggregate metrics based on event type
            if event_type == 'PURCHASE':
                # Update GMV
                update_metric(date_str, hour_str, 'GMV', amount)
                # Update ORDER_COUNT
                update_metric(date_str, hour_str, 'ORDER_COUNT', Decimal('1'))
            
            # Update ACTIVE_USERS
            user_id = payload.get('user_id', 'unknown')
            update_active_users(date_str, hour_str, user_id)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Metrics aggregated successfully')
        }
    
    except Exception as e:
        print(f"Error processing record: {str(e)}")
        raise

def update_metric(date_str, hour_str, metric_type, value):
    """
    Update metric in DynamoDB using atomic ADD operation
    """
    pk = f"METRIC#{date_str}#{hour_str}"
    sk = metric_type
    ttl = int(datetime.utcnow().timestamp()) + (48 * 3600)  # 48 hour TTL
    
    try:
        table.update_item(
            Key={'pk': pk, 'sk': sk},
            UpdateExpression='ADD #v :val SET #lu = :ts, #ttl = :ttl_val',
            ExpressionAttributeNames={
                '#v': 'value',
                '#lu': 'last_updated',
                '#ttl': 'ttl'
            },
            ExpressionAttributeValues={
                ':val': value,
                ':ts': datetime.utcnow().isoformat() + 'Z',
                ':ttl_val': ttl
            }
        )
    except Exception as e:
        print(f"Error updating metric {metric_type}: {str(e)}")
        raise

def update_active_users(date_str, hour_str, user_id):
    """
    Track active users using a set-like approach
    """
    pk = f"METRIC#{date_str}#{hour_str}"
    sk = 'ACTIVE_USERS'
    ttl = int(datetime.utcnow().timestamp()) + (48 * 3600)
    
    try:
        # For simplicity, increment counter (in production, use HyperLogLog or similar)
        table.update_item(
            Key={'pk': pk, 'sk': sk},
            UpdateExpression='ADD #v :val SET #lu = :ts, #ttl = :ttl_val',
            ExpressionAttributeNames={
                '#v': 'value',
                '#lu': 'last_updated',
                '#ttl': 'ttl'
            },
            ExpressionAttributeValues={
                ':val': Decimal('1'),
                ':ts': datetime.utcnow().isoformat() + 'Z',
                ':ttl_val': ttl
            }
        )
    except Exception as e:
        print(f"Error updating active users: {str(e)}")
        raise
