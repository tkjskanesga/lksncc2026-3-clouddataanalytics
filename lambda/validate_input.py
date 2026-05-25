"""
Lambda: nusa-validate-input
Validates pipeline input event (bucket, key, timestamp) before
the Step Functions pipeline proceeds.

Memory: 128 MB | Timeout: 30 sec
"""

import json
from datetime import datetime


def lambda_handler(event, context):
    """
    Validate that the input event contains valid bucket, key, and timestamp fields.
    Returns validation result with statusCode.
    """
    try:
        # Extract from direct input or from Lambda invoke wrapper
        payload = event
        if 'Payload' in event:
            payload = event['Payload']

        bucket    = payload.get('bucket')
        key       = payload.get('key')
        timestamp = payload.get('timestamp')
        errors    = []

        # Required field checks
        if not bucket:
            errors.append('Missing required field: bucket')
        if not key:
            errors.append('Missing required field: key')
        if not timestamp:
            errors.append('Missing required field: timestamp')

        # Bucket name validation
        if bucket and not bucket.startswith('nusa-'):
            errors.append(f'Invalid bucket name: must start with nusa- (got: {bucket})')

        # Key prefix validation
        if key:
            valid_prefixes = ['transactions/', 'shipments/', 'sellers/']
            if not any(key.startswith(p) for p in valid_prefixes):
                errors.append(f'Invalid key prefix: must start with transactions/, shipments/, or sellers/ (got: {key})')

        # Timestamp format validation
        if timestamp:
            try:
                # Try ISO 8601 format
                datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                errors.append(f'Invalid timestamp format: {timestamp}')

        if errors:
            return {
                'statusCode': 400,
                'valid': False,
                'errors': errors,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }

        return {
            'statusCode': 200,
            'valid': True,
            'bucket': bucket,
            'key': key,
            'timestamp': timestamp,
            'validated_at': datetime.utcnow().isoformat() + 'Z'
        }

    except Exception as e:
        print(f"validate_input error: {e}")
        return {
            'statusCode': 500,
            'valid': False,
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
