import os, time, boto3, urllib.parse

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

TABLE_NAME = os.environ['TABLE_NAME']
BUCKET_SRC = os.environ['BUCKET_SRC']
BUCKET_DST = os.environ['BUCKET_DST']
GSI_SRC_STATUS = os.environ['GSI_SRC_STATUS']
MAX_ACTIVE = int(os.environ.get('MAX_ACTIVE', '3'))

table = dynamodb.Table(TABLE_NAME)

def _pad13(n: int) -> str:
    return str(n).zfill(13)

def handler(event, context):
    """
    Handles:
      - EventBridge S3 events: event['detail'] {...}
      - (Optionally) Raw S3 'Records' events (if ever sent directly)
    """
    # EventBridge
    if 'detail' in event and 'bucket' in event['detail']:
        d = event['detail']
        src_bucket = d['bucket']['name']
        key = d['object']['key']  # already URL-safe string
        # EventBridge detail-type is "Object Created" or "Object Deleted"
        detail_type = event.get('detail-type', '')
        event_name = d.get('requestParameters', {}).get('operation') or d.get('eventName', '')
        # Normalize
        if detail_type == 'Object Created':
            on_put(key, src_bucket)
        elif detail_type == 'Object Deleted':
            on_delete(key, src_bucket)
        return {'statusCode': 200, 'body': 'ok'}

    # Fallback: raw S3 events (not expected when using EventBridge)
    for rec in event.get('Records', []):
        evt = rec['eventName']  # e.g., ObjectCreated:Put / ObjectRemoved:Delete
        src_bucket = rec['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(rec['s3']['object']['key'])
        if evt.startswith('ObjectCreated'):
            on_put(key, src_bucket)
        elif evt.startswith('ObjectRemoved'):
            on_delete(key, src_bucket)

    return {'statusCode': 200, 'body': 'ok'}

def on_put(src_key: str, src_bucket: str):
    if src_bucket != BUCKET_SRC:
        return
    now_ms = int(time.time() * 1000)
    copy_key = f"{src_key}.copy.{now_ms}"

    # 1) Copy in S3
    s3.copy_object(
        Bucket=BUCKET_DST,
        CopySource={'Bucket': BUCKET_SRC, 'Key': src_key},
        Key=copy_key,
    )

    # 2) Insert row
    status = 'ACTIVE'
    table.put_item(Item={
        'srcKey': src_key,
        'createdAt': now_ms,
        'copyKey': copy_key,
        'status': status,
        'statusCreatedAt': f"{status}#{_pad13(now_ms)}",
    })

    # 3) Keep at most MAX_ACTIVE ACTIVE copies (query by GSI)
    resp = table.query(
        IndexName=GSI_SRC_STATUS,
        KeyConditionExpression="srcKey = :k AND begins_with(statusCreatedAt, :pfx)",
        ExpressionAttributeValues={":k": src_key, ":pfx": "ACTIVE#"},
        ScanIndexForward=True  # oldest first
    )
    items = resp['Items']
    excess = len(items) - MAX_ACTIVE
    for i in range(max(0, excess)):
        victim = items[i]
        try:
            s3.delete_object(Bucket=BUCKET_DST, Key=victim['copyKey'])
        except Exception:
            pass
        table.update_item(
            Key={'srcKey': victim['srcKey'], 'createdAt': victim['createdAt']},
            UpdateExpression="SET #s = :deleted, statusCreatedAt = :sc REMOVE disownedAt",
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={
                ':deleted': 'DELETED',
                ':sc': f"DELETED#{_pad13(victim['createdAt'])}",
            }
        )

def on_delete(src_key: str, src_bucket: str):
    if src_bucket != BUCKET_SRC:
        return
    now_ms = int(time.time() * 1000)
    resp = table.query(
        IndexName=GSI_SRC_STATUS,
        KeyConditionExpression="srcKey = :k AND begins_with(statusCreatedAt, :pfx)",
        ExpressionAttributeValues={":k": src_key, ":pfx": "ACTIVE#"},
        ScanIndexForward=True
    )
    for it in resp['Items']:
        table.update_item(
            Key={'srcKey': it['srcKey'], 'createdAt': it['createdAt']},
            UpdateExpression="SET #s = :dis, disownedAt = :t, statusCreatedAt = :sc",
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={
                ':dis': 'DISOWNED',
                ':t': now_ms,
                ':sc': f"DISOWNED#{_pad13(it['createdAt'])}",
            }
        )
