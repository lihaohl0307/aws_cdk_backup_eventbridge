import os, time, boto3, json

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

TABLE_NAME = os.environ['TABLE_NAME']
BUCKET_DST = os.environ['BUCKET_DST']
GSI_STATUS_AGE = os.environ['GSI_STATUS_AGE']
DISOWNED_LAG_SECONDS = int(os.environ.get('DISOWNED_LAG_SECONDS', '10'))

table = dynamodb.Table(TABLE_NAME)

def _pad13(n: int) -> str:
    return str(n).zfill(13)

def handler(event, context):
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - (DISOWNED_LAG_SECONDS * 1000)

    print(json.dumps({
        "msg": "Cleaner start",
        "now_ms": now_ms,
        "cutoff": cutoff,
        "lag_seconds": DISOWNED_LAG_SECONDS,
        "table": TABLE_NAME,
        "bucket_dst": BUCKET_DST,
        "gsi": GSI_STATUS_AGE
    }))

    total_deleted = 0
    last_evaluated_key = None
    page_num = 0

    while True:
        page_num += 1
        try:
            kwargs = {
                "IndexName": GSI_STATUS_AGE,
                # Alias reserved word `status` -> `#s` to fix reserved word issue
                "KeyConditionExpression": "#s = :s AND disownedAt BETWEEN :z AND :c",
                "ExpressionAttributeNames": { "#s": "status" },
                "ExpressionAttributeValues": { ":s": "DISOWNED", ":z": 0, ":c": cutoff },
                "ScanIndexForward": True
            }
            if last_evaluated_key:
                kwargs["ExclusiveStartKey"] = last_evaluated_key

            resp = table.query(**kwargs)
            items = resp.get("Items", [])
            print(json.dumps({"msg": "Query page", "page": page_num, "count": len(items)}))

        except Exception as e:
            print(json.dumps({"msg": "Query error", "error": str(e)}))
            raise

        for it in items:
            src_key = it.get("srcKey")
            created_at = it.get("createdAt")
            copy_key = it.get("copyKey")
            print(json.dumps({"msg": "Deleting copy", "copyKey": copy_key}))

            # 1) Delete object from destination bucket
            try:
                s3.delete_object(Bucket=BUCKET_DST, Key=copy_key)
            except s3.exceptions.NoSuchKey:
                print(json.dumps({"msg": "Copy not found (already gone)", "copyKey": copy_key}))
            except Exception as e:
                print(json.dumps({"msg": "S3 delete error", "copyKey": copy_key, "error": str(e)}))
                # proceed to mark DELETED anyway to avoid getting stuck

            # 2) Mark row DELETED & clear disownedAt (so it won’t match future queries)
            try:
                table.update_item(
                    Key={"srcKey": src_key, "createdAt": created_at},
                    UpdateExpression="SET #s = :deleted, statusCreatedAt = :sc REMOVE disownedAt",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":deleted": "DELETED",
                        ":sc": f"DELETED#{_pad13(created_at)}",
                    }
                )
                total_deleted += 1
            except Exception as e:
                print(json.dumps({"msg": "DDB update error", "key": {"srcKey": src_key, "createdAt": created_at}, "error": str(e)}))

        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    print(json.dumps({"msg": "Cleaner done", "deleted": total_deleted}))
    return {"statusCode": 200, "deleted": total_deleted}
