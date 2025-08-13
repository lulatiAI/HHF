import os
import uuid
import json
import time
import threading
import hashlib
from datetime import datetime

import boto3
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# ========= Env & Flask =========
load_dotenv()
app = Flask(__name__)

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Buckets (uploads go to pending/; approved go to approved/)
BUCKETS = {
    "user": os.getenv("S3_BUCKET_USER"),
    "musician": os.getenv("S3_BUCKET_MUSICIAN"),
    "advertiser": os.getenv("S3_BUCKET_ADVERTISER"),
    "radio": os.getenv("S3_BUCKET_RADIO"),
}

# Infra names
DDB_TABLE = os.getenv("DYNAMODB_TABLE", "HipHopFeverFileHashes")
SNS_TOPIC_NAME = os.getenv("SNS_TOPIC_NAME", "HipHopFeverNotifications")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "HipHopFeverObjectCreatedQueue")
NOTIFY_EMAIL = os.getenv("NOTIFICATION_EMAIL")  # required

# ========= AWS Clients =========
common_kwargs = dict(
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

s3 = boto3.client("s3", **common_kwargs)
rekognition = boto3.client("rekognition", **common_kwargs)
dynamodb = boto3.client("dynamodb", **common_kwargs)
sns = boto3.client("sns", **common_kwargs)
sqs = boto3.client("sqs", **common_kwargs)

# ========= Infra Setup =========
def ensure_dynamodb_table():
    try:
        dynamodb.describe_table(TableName=DDB_TABLE)
        print(f"[INIT] DynamoDB table exists: {DDB_TABLE}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(f"[INIT] Creating DynamoDB table: {DDB_TABLE}")
            dynamodb.create_table(
                TableName=DDB_TABLE,
                KeySchema=[{"AttributeName": "file_hash", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "file_hash", "AttributeType": "S"}],
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )
            # Wait until active
            waiter = boto3.client("dynamodb", **common_kwargs).get_waiter("table_exists")
            waiter.wait(TableName=DDB_TABLE)
        else:
            raise

def ensure_sns_topic_and_subscription():
    # Create or find topic
    topic_arn = None
    next_token = None
    while True:
        args = {"NextToken": next_token} if next_token else {}
        resp = sns.list_topics(**args)
        for t in resp.get("Topics", []):
            if t["TopicArn"].split(":")[-1] == SNS_TOPIC_NAME:
                topic_arn = t["TopicArn"]
                break
        if topic_arn or "NextToken" not in resp:
            break
        next_token = resp["NextToken"]

    if not topic_arn:
        topic_arn = sns.create_topic(Name=SNS_TOPIC_NAME)["TopicArn"]
        print(f"[INIT] Created SNS topic: {SNS_TOPIC_NAME}")

    # Ensure email subscription
    if NOTIFY_EMAIL:
        subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn).get("Subscriptions", [])
        if not any(s["Protocol"] == "email" and s["Endpoint"] == NOTIFY_EMAIL for s in subs):
            sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=NOTIFY_EMAIL)
            print(f"[INIT] Subscribed {NOTIFY_EMAIL} to SNS topic (check email to confirm).")
    else:
        print("[WARN] NOTIFICATION_EMAIL not set; no emails will be sent.")
    return topic_arn

def ensure_sqs_queue():
    # Create or get queue
    resp = sqs.create_queue(
        QueueName=SQS_QUEUE_NAME,
        Attributes={"ReceiveMessageWaitTimeSeconds": "20"}  # long polling
    )
    queue_url = resp["QueueUrl"]
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
    queue_arn = attrs["Attributes"]["QueueArn"]
    print(f"[INIT] SQS queue ready: {SQS_QUEUE_NAME} ({queue_arn})")
    return queue_url, queue_arn

def ensure_queue_policy_for_s3(queue_url, queue_arn, bucket_name):
    # Allow this S3 bucket to send events to the queue
    bucket_arn = f"arn:aws:s3:::{bucket_name}"
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": f"AllowS3SendMessage-{bucket_name}",
            "Effect": "Allow",
            "Principal": {"Service": "s3.amazonaws.com"},
            "Action": "sqs:SendMessage",
            "Resource": queue_arn,
            "Condition": {"ArnEquals": {"aws:SourceArn": bucket_arn}}
        }]
    }
    # Merge with existing policy if any
    try:
        current = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["Policy"]
        ).get("Attributes", {}).get("Policy")
        if current:
            pol = json.loads(current)
            # Replace or append statement with same Sid
            sid = policy["Statement"][0]["Sid"]
            pol["Statement"] = [st for st in pol["Statement"] if st.get("Sid") != sid] + policy["Statement"]
            policy = pol
    except ClientError:
        pass

    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(policy)}
    )

def ensure_s3_event_notification(bucket_name, queue_arn):
    # Configure S3 to send ObjectCreated events (only for pending/ prefix) to SQS
    cfg = {
        "QueueConfigurations": [{
            "Id": f"{bucket_name}-objectcreated-to-{SQS_QUEUE_NAME}",
            "QueueArn": queue_arn,
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "pending/"}]}}
        }]
    }
    s3.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration=cfg
    )
    print(f"[INIT] S3 event notification set for bucket {bucket_name} → {SQS_QUEUE_NAME} (pending/ only)")

# ========= Duplicate & Moderation Helpers =========
def s3_stream_md5(bucket, key):
    """MD5 hash of the S3 object (streaming)."""
    h = hashlib.md5()
    obj = s3.get_object(Bucket=bucket, Key=key)
    # stream in chunks
    for chunk in obj["Body"].iter_chunks(chunk_size=1024 * 1024):
        if chunk:
            h.update(chunk)
    return h.hexdigest()

def ddb_is_duplicate(file_hash):
    resp = dynamodb.get_item(
        TableName=DDB_TABLE,
        Key={"file_hash": {"S": file_hash}}
    )
    return "Item" in resp

def ddb_record_hash(file_hash):
    dynamodb.put_item(
        TableName=DDB_TABLE,
        Item={"file_hash": {"S": file_hash}, "created_at": {"S": datetime.utcnow().isoformat()}}
    )

def rekogn_labels(bucket, key):
    resp = rekognition.detect_moderation_labels(
        Image={"S3Object": {"Bucket": bucket, "Name": key}},
        MinConfidence=80
    )
    return resp.get("ModerationLabels", [])

def s3_delete(bucket, key):
    s3.delete_object(Bucket=bucket, Key=key)

def s3_move(bucket, src_key, dst_key):
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": src_key}, Key=dst_key)
    s3_delete(bucket, src_key)

def sns_notify(topic_arn, bucket, key):
    if not topic_arn:
        return
    sns.publish(
        TopicArn=topic_arn,
        Subject="HipHopFever: New Accepted Upload",
        Message=f"Accepted file: s3://{bucket}/{key}"
    )

# ========= Worker (SQS Consumer) =========
def process_s3_event_record(topic_arn, record):
    s3info = record["s3"]
    bucket = s3info["bucket"]["name"]
    key = s3info["object"]["key"]

    # S3 event URL-encodes keys
    key = key.replace("+", " ")
    key = bytes(key, "utf-8").decode("utf-8")

    try:
        # 1) Duplicate check
        file_hash = s3_stream_md5(bucket, key)
        if ddb_is_duplicate(file_hash):
            s3_delete(bucket, key)
            print(f"[PROCESS] Rejected duplicate: s3://{bucket}/{key}")
            return

        # 2) Moderate
        labels = rekogn_labels(bucket, key)
        if labels:
            s3_delete(bucket, key)
            print(f"[PROCESS] Rejected by moderation: s3://{bucket}/{key} -> {labels}")
            return

        # 3) Accept → move & record & notify
        final_key = key.replace("pending/", "approved/", 1)
        s3_move(bucket, key, final_key)
        ddb_record_hash(file_hash)
        sns_notify(topic_arn, bucket, final_key)
        print(f"[PROCESS] Accepted & moved: s3://{bucket}/{final_key}")

    except ClientError as e:
        print(f"[ERROR] Processing failed for s3://{bucket}/{key}: {e}")

def sqs_worker(topic_arn, queue_url, stop_evt):
    print("[WORKER] SQS worker started.")
    while not stop_evt.is_set():
        try:
            msgs = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20
            )
            for m in msgs.get("Messages", []):
                body = json.loads(m["Body"])
                # S3 → SQS can arrive plain or via SNS wrapper; handle both
                if "Records" in body:
                    records = body["Records"]
                else:
                    # SNS wrapper
                    msg = json.loads(body.get("Message", "{}"))
                    records = msg.get("Records", [])

                for rec in records:
                    if rec.get("eventSource") == "aws:s3":
                        process_s3_event_record(topic_arn, rec)

                # delete message
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
        except ClientError as e:
            print(f"[WORKER ERROR] {e}")
        except Exception as e:
            print(f"[WORKER ERROR] {e}")
        # small sleep to avoid tight loop in edge cases
        time.sleep(1)
    print("[WORKER] Stopped.")

stop_event = threading.Event()

def bootstrap_infra_and_worker():
    # 1) Core infra
    ensure_dynamodb_table()
    topic_arn = ensure_sns_topic_and_subscription()
    queue_url, queue_arn = ensure_sqs_queue()

    # 2) Wire each bucket to queue for pending/ prefix
    for name, bucket in BUCKETS.items():
        if bucket:
            ensure_queue_policy_for_s3(queue_url, queue_arn, bucket)
            ensure_s3_event_notification(bucket, queue_arn)

    # 3) Start SQS worker thread
    worker = threading.Thread(target=sqs_worker, args=(topic_arn, queue_url, stop_event), daemon=True)
    worker.start()
    return worker

# Start background worker at import time (once per process)
_worker_thread = bootstrap_infra_and_worker()

# ========= API =========
def generate_presigned_url(bucket_name, folder, file_name, content_type):
    key = f"{folder}/{file_name}"
    url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket_name, "Key": key, "ContentType": content_type},
        ExpiresIn=3600
    )
    return url, key

@app.route("/upload/<upload_type>", methods=["POST"])
def upload_file(upload_type):
    """Return a presigned URL for clients to upload directly to S3 (pending/)."""
    if upload_type not in BUCKETS or not BUCKETS[upload_type]:
        return jsonify({"error": "Invalid upload type"}), 400

    data = request.get_json(silent=True) or {}
    file_name = data.get("file_name")
    content_type = data.get("content_type")
    if not file_name or not content_type:
        return jsonify({"error": "Missing required fields: file_name, content_type"}), 400

    unique_name = f"{uuid.uuid4()}-{file_name}"
    presigned_url, s3_key = generate_presigned_url(
        BUCKETS[upload_type], "pending", unique_name, content_type
    )
    return jsonify({
        "upload_url": presigned_url,
        "file_key": s3_key,
        "message": "Upload URL generated. Upload your file to this URL within 1 hour."
    })

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})

@app.route("/shutdown-worker", methods=["POST"])
def shutdown_worker():
    # Optional endpoint to stop worker gracefully (e.g., before scaling down)
    stop_event.set()
    return jsonify({"status": "stopping worker"})

# ========= Main =========
if __name__ == "__main__":
    try:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
    finally:
        stop_event.set()