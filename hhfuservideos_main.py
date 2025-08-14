# main.py
import os
import uuid
import json
import time
import threading
import hashlib
from datetime import datetime
from urllib.parse import unquote_plus

import boto3
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# ==================== ENV & FLASK ====================
load_dotenv()
app = Flask(__name__)

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Three buckets only (uploads go to pending/, accepted to approved/)
BUCKETS = {
    "videos": os.getenv("S3_BUCKET_VIDEOS"),
    "promo": os.getenv("S3_BUCKET_PROMO"),
    "advertising": os.getenv("S3_BUCKET_ADVERTISING"),
}

# Infra names (override via env if needed)
DDB_TABLE = os.getenv("DYNAMODB_TABLE", "HipHopFeverFileHashes")
SNS_TOPIC_NAME = os.getenv("SNS_TOPIC_NAME", "HipHopFeverNotifications")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "HipHopFeverObjectCreatedQueue")
NOTIFY_EMAIL = os.getenv("NOTIFICATION_EMAIL")  # REQUIRED for emails

VIDEO_EXTS = {
    ".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm", ".wmv", ".flv", ".mpeg", ".mpg"
}

# ==================== AWS CLIENTS ====================
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

# ==================== INFRA SETUP ====================
def ensure_dynamodb_table():
    """Create DynamoDB table for file-hash de-dup if missing."""
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
            waiter = boto3.client("dynamodb", **common_kwargs).get_waiter("table_exists")
            waiter.wait(TableName=DDB_TABLE)
        else:
            raise

def ensure_sns_topic_and_subscription():
    """Create/find SNS topic and subscribe NOTIFY_EMAIL (must confirm once)."""
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

    if NOTIFY_EMAIL:
        subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn).get("Subscriptions", [])
        if not any(s["Protocol"] == "email" and s["Endpoint"] == NOTIFY_EMAIL for s in subs):
            sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=NOTIFY_EMAIL)
            print(f"[INIT] Subscribed {NOTIFY_EMAIL} to SNS topic (check email to confirm).")
    else:
        print("[WARN] NOTIFICATION_EMAIL not set; no emails will be sent.")

    return topic_arn

def ensure_sqs_queue():
    """Create or get the SQS queue used by S3 event notifications."""
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
    """Allow the specific S3 bucket to send events to the SQS queue."""
    bucket_arn = f"arn:aws:s3:::{bucket_name}"
    new_stmt = {
        "Sid": f"AllowS3SendMessage-{bucket_name}",
        "Effect": "Allow",
        "Principal": {"Service": "s3.amazonaws.com"},
        "Action": "sqs:SendMessage",
        "Resource": queue_arn,
        "Condition": {"ArnEquals": {"aws:SourceArn": bucket_arn}}
    }

    # Merge with existing policy
    try:
        current = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["Policy"]
        ).get("Attributes", {}).get("Policy")
        if current:
            policy = json.loads(current)
            policy["Statement"] = [s for s in policy["Statement"] if s.get("Sid") != new_stmt["Sid"]]
            policy["Statement"].append(new_stmt)
        else:
            policy = {"Version": "2012-10-17", "Statement": [new_stmt]}
    except ClientError:
        policy = {"Version": "2012-10-17", "Statement": [new_stmt]}

    sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"Policy": json.dumps(policy)})

def ensure_s3_event_notification(bucket_name, queue_arn):
    """Configure S3 to send ObjectCreated events for 'pending/' prefix to SQS."""
    cfg = {
        "QueueConfigurations": [{
            "Id": f"{bucket_name}-objcreated-to-{SQS_QUEUE_NAME}",
            "QueueArn": queue_arn,
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "pending/"}]}}
        }]
    }
    s3.put_bucket_notification_configuration(
        Bucket=bucket_name, NotificationConfiguration=cfg
    )
    print(f"[INIT] S3 event notification set: {bucket_name} → {SQS_QUEUE_NAME} (pending/ only)")

# ==================== HELPERS ====================
def head_object(bucket, key):
    return s3.head_object(Bucket=bucket, Key=key)

def guess_is_video(content_type: str, key: str) -> bool:
    ct = (content_type or "").lower()
    if ct.startswith("video/"):
        return True
    # fallback by extension
    for ext in VIDEO_EXTS:
        if key.lower().endswith(ext):
            return True
    return False

def s3_stream_md5(bucket, key):
    """MD5 hash of the S3 object (streaming chunks)."""
    h = hashlib.md5()
    obj = s3.get_object(Bucket=bucket, Key=key)
    for chunk in obj["Body"].iter_chunks(chunk_size=1024 * 1024):
        if chunk:
            h.update(chunk)
    return h.hexdigest()

def ddb_is_duplicate(file_hash):
    resp = dynamodb.get_item(TableName=DDB_TABLE, Key={"file_hash": {"S": file_hash}})
    return "Item" in resp

def ddb_record_hash(file_hash):
    dynamodb.put_item(
        TableName=DDB_TABLE,
        Item={"file_hash": {"S": file_hash}, "created_at": {"S": datetime.utcnow().isoformat()}}
    )

def rekogn_image_labels(bucket, key):
    resp = rekognition.detect_moderation_labels(
        Image={"S3Object": {"Bucket": bucket, "Name": key}}, MinConfidence=80
    )
    return resp.get("ModerationLabels", [])

def rekogn_video_moderation(bucket, key, max_wait_seconds=900, poll_every=5):
    """
    StartContentModeration (async) + poll GetContentModeration until complete.
    Returns list of moderation labels (empty list means clean).
    """
    start = rekognition.start_content_moderation(
        Video={"S3Object": {"Bucket": bucket, "Name": key}},
        MinConfidence=80
    )
    job_id = start["JobId"]

    waited = 0
    next_token = None
    labels = []

    while waited <= max_wait_seconds:
        time.sleep(poll_every)
        waited += poll_every

        kwargs = {"JobId": job_id, "SortBy": "TIMESTAMP"}
        if next_token:
            kwargs["NextToken"] = next_token

        status = rekognition.get_content_moderation(**kwargs)
        job_status = status.get("JobStatus")
        if job_status == "SUCCEEDED":
            mods = status.get("ModerationLabels", [])
            labels.extend(mods)
            next_token = status.get("NextToken")
            if not next_token:
                break
        elif job_status in {"FAILED", "ERROR"}:
            # treat as failed moderation -> reject
            return [{"Name": f"Rekognition{job_status}", "Confidence": 100.0}]
        # if IN_PROGRESS, continue

    # condense label names to unique simple list (match image path shape)
    condensed = []
    seen = set()
    for item in labels:
        name = item.get("ModerationLabel", {}).get("Name") or item.get("Name")
        if name and name not in seen:
            seen.add(name)
            condensed.append({"Name": name})
    return condensed

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

# ==================== PROCESSOR ====================
def process_object(topic_arn, bucket, key):
    """End-to-end processing for one S3 object."""
    # 0) Get metadata & normalize key
    key = unquote_plus(key)

    # 1) Duplicate check
    file_hash = s3_stream_md5(bucket, key)
    if ddb_is_duplicate(file_hash):
        s3_delete(bucket, key)
        print(f"[PROCESS] Rejected duplicate: s3://{bucket}/{key}")
        return

    # 2) Decide moderation path (image vs video)
    try:
        meta = head_object(bucket, key)
        content_type = meta.get("ContentType") or ""
    except ClientError:
        # If we can't head the object, delete as invalid
        s3_delete(bucket, key)
        print(f"[PROCESS] Rejected (no head): s3://{bucket}/{key}")
        return

    try:
        if guess_is_video(content_type, key):
            labels = rekogn_video_moderation(bucket, key)
        else:
            labels = rekogn_image_labels(bucket, key)
    except ClientError as e:
        # Unsupported or Rekognition error -> reject
        s3_delete(bucket, key)
        print(f"[PROCESS] Rejected by moderation error: s3://{bucket}/{key} ({e.response['Error']['Code']})")
        return

    # 3) If any labels returned -> reject
    if labels and len(labels) > 0:
        s3_delete(bucket, key)
        print(f"[PROCESS] Rejected by moderation: s3://{bucket}/{key} -> {labels}")
        return

    # 4) Accept → move & record & notify
    final_key = key.replace("pending/", "approved/", 1)
    s3_move(bucket, key, final_key)
    ddb_record_hash(file_hash)
    sns_notify(topic_arn, bucket, final_key)
    print(f"[PROCESS] Accepted & moved: s3://{bucket}/{final_key}")

# ==================== WORKER (SQS CONSUMER) ====================
def process_s3_event_record(topic_arn, record):
    s3info = record["s3"]
    bucket = s3info["bucket"]["name"]
    key = s3info["object"]["key"]
    process_object(topic_arn, bucket, key)

def sqs_worker(topic_arn, queue_url, stop_evt):
    print("[WORKER] SQS worker started.")
    while not stop_evt.is_set():
        try:
            msgs = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=5, WaitTimeSeconds=20
            )
            for m in msgs.get("Messages", []):
                body = json.loads(m["Body"])
                records = body.get("Records")
                if not records and "Message" in body:
                    inner = json.loads(body["Message"])
                    records = inner.get("Records", [])
                if records:
                    for rec in records:
                        if rec.get("eventSource") == "aws:s3":
                            process_s3_event_record(topic_arn, rec)
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
        except Exception as e:
            print(f"[WORKER ERROR] {e}")
        time.sleep(1)
    print("[WORKER] Stopped.")

stop_event = threading.Event()

def bootstrap_infra_and_worker():
    # 1) Validate required env
    missing = [k for k, v in {
        "AWS_REGION": AWS_REGION, "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY, "S3_BUCKET_VIDEOS": BUCKETS["videos"],
        "S3_BUCKET_PROMO": BUCKETS["promo"], "S3_BUCKET_ADVERTISING": BUCKETS["advertising"]
    }.items() if not v]
    if missing:
        print(f"[WARN] Missing env vars: {missing}")

    # 2) Core infra
    ensure_dynamodb_table()
    topic_arn = ensure_sns_topic_and_subscription()
    queue_url, queue_arn = ensure_sqs_queue()

    # 3) Wire each bucket to queue for pending/ prefix
    for bucket in BUCKETS.values():
        if bucket:
            ensure_queue_policy_for_s3(queue_url, queue_arn, bucket)
            ensure_s3_event_notification(bucket, queue_arn)

    # 4) Start SQS worker thread
    worker = threading.Thread(target=sqs_worker, args=(topic_arn, queue_url, stop_event), daemon=True)
    worker.start()
    return worker

# Start background worker on import (once per process)
_worker_thread = bootstrap_infra_and_worker()

# ==================== API ====================
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
    """
    Return a presigned URL for clients to upload directly to S3 (pending/).
    Body: { "file_name": "myfile.ext", "content_type": "video/mp4" }
    """
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
    stop_event.set()
    return jsonify({"status": "stopping worker"})

# ==================== MAIN ====================
if __name__ == "__main__":
    try:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
    finally:
        stop_event.set()