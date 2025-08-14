# main.py
import os
import uuid
import json
import hashlib
from datetime import datetime
from urllib.parse import unquote_plus

import boto3
import stripe
from flask import Flask, request, jsonify, abort
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# ================ ENV & FLASK ================
load_dotenv()
app = Flask(__name__)

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")

BUCKETS = {
    "videos": os.getenv("S3_BUCKET_VIDEOS"),
    "promo": os.getenv("S3_BUCKET_PROMO"),
    "advertising": os.getenv("S3_BUCKET_ADVERTISING"),
}

DDB_TABLE = os.getenv("DYNAMODB_TABLE", "HipHopFeverFileHashes")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")
NOTIFY_EMAIL = os.getenv("NOTIFICATION_EMAIL")  # admin notifications

VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm", ".wmv", ".flv", ".mpeg", ".mpg"}

# AWS clients
common_kwargs = dict(region_name=AWS_REGION,
                     aws_access_key_id=AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3 = boto3.client("s3", **common_kwargs)
rekognition = boto3.client("rekognition", **common_kwargs)
dynamodb = boto3.client("dynamodb", **common_kwargs)
sns = boto3.client("sns", **common_kwargs)

stripe.api_key = STRIPE_API_KEY

# ================ UTILITIES ================
def s3_upload_url(bucket, key, expires=3600):
    return s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires,
    )

def s3_delete(bucket, key):
    s3.delete_object(Bucket=bucket, Key=key)

def s3_move(bucket, src_key, dst_key):
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": src_key}, Key=dst_key)
    s3_delete(bucket, src_key)

def s3_stream_md5(bucket, key):
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

def guess_is_video(content_type: str, key: str) -> bool:
    ct = (content_type or "").lower()
    if ct.startswith("video/"):
        return True
    for ext in VIDEO_EXTS:
        if key.lower().endswith(ext):
            return True
    return False

def rekogn_image_labels(bucket, key):
    resp = rekognition.detect_moderation_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}}, MinConfidence=80)
    return resp.get("ModerationLabels", [])

def rekogn_video_moderation(bucket, key, max_wait_seconds=300, poll_every=5):
    start = rekognition.start_content_moderation(Video={"S3Object": {"Bucket": bucket, "Name": key}}, MinConfidence=80)
    job_id = start["JobId"]
    waited = 0
    next_token = None
    labels = []
    while waited <= max_wait_seconds:
        import time
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
            return [{"Name": f"Rekognition{job_status}", "Confidence": 100.0}]
    return labels

def sns_notify(message):
    if SNS_TOPIC_ARN:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject="HipHopFever Upload Accepted", Message=message)

# ================ ENDPOINTS ================

@app.route("/get-upload-url", methods=["POST"])
def get_upload_url():
    """
    Client uploads file first, server runs moderation immediately.
    If accepted → return Stripe Checkout Session URL for payment.
    """
    data = request.json
    upload_type = data.get("upload_type")
    file_name = data.get("file_name")
    content_type = data.get("content_type")
    price_id = data.get("price_id")  # Stripe Price ID for promo or advertising

    if upload_type not in BUCKETS:
        return jsonify({"error": "Invalid upload_type"}), 400

    bucket = BUCKETS[upload_type]
    key = f"pending/{uuid.uuid4()}_{file_name}"

    # Generate S3 presigned URL for initial upload
    presigned_url = s3_upload_url(bucket, key)

    # Immediately run moderation after upload (simulate polling for uploaded object)
    # For simplicity, we assume the client uploads the file instantly after receiving this URL
    # In production, you may use S3 Event + Lambda or a small delay + polling
    # Here, we just return presigned_url + key and require client to call /moderate endpoint
    return jsonify({"upload_url": presigned_url, "s3_key": key})

@app.route("/moderate", methods=["POST"])
def moderate_and_checkout():
    """
    After the client uploads, they call this endpoint with s3_key.
    Server performs moderation.
    If passed → create Stripe Checkout Session.
    """
    data = request.json
    upload_type = data.get("upload_type")
    key = data.get("s3_key")
    price_id = data.get("price_id")
    file_name = data.get("file_name")
    
    if upload_type not in BUCKETS:
        return jsonify({"error": "Invalid upload_type"}), 400
    bucket = BUCKETS[upload_type]

    # Compute MD5 hash
    try:
        file_hash = s3_stream_md5(bucket, key)
    except ClientError:
        return jsonify({"error": "Could not access uploaded file"}), 400

    if ddb_is_duplicate(file_hash):
        s3_delete(bucket, key)
        return jsonify({"error": "Duplicate file"}), 400

    # Detect if video or image
    meta = s3.head_object(Bucket=bucket, Key=key)
    content_type = meta.get("ContentType", "")
    is_video = guess_is_video(content_type, key)

    # Run moderation
    if is_video:
        labels = rekogn_video_moderation(bucket, key)
    else:
        labels = rekogn_image_labels(bucket, key)

    if labels:
        s3_delete(bucket, key)
        return jsonify({"error": "Content rejected by moderation", "labels": labels}), 400

    # Passed moderation → create Stripe Checkout Session
    session = stripe.checkout.Session.create(
        payment_method_types=['card'],
        mode='payment',
        line_items=[{"price": price_id, "quantity": 1}],
        metadata={"s3_bucket": bucket, "s3_key": key, "upload_type": upload_type, "file_hash": file_hash},
        success_url=f"https://yourfrontend.com/success?session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"https://yourfrontend.com/cancel",
    )
    return jsonify({"checkout_url": session.url})

@app.route("/stripe-webhook", methods=["POST"])
def stripe_webhook():
    payload = request.data
    sig_header = request.headers.get('Stripe-Signature')
    event = None
    try:
        if STRIPE_WEBHOOK_SECRET:
            event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(payload)
    except Exception as e:
        print(f"Webhook error: {e}")
        return '', 400

    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        bucket = session['metadata']['s3_bucket']
        key = session['metadata']['s3_key']
        file_hash = session['metadata']['file_hash']

        # Move to approved
        approved_key = key.replace("pending/", "approved/")
        s3_move(bucket, key, approved_key)

        # Record hash to prevent duplicates
        ddb_record_hash(file_hash)

        # Notify admin
        sns_notify(f"File approved & paid: s3://{bucket}/{approved_key}")
    return '', 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))