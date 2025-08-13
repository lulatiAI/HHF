import os
import uuid
from datetime import datetime

import boto3
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# AWS S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

# AWS Rekognition client
rekognition_client = boto3.client(
    "rekognition",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

# Bucket mapping for each upload type
BUCKETS = {
    "user": os.getenv("S3_BUCKET_USER"),
    "musician": os.getenv("S3_BUCKET_MUSICIAN"),
    "advertiser": os.getenv("S3_BUCKET_ADVERTISER"),
    "radio": os.getenv("S3_BUCKET_RADIO"),
}


def generate_presigned_url(bucket_name, folder, file_name, content_type):
    """Generate a pre-signed S3 URL for uploading a file."""
    key = f"{folder}/{file_name}"
    url = s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket_name, "Key": key, "ContentType": content_type},
        ExpiresIn=3600,  # 1 hour
    )
    return url, key


def moderate_file(bucket_name, key):
    """Check uploaded file for moderation labels using Rekognition."""
    response = rekognition_client.detect_moderation_labels(
        Image={"S3Object": {"Bucket": bucket_name, "Name": key}},
        MinConfidence=80,
    )
    return response.get("ModerationLabels", [])


@app.route("/upload/<upload_type>", methods=["POST"])
def upload_file(upload_type):
    """Generate an S3 pre-signed upload URL."""
    if upload_type not in BUCKETS:
        return jsonify({"error": "Invalid upload type"}), 400

    data = request.get_json()
    if not data or "file_name" not in data or "content_type" not in data:
        return jsonify({"error": "Missing required fields"}), 400

    file_name = f"{uuid.uuid4()}-{data['file_name']}"
    bucket_name = BUCKETS[upload_type]

    presigned_url, s3_key = generate_presigned_url(
        bucket_name, "pending", file_name, data["content_type"]
    )

    submission_record = {
        "id": str(uuid.uuid4()),
        "upload_type": upload_type,
        "bucket": bucket_name,
        "s3_key": s3_key,
        "status": "pending",
        "submitted_at": datetime.utcnow().isoformat(),
    }
    print("New submission:", submission_record)

    return jsonify({
        "upload_url": presigned_url,
        "file_key": s3_key,
        "message": "Upload URL generated."
    })


@app.route("/moderate", methods=["POST"])
def moderate():
    """Moderate a file in S3 using AWS Rekognition."""
    data = request.get_json()
    if not data or "bucket" not in data or "key" not in data:
        return jsonify({"error": "Missing required fields"}), 400

    labels = moderate_file(data["bucket"], data["key"])
    return jsonify({"moderation_labels": labels})


# This allows Gunicorn to detect `app` when running `gunicorn app:app`
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))