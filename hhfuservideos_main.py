import os
import uuid
import hashlib
import tempfile
import subprocess
from datetime import datetime

import boto3
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import stripe

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# AWS clients
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

rekognition_client = boto3.client(
    "rekognition",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

sns_client = boto3.client(
    "sns",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

# Stripe client
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

# Buckets (matching your Render env vars)
BUCKETS = {
    "user": os.getenv("S3_BUCKET_USER_VIDEOS"),
    "music": os.getenv("S3_BUCKET_MUSIC_PROMO"),
    "advertising": os.getenv("S3_BUCKET_ADVERTISING"),
}

# Limits
MAX_USER_DURATION_SECONDS = 3 * 60 + 30  # 3.5 minutes
MAX_USER_FILE_SIZE_MB = 50

# Temporary local storage for moderation
TEMP_DIR = tempfile.gettempdir()

# Store uploaded file hashes to prevent duplicates (in-memory)
uploaded_hashes = set()


def get_file_hash(file_path):
    """Return SHA256 hash of a file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()


def get_video_duration(file_path):
    """Return video duration in seconds using ffprobe."""
    cmd = f'ffprobe -i "{file_path}" -show_entries format=duration -v quiet -of csv="p=0"'
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {result.stderr.decode()}")
    return float(result.stdout)


def moderate_file(bucket_name, key):
    """Check file using AWS Rekognition moderation labels."""
    response = rekognition_client.detect_moderation_labels(
        Image={"S3Object": {"Bucket": bucket_name, "Name": key}},
        MinConfidence=80,
    )
    labels = response.get("ModerationLabels", [])
    return labels


def generate_presigned_url(bucket_name, key, content_type):
    url = s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket_name, "Key": key, "ContentType": content_type},
        ExpiresIn=3600,
    )
    return url


def send_sns_email(subject, message):
    """Send an SNS email notification."""
    topic_arn = os.getenv("SNS_TOPIC_ARN")
    if topic_arn:
        sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=message)


@app.route("/upload/<upload_type>", methods=["POST"])
def upload_file(upload_type):
    """Handle file upload, moderation, duplicate check, and Stripe session creation."""
    if upload_type not in BUCKETS:
        return jsonify({"error": "Invalid upload type"}), 400

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    user_role = request.form.get("role", "user")
    file = request.files["file"]
    content_type = file.content_type
    original_filename = file.filename
    temp_path = os.path.join(TEMP_DIR, f"{uuid.uuid4()}_{original_filename}")
    file.save(temp_path)

    # Check duplicates
    file_hash = get_file_hash(temp_path)
    if file_hash in uploaded_hashes:
        os.remove(temp_path)
        return jsonify({"error": "Duplicate file detected"}), 400

    # Check file size (for non-admin users)
    if user_role != "admin":
        size_mb = os.path.getsize(temp_path) / (1024 * 1024)
        if size_mb > MAX_USER_FILE_SIZE_MB:
            os.remove(temp_path)
            return jsonify({"error": f"File too large. Max {MAX_USER_FILE_SIZE_MB} MB"}), 400

        # Check duration
        duration = get_video_duration(temp_path)
        if duration > MAX_USER_DURATION_SECONDS:
            os.remove(temp_path)
            return jsonify({"error": f"Video too long. Max {MAX_USER_DURATION_SECONDS / 60:.1f} minutes"}), 400

    # Upload to S3 pending folder
    bucket_name = BUCKETS[upload_type]
    s3_key = f"pending/{uuid.uuid4()}_{original_filename}"
    s3_client.upload_file(temp_path, bucket_name, s3_key, ExtraArgs={"ContentType": content_type})

    # Moderate file
    labels = moderate_file(bucket_name, s3_key)
    if labels:
        s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
        os.remove(temp_path)
        return jsonify({"error": "File rejected by moderation", "labels": labels}), 400

    # Mark file hash as uploaded
    uploaded_hashes.add(file_hash)

    # Create Stripe session for paid uploads (only for music or advertising)
    price_cents = 0
    stripe_url = None
    if upload_type in ["music", "advertising"]:
        price_cents = int(request.form.get("price_cents", 500))  # default $5.00
        try:
            session = stripe.checkout.Session.create(
                payment_method_types=["card"],
                line_items=[{
                    "price_data": {
                        "currency": "usd",
                        "product_data": {"name": f"{upload_type} upload"},
                        "unit_amount": price_cents,
                    },
                    "quantity": 1,
                }],
                mode="payment",
                success_url=os.getenv("FRONTEND_SUCCESS_URL", "http://localhost:3000/success"),
                cancel_url=os.getenv("FRONTEND_CANCEL_URL", "http://localhost:3000/cancel"),
            )
            stripe_url = session.url
        except Exception as e:
            return jsonify({"error": f"Stripe error: {str(e)}"}), 500

    # Move file to approved folder
    approved_key = s3_key.replace("pending/", "approved/")
    s3_client.copy_object(Bucket=bucket_name, CopySource={"Bucket": bucket_name, "Key": s3_key}, Key=approved_key)
    s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
    os.remove(temp_path)

    # Notify via SNS
    send_sns_email(
        subject="New Approved Upload",
        message=f"{upload_type} upload approved: {approved_key}"
    )

    return jsonify({
        "message": "Upload successful and approved",
        "s3_key": approved_key,
        "stripe_checkout_url": stripe_url,
    })


@app.route("/health", methods=["GET"])
def health_check():
    """Simple endpoint to test if the API is live."""
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
