from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import boto3
import uuid
import threading
import mimetypes
import pathlib
import time
from botocore.exceptions import ClientError

app = Flask(__name__)
CORS(app)

# -------------------------
# AWS setup
# -------------------------
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

TEMP_BUCKET = "hhftempuservids"
PERM_BUCKET = "hhfuservideos"

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

rekognition = boto3.client(
    "rekognition",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".webm", ".mkv"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif"}

# -------------------------
# Helpers
# -------------------------
def guess_content_type(filename: str) -> str:
    ctype, _ = mimetypes.guess_type(filename)
    return ctype or "application/octet-stream"

def is_video(filename: str) -> bool:
    return pathlib.Path(filename.lower()).suffix in VIDEO_EXTS

def is_image(filename: str) -> bool:
    return pathlib.Path(filename.lower()).suffix in IMAGE_EXTS

def generate_presigned_get(bucket, key, expires=3600):
    try:
        return s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires,
        )
    except Exception as e:
        print("Error generating presigned URL:", e)
        return None

def approve_and_move(temp_key: str, filename: str, metadata: dict):
    perm_key = f"{uuid.uuid4()}_{filename}"
    s3_client.copy_object(
        Bucket=PERM_BUCKET,
        Key=perm_key,
        CopySource={"Bucket": TEMP_BUCKET, "Key": temp_key},
        Metadata=metadata,
        MetadataDirective="REPLACE",
    )
    s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
    video_url = generate_presigned_get(PERM_BUCKET, perm_key)
    print(f"Approved: {perm_key}, presigned URL: {video_url}")

def moderate_video(temp_key, filename, metadata):
    try:
        # Skip downloading, just moderate via Rekognition for simplicity
        if is_video(filename):
            response = rekognition.start_content_moderation(
                Video={"S3Object": {"Bucket": TEMP_BUCKET, "Name": temp_key}},
                MinConfidence=90,
            )
            job_id = response["JobId"]
            while True:
                result = rekognition.get_content_moderation(JobId=job_id)
                if result.get("JobStatus") in ["SUCCEEDED", "FAILED"]:
                    break
                time.sleep(5)
            labels = result.get("ModerationLabels", [])
            if labels:
                print(f"Rejected video due to labels: {labels}")
                s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
                return

        elif is_image(filename):
            result = rekognition.detect_moderation_labels(
                Image={"S3Object": {"Bucket": TEMP_BUCKET, "Name": temp_key}},
                MinConfidence=90,
            )
            if result.get("ModerationLabels"):
                print(f"Rejected image due to labels")
                s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
                return

        approve_and_move(temp_key, filename, metadata)

    except Exception as e:
        print("Moderation error:", e)
        s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)

# -------------------------
# Routes
# -------------------------
@app.route("/get-upload-url", methods=["POST"])
def get_upload_url():
    data = request.get_json() or {}
    filename = data.get("filename")
    email = data.get("email")
    video_type = data.get("videoType")
    consent = data.get("consent", True)

    if not filename or not email or not video_type or not consent:
        return jsonify({"status": "error", "message": "Missing required fields"}), 400

    temp_key = f"{uuid.uuid4()}_{filename}"
    content_type = guess_content_type(filename)

    try:
        presigned_url = s3_client.generate_presigned_url(
            "put_object",
            Params={"Bucket": TEMP_BUCKET, "Key": temp_key, "ContentType": content_type},
            ExpiresIn=3600,
        )
        return jsonify({
            "status": "success",
            "upload_url": presigned_url,
            "temp_key": temp_key,
            "required_headers": {"Content-Type": content_type},
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/confirm-upload", methods=["POST"])
def confirm_upload():
    data = request.get_json() or {}
    temp_key = data.get("temp_key")
    filename = data.get("filename")
    email = data.get("email")
    video_type = data.get("videoType")
    comments = data.get("comments", "")

    if not temp_key or not filename or not email or not video_type:
        return jsonify({"status": "error", "message": "Missing required fields"}), 400

    metadata = {"email": email, "videoType": video_type, "comments": comments}
    threading.Thread(target=moderate_video, args=(temp_key, filename, metadata), daemon=True).start()
    return jsonify({"status": "success", "message": "Moderation started"})

@app.route("/get-perm-video", methods=["POST"])
def get_perm_video():
    """Return a presigned URL for a permanent video after moderation"""
    data = request.get_json() or {}
    perm_key = data.get("perm_key")
    if not perm_key:
        return jsonify({"status": "error", "message": "Missing perm_key"}), 400

    url = generate_presigned_get(PERM_BUCKET, perm_key)
    if not url:
        return jsonify({"status": "error", "message": "Could not generate URL"}), 500

    return jsonify({"status": "success", "video_url": url})

@app.route("/test")
def test():
    return jsonify({"status": "ok", "message": "Server is live"})

# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
