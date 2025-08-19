from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import boto3
import uuid
import threading
import subprocess
import time
from botocore.exceptions import ClientError

app = Flask(__name__)
CORS(app)

# -------------------------
# AWS setup
# -------------------------
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

TEMP_BUCKET = os.getenv("S3_BUCKET_USER")
PERM_BUCKET = os.getenv("S3_BUCKET_MUSICIAN")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

rekognition = boto3.client(
    "rekognition",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

ses_client = boto3.client(
    "ses",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

NOTIFY_EMAIL = os.getenv("NOTIFICATION_EMAIL")

# -------------------------
# Helpers
# -------------------------
def get_video_duration(file_path):
    """Return video duration in seconds using ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries",
             "format=duration", "-of",
             "default=noprint_wrappers=1:nokey=1", file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return float(result.stdout)
    except Exception as e:
        print("FFprobe error:", e)
        return None

def send_email_notification(subject, body):
    try:
        ses_client.send_email(
            Source=NOTIFY_EMAIL,
            Destination={"ToAddresses": [NOTIFY_EMAIL]},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": body}}
            }
        )
    except ClientError as e:
        print("SES email error:", e)

def moderate_video(temp_key, filename, metadata):
    """Moderate and move approved videos after upload confirmation."""
    try:
        tmp_file = f"/tmp/{uuid.uuid4()}_{filename}"
        s3_client.download_file(TEMP_BUCKET, temp_key, tmp_file)
        duration = get_video_duration(tmp_file)

        if duration is None or duration < 15 or duration > 240:
            print(f"Video '{filename}' rejected due to duration: {duration}")
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
            return

        response = rekognition.start_content_moderation(
            Video={"S3Object": {"Bucket": TEMP_BUCKET, "Name": temp_key}},
            MinConfidence=90
        )
        job_id = response["JobId"]

        # Wait for moderation results
        while True:
            result = rekognition.get_content_moderation(JobId=job_id)
            status = result.get("JobStatus")
            if status in ["SUCCEEDED", "FAILED"]:
                break
            time.sleep(5)

        moderation_labels = result.get("ModerationLabels", [])
        if status == "FAILED" or moderation_labels:
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
            print(f"Video '{filename}' rejected by Rekognition.")
        else:
            perm_key = f"{uuid.uuid4()}_{filename}"
            copy_source = {"Bucket": TEMP_BUCKET, "Key": temp_key}
            s3_client.copy_object(
                Bucket=PERM_BUCKET,
                Key=perm_key,
                CopySource=copy_source,
                ACL="public-read",
                Metadata=metadata,
                MetadataDirective="REPLACE"
            )
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
            video_url = f"https://{PERM_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{perm_key}"
            print(f"Video '{filename}' approved and moved to permanent bucket.")
            send_email_notification(
                "New Video Uploaded",
                f"Video '{filename}' has been approved and is available at {video_url}"
            )
    except Exception as e:
        print("Moderation error:", e)
        try:
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
        except:
            pass

# -------------------------
# Pre-signed URL Endpoint
# -------------------------
@app.route("/get-upload-url", methods=["POST"])
def get_upload_url():
    data = request.get_json()
    email = data.get("email", "").strip()
    video_type = data.get("videoType", "").strip()
    comments = data.get("comments", "").strip()
    consent = data.get("consent", True)
    filename = data.get("filename")

    if not email or not video_type or not consent or not filename:
        return jsonify({"status": "error", "message": "Missing required fields"}), 400

    temp_key = f"{uuid.uuid4()}_{filename}"

    try:
        presigned_url = s3_client.generate_presigned_url(
            ClientMethod="put_object",
            Params={
                "Bucket": TEMP_BUCKET,
                "Key": temp_key,
                "ACL": "private",
                "Metadata": {
                    "email": email,
                    "videoType": video_type,
                    "comments": comments
                }
            },
            ExpiresIn=3600
        )
        return jsonify({"status": "success", "upload_url": presigned_url, "temp_key": temp_key})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# -------------------------
# Post-upload confirmation Endpoint
# -------------------------
@app.route("/confirm-upload", methods=["POST"])
def confirm_upload():
    data = request.get_json()
    temp_key = data.get("temp_key")
    filename = data.get("filename")
    email = data.get("email")
    video_type = data.get("videoType")
    comments = data.get("comments", "")

    if not temp_key or not filename or not email or not video_type:
        return jsonify({"status": "error", "message": "Missing required fields"}), 400

    metadata = {"email": email, "videoType": video_type, "comments": comments}
    threading.Thread(target=moderate_video, args=(temp_key, filename, metadata)).start()

    return jsonify({"status": "success", "message": "Moderation started"})

# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)