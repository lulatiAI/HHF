from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import boto3
import uuid
import tempfile
import subprocess
from botocore.exceptions import ClientError
import time

app = Flask(__name__)
CORS(app)

# -------------------------
# Environment / AWS Setup
# -------------------------
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

TEMP_BUCKET = "hhfuservideo-temp"
PERM_BUCKET = "hhfuservideo"

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

NOTIFY_EMAIL = "Antoinemaxwell0@gmail.com"

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

# -------------------------
# Video Upload Endpoint
# -------------------------
@app.route("/upload-video", methods=["POST"])
def upload_video():
    tmp_file = None
    try:
        # Validate required fields
        email = request.form.get("email", "").strip()
        video_type = request.form.get("videoType", "").strip()
        comments = request.form.get("comments", "").strip()
        consent = request.form.get("consent", "true").lower() == "true"  # frontend ensures checkbox

        if not email or not video_type or not consent:
            return jsonify({"status": "error", "message": "Email, video type, and consent are required"}), 400

        if "video" not in request.files:
            return jsonify({"status": "error", "message": "No video file provided"}), 400

        file = request.files["video"]
        filename = file.filename
        ext = filename.split('.')[-1].lower()
        if ext not in ["mp4", "mov", "avi", "mkv"]:
            return jsonify({"status": "error", "message": "Unsupported video format"}), 400

        # Save video temporarily
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}")
        file.save(tmp_file.name)

        # Check duration
        duration = get_video_duration(tmp_file.name)
        if duration is None:
            return jsonify({"status": "error", "message": "Could not determine video duration"}), 400
        if duration < 15 or duration > 210:
            return jsonify({"status": "error", "message": "Video must be between 15s and 3:30s"}), 400

        # Upload to temp S3 bucket
        temp_key = f"{uuid.uuid4()}.{ext}"
        s3_client.upload_file(
            tmp_file.name, TEMP_BUCKET, temp_key,
            ExtraArgs={"ACL": "private", "Metadata": {
                "email": email,
                "videoType": video_type,
                "comments": comments
            }}
        )

        # Start Rekognition moderation
        response = rekognition.start_content_moderation(
            Video={"S3Object": {"Bucket": TEMP_BUCKET, "Name": temp_key}},
            MinConfidence=80
        )
        job_id = response["JobId"]

        # Poll until Rekognition finishes
        while True:
            result = rekognition.get_content_moderation(JobId=job_id)
            status = result.get("JobStatus")
            if status in ["SUCCEEDED", "FAILED"]:
                break
            time.sleep(5)

        if status == "FAILED":
            send_email_notification("Video Rejected", f"Rekognition failed for video: {filename}")
            return jsonify({"status": "rejected", "reason": "Moderation failed"}), 400

        moderation_labels = result.get("ModerationLabels", [])
        if moderation_labels:
            # Rejected by Rekognition
            send_email_notification(
                "Video Rejected",
                f"Your uploaded video '{filename}' was rejected by Rekognition: {moderation_labels}"
            )
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
            return jsonify({"status": "rejected", "reason": "Video failed moderation"}), 400

        # Approved: Move video to permanent bucket
        perm_key = f"{uuid.uuid4()}.{ext}"
        copy_source = {"Bucket": TEMP_BUCKET, "Key": temp_key}
        s3_client.copy_object(
            Bucket=PERM_BUCKET,
            Key=perm_key,
            CopySource=copy_source,
            ACL="public-read",
            Metadata={"email": email, "videoType": video_type, "comments": comments},
            MetadataDirective="REPLACE"
        )
        s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)

        video_url = f"https://{PERM_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{perm_key}"
        return jsonify({"status": "success", "video_url": video_url})

    except Exception as e:
        print("Error:", e)
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        # Cleanup temp file
        if tmp_file:
            try:
                os.unlink(tmp_file.name)
            except Exception as e:
                print("Temp file cleanup error:", e)

# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
