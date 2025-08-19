from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import boto3
import uuid
import threading
import subprocess
import time
import mimetypes
import pathlib
from botocore.exceptions import ClientError

app = Flask(__name__)
CORS(app)

# -------------------------
# AWS setup
# -------------------------
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")  # default to your bucket region

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

ses_client = boto3.client(
    "ses",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

NOTIFY_EMAIL = os.getenv("NOTIFICATION_EMAIL")

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

def get_video_duration(file_path):
    """
    Try ffprobe. If not available or fails, return None (do NOT fail the upload).
    """
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries",
             "format=duration", "-of",
             "default=noprint_wrappers=1:nokey=1", file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        val = result.stdout.decode().strip()
        return float(val) if val else None
    except Exception as e:
        print("FFprobe not available or failed:", e)
        return None

def send_email_notification(subject, body):
    if not NOTIFY_EMAIL:
        return
    try:
        ses_client.send_email(
            Source=NOTIFY_EMAIL,
            Destination={"ToAddresses": [NOTIFY_EMAIL]},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": body}},
            },
        )
    except ClientError as e:
        print("SES email error:", e)

def approve_and_move(temp_key: str, filename: str, metadata: dict):
    """
    Copy the object from TEMP to PERM, apply metadata, then delete the temp object.
    """
    perm_key = f"{uuid.uuid4()}_{filename}"
    copy_source = {"Bucket": TEMP_BUCKET, "Key": temp_key}
    # No ACL here (Object Ownership is bucket owner enforced)
    s3_client.copy_object(
        Bucket=PERM_BUCKET,
        Key=perm_key,
        CopySource=copy_source,
        Metadata=metadata,
        MetadataDirective="REPLACE",
    )
    s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
    video_url = f"https://{PERM_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{perm_key}"
    print(f"Approved and moved to permanent bucket: {perm_key}")
    send_email_notification(
        "New Upload Approved",
        f"File '{filename}' approved and available at {video_url}",
    )

def moderate_video(temp_key, filename, metadata):
    try:
        # Download to /tmp for optional duration check
        tmp_file = f"/tmp/{uuid.uuid4()}_{filename}"
        s3_client.download_file(TEMP_BUCKET, temp_key, tmp_file)

        ext = pathlib.Path(filename.lower()).suffix
        print(f"Moderation start for {filename} (ext={ext})")

        # Optional duration gate (only if ffprobe is available and returns a value)
        duration = get_video_duration(tmp_file) if is_video(filename) else None
        if duration is not None:
            if duration < 15 or duration > 240:
                print(f"Rejected due to duration ({duration}s). Deleting temp object.")
                s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
                return

        # Rekognition: video vs image
        if is_video(filename):
            # Async moderation for video
            response = rekognition.start_content_moderation(
                Video={"S3Object": {"Bucket": TEMP_BUCKET, "Name": temp_key}},
                MinConfidence=90,
            )
            job_id = response["JobId"]
            print(f"Rekognition job started: {job_id}")

            while True:
                result = rekognition.get_content_moderation(JobId=job_id)
                status = result.get("JobStatus")
                if status in ["SUCCEEDED", "FAILED"]:
                    break
                time.sleep(5)

            labels = result.get("ModerationLabels", [])
            if status == "FAILED" or labels:
                print(f"Rejected by Rekognition (video). Labels: {labels}")
                s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
                return

            approve_and_move(temp_key, filename, metadata)

        elif is_image(filename):
            # Sync moderation for image
            result = rekognition.detect_moderation_labels(
                Image={"S3Object": {"Bucket": TEMP_BUCKET, "Name": temp_key}},
                MinC
