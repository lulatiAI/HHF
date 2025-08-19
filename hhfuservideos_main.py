# filename: hhfuservideos_main.py
from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import boto3
import uuid
import threading
import subprocess
import time
from botocore.exceptions import ClientError, BotoCoreError
from urllib.request import urlopen
from urllib.error import URLError

app = Flask(__name__)
CORS(app)

# =========================================================
# AWS setup via environment variables (Render Dashboard)
# =========================================================
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# TEMP bucket must be in the SAME region as Rekognition
TEMP_BUCKET = os.getenv("S3_BUCKET_USER")        # temporary holding / moderation
PERM_BUCKET = os.getenv("S3_BUCKET_MUSICIAN")    # final, public bucket

NOTIFY_EMAIL = os.getenv("NOTIFICATION_EMAIL")   # SES-verified sender/recipient

# Basic sanity checks (won’t crash app if missing; just logs)
def _warn_env(name, val):
    if not val:
        print(f"[WARN] Missing env var: {name}")

for k, v in [
    ("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY),
    ("AWS_SECRET_ACCESS_KEY", AWS_SECRET_KEY),
    ("AWS_REGION", AWS_REGION),
    ("S3_BUCKET_USER", TEMP_BUCKET),
    ("S3_BUCKET_MUSICIAN", PERM_BUCKET),
    ("NOTIFICATION_EMAIL", NOTIFY_EMAIL),
]:
    _warn_env(k, v)

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

# =========================================================
# Helpers
# =========================================================
def get_video_duration(file_path):
    """
    Try to get duration using ffprobe. If ffprobe is not available in the
    Render image, return None and we'll skip strict duration checks.
    """
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                file_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        out = (result.stdout or b"").decode().strip()
        if out:
            return float(out)
        return None
    except Exception as e:
        print("[WARN] ffprobe not available or failed:", e)
        return None

def send_email_notification(subject, body):
    if not NOTIFY_EMAIL:
        print("[WARN] NOTIFICATION_EMAIL not set; skipping SES email.")
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
        print("[WARN] SES email error:", e)

def _move_to_perm_and_notify(temp_key, filename, metadata):
    perm_key = f"{uuid.uuid4()}_{filename}"
    copy_source = {"Bucket": TEMP_BUCKET, "Key": temp_key}
    s3_client.copy_object(
        Bucket=PERM_BUCKET,
        Key=perm_key,
        CopySource=copy_source,
        ACL="public-read",
        Metadata=metadata or {},
        MetadataDirective="REPLACE",
    )
    s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
    video_url = f"https://{PERM_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{perm_key}"
    print(f"[INFO] Video approved → {video_url}")
    send_email_notification(
        "New Video Uploaded",
        f"Video '{filename}' has been approved and is available at:\n{video_url}",
    )

def moderate_video(temp_key, filename, metadata):
    """
    1) (Optional) Duration check if ffprobe available.
    2) Rekognition moderation.
    3) If passed → move to PERM bucket, make public, email.
    """
    try:
        # Download temp object to /tmp for duration check (best-effort)
        tmp_file = f"/tmp/{uuid.uuid4()}_{filename}"
        try:
            s3_client.download_file(TEMP_BUCKET, temp_key, tmp_file)
            duration = get_video_duration(tmp_file)
        except (ClientError, BotoCoreError) as e:
            print("[WARN] Could not download for duration check:", e)
            duration = None

        # Soft duration policy: only reject if we have a number and it's out of range
        if duration is not None and (duration < 15 or duration > 240):
            print(f"[INFO] Rejected by duration policy: {duration:.2f}s")
            try:
                s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
            finally:
                return

        # Rekognition content moderation
        response = rekognition.start_content_moderation(
            Video={"S3Object": {"Bucket": TEMP_BUCKET, "Name": temp_key}},
            MinConfidence=90,
        )
        job_id = response["JobId"]
        print(f"[INFO] Rekognition job started: {job_id}")

        # Poll until done
        while True:
            result = rekognition.get_content_moderation(JobId=job_id)
            status = result.get("JobStatus")
            if status in ("SUCCEEDED", "FAILED"):
                break
            time.sleep(5)

        moderation_labels = result.get("ModerationLabels", [])
        if status == "FAILED":
            print("[INFO] Rekognition FAILED; deleting temp object")
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
            return

        if moderation_labels:
            print(f"[INFO] Rejected by moderation: {len(moderation_labels)} labels")
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
            return

        # Passed moderation
        _move_to_perm_and_notify(temp_key, filename, metadata)

    except Exception as e:
        print("[ERROR] Moderation pipeline error:", e)
        try:
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
        except Exception:
            pass

def _normalize_bool(val, default=False):
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "yes", "on")
    return default

# =========================================================
# Routes
# =========================================================

@app.route("/", methods=["GET"])
def root():
    # Simple health page; avoids index.html requirement
    return "✅ Backend is running", 200

# ---------- Forminator Webhook ---------- #
@app.route("/webhook", methods=["POST"])
def forminator_webhook():
    """
    Accepts Forminator submissions.
    Supports:
      - application/json
      - application/x-www-form-urlencoded or multipart/form-data

    Expected fields (names can be mapped in Forminator):
      email, videoType, comments, consent, filename, file_url

    If file_url is provided (e.g., WordPress upload URL), we download it
    to /tmp and upload to TEMP_BUCKET, then start moderation.
    """
    try:
        if request.is_json:
            data = request.get_json(silent=True) or {}
        else:
            # Flatten form fields (first value)
            data = {k: (v if not isinstance(v, list) else v[0]) for k, v in request.values.items()}

        email = (data.get("email") or "").strip()
        video_type = (data.get("videoType") or "").strip()
        comments = (data.get("comments") or "").strip()
        consent = _normalize_bool(data.get("consent", True), default=True)
        filename = (data.get("filename") or "").strip()
        file_url = (data.get("file_url") or "").strip()

        if not email or not video_type or not consent:
            return jsonify({"status": "error", "message": "Missing required fields (email, videoType, consent)"}), 400

        if not filename:
            # Try to infer filename from URL
            if file_url:
                filename = file_url.split("/")[-1].split("?")[0] or f"upload_{uuid.uuid4()}.bin"
            else:
                return jsonify({"status": "error", "message": "Missing filename"}), 400

        # Upload to TEMP bucket
        temp_key = f"{uuid.uuid4()}_{filename}"

        if file_url:
            # Download from URL then upload to S3
            try:
                with urlopen(file_url) as resp:
                    data_bytes = resp.read()
                s3_client.put_object(
                    Bucket=TEMP_BUCKET,
                    Key=temp_key,
                    Body=data_bytes,
                    ACL="private",
                    Metadata={
                        "email": email,
                        "videoType": video_type,
                        "comments": comments,
                    },
                )
            except URLError as e:
                return jsonify({"status": "error", "message": f"Could not fetch file_url: {e}"}), 400
        else:
            # No URL — Forminator didn’t pass a file. You can still save the metadata.
            # If your Forminator setup uploads a file separately via JS, use /get-upload-url + /confirm-upload.
            return jsonify({"status": "error", "message": "No file_url provided"}), 400

        # Kick off moderation in background
        metadata = {"email": email, "videoType": video_type, "comments": comments}
        threading.Thread(target=moderate_video, args=(temp_key, filename, metadata), daemon=True).start()

        return jsonify({"status": "success", "message": "Upload received; moderation started", "temp_key": temp_key})

    except Exception as e:
        print("[ERROR] /webhook error:", e)
        return jsonify({"status": "error", "message": "Internal server error"}), 500

# ---------- JS Flow: Get pre-signed PUT URL ---------- #
@app.route("/get-upload-url", methods=["POST"])
def get_upload_url():
    """
    Frontend (e.g., WordPress page JS) calls this to get a pre-signed S3 PUT URL.
    Then the browser uploads the file directly to S3 (TEMP bucket).
    Finally, frontend calls /confirm-upload to start moderation.
    """
    try:
        data = request.get_json(silent=True) or {}
        email = (data.get("email") or "").strip()
        video_type = (data.get("videoType") or "").strip()
        comments = (data.get("comments") or "").strip()
        consent = _normalize_bool(data.get("consent", True), default=True)
        filename = data.get("filename")

        if not email or not video_type or not consent or not filename:
            return jsonify({"status": "error", "message": "Missing required fields"}), 400

        temp_key = f"{uuid.uuid4()}_{filename}"

        presigned_url = s3_client.generate_presigned_url(
            ClientMethod="put_object",
            Params={
                "Bucket": TEMP_BUCKET,
                "Key": temp_key,
                "ACL": "private",
                # If client includes these headers on upload, they become x-amz-meta-*
                "Metadata": {
                    "email": email,
                    "videoType": video_type,
                    "comments": comments,
                },
            },
            ExpiresIn=3600,
        )

        return jsonify({
            "status": "success",
            "upload_url": presigned_url,
            "temp_key": temp_key
        })
    except Exception as e:
        print("[ERROR] /get-upload-url error:", e)
        return jsonify({"status": "error", "message": "Internal server error"}), 500

# ---------- JS Flow: Confirm upload & start moderation ---------- #
@app.route("/confirm-upload", methods=["POST"])
def confirm_upload():
    try:
        data = request.get_json(silent=True) or {}
        temp_key = data.get("temp_key")
        filename = data.get("filename")
        email = (data.get("email") or "").strip()
        video_type = (data.get("videoType") or "").strip()
        comments = (data.get("comments") or "").strip()

        if not temp_key or not filename or not email or not video_type:
            return jsonify({"status": "error", "message": "Missing required fields"}), 400

        metadata = {"email": email, "videoType": video_type, "comments": comments}
        threading.Thread(target=moderate_video, args=(temp_key, filename, metadata), daemon=True).start()

        return jsonify({"status": "success", "message": "Moderation started"})
    except Exception as e:
        print("[ERROR] /confirm-upload error:", e)
        return jsonify({"status": "error", "message": "Internal server error"}), 500

# ---------- Health ---------- #
@app.route("/test", methods=["GET"])
def test():
    return jsonify({"status": "ok", "message": "Server is live"})

# =========================================================
# Entrypoint (Render runs with gunicorn)
# =========================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)