from flask import Flask, request, jsonify
from flask_cors import CORS
from flasgger import Swagger
import os
import boto3
import uuid
import threading
import subprocess
import time
import json
import requests
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# -------------------------------------------------
# App + Swagger
# -------------------------------------------------
app = Flask(__name__)
CORS(app)

swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": "apispec_1",
            "route": "/apispec_1.json",
            "rule_filter": lambda rule: True,
            "model_filter": lambda tag: True,
        }
    ],
    "swagger_ui": True,
    "specs_route": "/docs/",
}
swagger_template = {
    "info": {
        "title": "HHF User Videos API",
        "version": "1.1",
        "description": "Direct-to-S3 uploads + automated moderation (Rekognition) + email + optional WP callback",
    }
}
Swagger(app, config=swagger_config, template=swagger_template)

# -------------------------------------------------
# Config (env)
# -------------------------------------------------
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# S3 buckets
TEMP_BUCKET = os.getenv("TEMP_BUCKET", "hhfuservideo-temp")
PERM_BUCKET = os.getenv("PERM_BUCKET", "hhfuservideo")

# Email (SES)
NOTIFY_FROM_EMAIL = os.getenv("NOTIFY_FROM_EMAIL", "no-reply@yourdomain.com")
NOTIFY_INTERNAL_EMAIL = os.getenv("NOTIFY_INTERNAL_EMAIL", "Antoinemaxwell0@gmail.com")

# Optional callback to WordPress (Forminator) to update entry status
# (Your Forminator form can store a hidden field with entry_id & callback URL)
CALLBACK_SHARED_SECRET = os.getenv("CALLBACK_SHARED_SECRET", "")  # if you want to verify
DEFAULT_CALLBACK_URL = os.getenv("DEFAULT_CALLBACK_URL", "")       # optional

# Security (optional): reCAPTCHA
RECAPTCHA_SECRET = os.getenv("RECAPTCHA_SECRET", "")  # if you want to verify captcha tokens

# Length limits
MIN_SECONDS = 15
MAX_SECONDS = 240  # 4 minutes

# Allowed content types (adjust as you like)
ALLOWED_CONTENT_TYPES = {
    "video/mp4",
    "video/quicktime",
    "video/x-matroska",
    "video/webm",
    "video/3gpp",
}

# -------------------------------------------------
# AWS clients
# -------------------------------------------------
session_kwargs = dict(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

s3_client = boto3.client("s3", **session_kwargs)
rekognition = boto3.client("rekognition", **session_kwargs)
ses_client = boto3.client("ses", **session_kwargs)

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def verify_recaptcha(token: str) -> bool:
    """Optional reCAPTCHA verification."""
    if not RECAPTCHA_SECRET:
        return True
    try:
        r = requests.post(
            "https://www.google.com/recaptcha/api/siteverify",
            data={"secret": RECAPTCHA_SECRET, "response": token},
            timeout=8,
        )
        data = r.json()
        return bool(data.get("success"))
    except Exception:
        return False

def get_video_duration(file_path):
    """Return video duration in seconds using ffprobe, if available."""
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                file_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60,
        )
        out = result.stdout.decode("utf-8", errors="ignore").strip()
        return float(out) if out else None
    except Exception as e:
        print("FFprobe error:", e)
        return None

def send_email_ses(to_address: str, subject: str, body: str):
    """Send a plain-text email via SES."""
    if not to_address:
        return
    try:
        ses_client.send_email(
            Source=NOTIFY_FROM_EMAIL,
            Destination={"ToAddresses": [to_address]},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": body}},
            },
        )
    except ClientError as e:
        print("SES email error:", e)

def notify_submitter_and_internal(email: str, subject: str, body: str):
    send_email_ses(email, subject, body)
    if NOTIFY_INTERNAL_EMAIL and NOTIFY_INTERNAL_EMAIL.lower() != email.lower():
        send_email_ses(NOTIFY_INTERNAL_EMAIL, f"[INTERNAL] {subject}", body)

def post_wp_callback(callback_url: str, payload: dict):
    """Optional: push status back to WP/Forminator."""
    if not callback_url:
        return
    try:
        headers = {"Content-Type": "application/json"}
        if CALLBACK_SHARED_SECRET:
            headers["X-Callback-Secret"] = CALLBACK_SHARED_SECRET
        requests.post(callback_url, headers=headers, data=json.dumps(payload), timeout=10)
    except Exception as e:
        print("Callback error:", e)

def s3_object_exists(bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

def sanitize_metadata(meta: dict) -> dict:
    """S3 metadata values must be strings and reasonably small."""
    clean = {}
    for k, v in (meta or {}).items():
        if v is None:
            continue
        s = str(v)
        # S3 metadata is limited; keep it modest
        clean[k[:32].lower()] = s[:1024]
    return clean

# -------------------------------------------------
# Moderation worker
# -------------------------------------------------
def moderate_video(temp_key, filename, metadata, content_type, callback_url):
    """
    - Validates duration
    - Calls Rekognition Content Moderation
    - Moves accepted videos to PERM bucket (public-read)
    - Deletes rejected videos from TEMP bucket
    - Emails submitter + internal
    - Posts optional callback to WP
    """
    email = metadata.get("email", "")
    submitter_name = metadata.get("name", "")
    entry_id = metadata.get("entry_id", "")  # if you pass it from Forminator
    ig_handle = metadata.get("ig_handle", "")
    video_type = metadata.get("videotype", "")
    comments = metadata.get("comments", "")

    # Ensure object still exists
    if not s3_object_exists(TEMP_BUCKET, temp_key):
        msg = f"Temp object missing: {temp_key}"
        print(msg)
        notify_submitter_and_internal(email, "Upload Error", msg)
        post_wp_callback(callback_url, {
            "status": "error",
            "reason": "temp_missing",
            "entry_id": entry_id,
            "temp_key": temp_key,
            "timestamp": now_iso(),
        })
        return

    # Download locally to probe duration (best-effort)
    tmp_file = f"/tmp/{uuid.uuid4()}_{os.path.basename(filename)}"
    try:
        s3_client.download_file(TEMP_BUCKET, temp_key, tmp_file)
    except Exception as e:
        print("Download error:", e)
        notify_submitter_and_internal(email, "Upload Error", "We could not retrieve your file for moderation.")
        post_wp_callback(callback_url, {
            "status": "error",
            "reason": "download_failed",
            "entry_id": entry_id,
            "temp_key": temp_key,
            "timestamp": now_iso(),
        })
        # Try to clean up temp object anyway
        try:
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
        except Exception:
            pass
        return

    # Check duration
    duration = get_video_duration(tmp_file)
    if duration is None:
        # If we can't measure, be conservative and reject (or you can accept and rely solely on Rekognition)
        print("Duration unknown -> rejecting to be safe.")
        try:
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
        except Exception:
            pass
        notify_submitter_and_internal(
            email,
            "Video Rejected",
            "We couldn't verify your video duration. Please re-submit a valid video (15s–4m).",
        )
        post_wp_callback(callback_url, {
            "status": "rejected",
            "reason": "duration_unknown",
            "entry_id": entry_id,
            "temp_key": temp_key,
            "timestamp": now_iso(),
        })
        try:
            os.remove(tmp_file)
        except Exception:
            pass
        return

    if duration < MIN_SECONDS or duration > MAX_SECONDS:
        print(f"Rejected by length: {duration:.2f}s")
        try:
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
        except Exception:
            pass
        notify_submitter_and_internal(
            email,
            "Video Rejected",
            f"Your video duration is {int(duration)} seconds. We accept videos between {MIN_SECONDS}s and {MAX_SECONDS}s.",
        )
        post_wp_callback(callback_url, {
            "status": "rejected",
            "reason": "duration_out_of_range",
            "duration_seconds": duration,
            "entry_id": entry_id,
            "temp_key": temp_key,
            "timestamp": now_iso(),
        })
        try:
            os.remove(tmp_file)
        except Exception:
            pass
        return

    # Start Rekognition Content Moderation
    try:
        response = rekognition.start_content_moderation(
            Video={"S3Object": {"Bucket": TEMP_BUCKET, "Name": temp_key}},
            MinConfidence=90,
        )
        job_id = response["JobId"]
    except Exception as e:
        print("Rekognition start error:", e)
        try:
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
        except Exception:
            pass
        notify_submitter_and_internal(
            email,
            "Video Rejected",
            "There was a problem analyzing your video. Please try again.",
        )
        post_wp_callback(callback_url, {
            "status": "rejected",
            "reason": "rekognition_start_failed",
            "entry_id": entry_id,
            "temp_key": temp_key,
            "timestamp": now_iso(),
        })
        try:
            os.remove(tmp_file)
        except Exception:
            pass
        return

    # Poll for result
    status = "IN_PROGRESS"
    result = {}
    while True:
        time.sleep(5)
        try:
            result = rekognition.get_content_moderation(JobId=job_id, SortBy="TIMESTAMP")
            status = result.get("JobStatus")
            if status in ("SUCCEEDED", "FAILED"):
                break
        except Exception as e:
            print("Rekognition poll error:", e)
            break

    moderation_labels = result.get("ModerationLabels", []) if isinstance(result, dict) else []

    # Clean temp file
    try:
        os.remove(tmp_file)
    except Exception:
        pass

    if status != "SUCCEEDED":
        # Treat as reject
        print("Rekognition job failed -> reject")
        try:
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
        except Exception:
            pass
        notify_submitter_and_internal(
            email,
            "Video Rejected",
            "Your video could not be analyzed. Please try again with a different file.",
        )
        post_wp_callback(callback_url, {
            "status": "rejected",
            "reason": "rekognition_failed",
            "entry_id": entry_id,
            "temp_key": temp_key,
            "timestamp": now_iso(),
        })
        return

    if moderation_labels:
        # Reject + delete from temp
        print(f"Rejected by Rekognition: {len(moderation_labels)} labels")
        try:
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
        except Exception:
            pass
        notify_submitter_and_internal(
            email,
            "Video Rejected",
            "Your video was flagged by our safety filters and cannot be posted.",
        )
        post_wp_callback(callback_url, {
            "status": "rejected",
            "reason": "rekognition_labels",
            "labels_count": len(moderation_labels),
            "entry_id": entry_id,
            "temp_key": temp_key,
            "timestamp": now_iso(),
        })
        return

    # ACCEPT: move to permanent bucket
    try:
        perm_key = f"videos/{datetime.utcnow().strftime('%Y/%m/%d')}/{uuid.uuid4()}_{os.path.basename(filename)}"
        copy_source = {"Bucket": TEMP_BUCKET, "Key": temp_key}

        # Preserve useful metadata
        new_metadata = sanitize_metadata({
            "email": email,
            "name": submitter_name,
            "ig_handle": ig_handle,
            "videotype": video_type,
            "comments": comments,
            "submitted_at": now_iso(),
            "duration_sec": str(int(duration)),
            "content_type": content_type or "",
        })

        s3_client.copy_object(
            Bucket=PERM_BUCKET,
            Key=perm_key,
            CopySource=copy_source,
            ACL="public-read",  # or "private" + generate signed URLs on demand
            Metadata=new_metadata,
            MetadataDirective="REPLACE",
            ContentType=content_type or "application/octet-stream",
        )
        # Delete temp
        s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)

        video_url = f"https://{PERM_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{perm_key}"
        print(f"Approved and moved -> {video_url}")

        # Emails
        notify_submitter_and_internal(
            email,
            "Video Accepted",
            f"Congrats! Your video was accepted and queued for posting.\n\nLink: {video_url}",
        )

        # Callback
        post_wp_callback(callback_url or DEFAULT_CALLBACK_URL, {
            "status": "accepted",
            "entry_id": entry_id,
            "video_url": video_url,
            "perm_key": perm_key,
            "duration_seconds": int(duration),
            "timestamp": now_iso(),
        })

    except Exception as e:
        print("Move-to-perm error:", e)
        # Best-effort cleanup
        try:
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
        except Exception:
            pass
        notify_submitter_and_internal(
            email,
            "Video Rejected",
            "We had an issue finalizing your upload. Please try again.",
        )
        post_wp_callback(callback_url, {
            "status": "rejected",
            "reason": "move_failed",
            "entry_id": entry_id,
            "temp_key": temp_key,
            "timestamp": now_iso(),
        })

# -------------------------------------------------
# API: get pre-signed URL
# -------------------------------------------------
@app.route("/get-upload-url", methods=["POST"])
def get_upload_url():
    """
    Request JSON:
    {
      "email": "...",               (required)
      "name": "...",                (optional)
      "igHandle": "...",            (optional)
      "videoType": "...",           (required)  e.g., "music", "news", "ugc"
      "comments": "...",            (optional)
      "consent": true,              (required)  (they agree to publish/monetize)
      "filename": "clip.mp4",       (required)
      "contentType": "video/mp4",   (required)
      "entryId": "12345",           (optional)  (WP/Forminator entry id)
      "callbackUrl": "https://...", (optional)  (WP endpoint to flip status)
      "recaptchaToken": "..."       (optional)  (if you enable captcha)
    }
    """
    data = request.get_json() or {}
    email = (data.get("email") or "").strip()
    name = (data.get("name") or "").strip()
    ig_handle = (data.get("igHandle") or "").strip()
    video_type = (data.get("videoType") or "").strip().lower()
    comments = (data.get("comments") or "").strip()
    consent = bool(data.get("consent", True))
    filename = (data.get("filename") or "").strip()
    content_type = (data.get("contentType") or "").strip().lower()
    entry_id = (data.get("entryId") or "").strip()
    callback_url = (data.get("callbackUrl") or DEFAULT_CALLBACK_URL).strip()
    recaptcha_token = (data.get("recaptchaToken") or "").strip()

    # Basic validations
    if not email or not video_type or not consent or not filename or not content_type:
        return jsonify({"status": "error", "message": "Missing required fields"}), 400

    if content_type not in ALLOWED_CONTENT_TYPES:
        return jsonify({"status": "error", "message": "Unsupported content type"}), 400

    if recaptcha_token and not verify_recaptcha(recaptcha_token):
        return jsonify({"status": "error", "message": "Captcha failed"}), 400

    # Key namespaced by date for cleanliness
    temp_key = f"uploads/{datetime.utcnow().strftime('%Y/%m/%d')}/{uuid.uuid4()}_{os.path.basename(filename)}"

    user_metadata = sanitize_metadata({
        "email": email,
        "name": name,
        "ig_handle": ig_handle,
        "videotype": video_type,
        "comments": comments,
        "entry_id": entry_id,
        "filename": filename,
        "requested_at": now_iso(),
    })

    try:
        # Pre-signed PUT URL (simple). If you want size limits/conditions, switch to Presigned POST.
        presigned_url = s3_client.generate_presigned_url(
            ClientMethod="put_object",
            Params={
                "Bucket": TEMP_BUCKET,
                "Key": temp_key,
                "ACL": "private",
                "ContentType": content_type,
                "Metadata": user_metadata,
            },
            ExpiresIn=3600,
        )
        return jsonify({
            "status": "success",
            "upload_url": presigned_url,
            "temp_key": temp_key,
            "content_type": content_type,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# -------------------------------------------------
# API: confirm upload (kick off moderation)
# -------------------------------------------------
@app.route("/confirm-upload", methods=["POST"])
def confirm_upload():
    """
    Request JSON:
    {
      "temp_key": "...",         (required)
      "filename": "clip.mp4",    (required)
      "email": "...",            (required)
      "name": "...",             (optional)
      "igHandle": "...",         (optional)
      "videoType": "...",        (required)
      "comments": "...",         (optional)
      "entryId": "12345",        (optional)
      "contentType": "video/mp4",(required - must match what you PUT)
      "callbackUrl": "https://..." (optional)
    }
    """
    data = request.get_json() or {}
    temp_key = (data.get("temp_key") or "").strip()
    filename = (data.get("filename") or "").strip()
    email = (data.get("email") or "").strip()
    name = (data.get("name") or "").strip()
    ig_handle = (data.get("igHandle") or "").strip()
    video_type = (data.get("videoType") or "").strip().lower()
    comments = (data.get("comments") or "").strip()
    entry_id = (data.get("entryId") or "").strip()
    content_type = (data.get("contentType") or "").strip().lower()
    callback_url = (data.get("callbackUrl") or DEFAULT_CALLBACK_URL).strip()

    if not all([temp_key, filename, email, video_type, content_type]):
        return jsonify({"status": "error", "message": "Missing required fields"}), 400

    if content_type not in ALLOWED_CONTENT_TYPES:
        return jsonify({"status": "error", "message": "Unsupported content type"}), 400

    metadata = sanitize_metadata({
        "email": email,
        "name": name,
        "ig_handle": ig_handle,
        "videotype": video_type,
        "comments": comments,
        "entry_id": entry_id,
        "filename": filename,
        "confirmed_at": now_iso(),
    })

    # Kick off moderation in a background thread
    threading.Thread(
        target=moderate_video,
        args=(temp_key, filename, metadata, content_type, callback_url),
        daemon=True,
    ).start()

    return jsonify({"status": "success", "message": "Moderation started"})

# -------------------------------------------------
# Health
# -------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "region": AWS_REGION, "time": now_iso()})

# -------------------------------------------------
# Run
# -------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)