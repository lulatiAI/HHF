from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import boto3
import uuid
import pathlib
import mimetypes
import threading
import time
import os
import subprocess
import logging
from typing import Optional, Dict, List

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

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
# FastAPI app
# -------------------------
app = FastAPI(title="Video Upload API with Moderation")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Request logging middleware (shows up in Render logs)
# -------------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"➡️ {request.method} {request.url}")
    try:
        response = await call_next(request)
        logger.info(f"⬅️ {request.method} {request.url} - {response.status_code}")
        return response
    except Exception as e:
        logger.exception(f"🔥 Unhandled error for {request.method} {request.url}: {e}")
        raise

# -------------------------
# Root route (fixes 404 at '/')
# -------------------------
@app.get("/")
async def root():
    return {"status": "ok", "message": "FastAPI is running 🚀"}

# -------------------------
# Pydantic models
# -------------------------
class UploadRequest(BaseModel):
    filename: str
    email: str
    videoType: str
    consent: Optional[bool] = True
    comments: Optional[str] = ""

class ConfirmUploadRequest(BaseModel):
    temp_key: str
    filename: str
    email: str
    videoType: str
    comments: Optional[str] = ""

# -------------------------
# Helpers
# -------------------------
def check_ffmpeg() -> bool:
    try:
        subprocess.run(["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except Exception as e:
        logger.warning(f"FFmpeg check failed (likely not installed): {e}")
        return False

def guess_content_type(filename: str) -> str:
    ctype, _ = mimetypes.guess_type(filename)
    return ctype or "application/octet-stream"

def is_video(filename: str) -> bool:
    return pathlib.Path(filename.lower()).suffix in VIDEO_EXTS

def is_image(filename: str) -> bool:
    return pathlib.Path(filename.lower()).suffix in IMAGE_EXTS

def generate_presigned_get(bucket: str, key: str, expires: int = 3600) -> Optional[str]:
    try:
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires,
        )
        return url
    except Exception as e:
        logger.error(f"Error generating presigned GET URL: {e}")
        return None

def reencode_video(local_path: str, output_path: str) -> bool:
    """Re-encode video to MP4 H.264 + AAC for broad compatibility"""
    if not check_ffmpeg():
        logger.error("FFmpeg is not installed on the server. Re-encode will be skipped and fail.")
        return False
    try:
        cmd = [
            "ffmpeg",
            "-i", local_path,
            "-c:v", "libx264",
            "-preset", "fast",
            "-movflags", "+faststart",
            "-c:a", "aac",
            "-b:a", "128k",
            output_path,
            "-y"
        ]
        cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        logger.info(f"FFmpeg re-encode complete: {output_path}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg re-encode failed (returncode {e.returncode}): {e.stderr.decode(errors='ignore')[:600]}")
        return False
    except Exception as e:
        logger.exception(f"FFmpeg re-encode unexpected error: {e}")
        return False

def approve_and_move(temp_key: str, filename: str, metadata: Dict[str, str]):
    try:
        # Download temp video
        local_temp = f"/tmp/{uuid.uuid4()}_{filename}"
        logger.info(f"Downloading from s3://{TEMP_BUCKET}/{temp_key} -> {local_temp}")
        s3_client.download_file(TEMP_BUCKET, temp_key, local_temp)

        # Re-encode to mp4
        new_filename = pathlib.Path(filename).stem + ".mp4"
        local_out = f"/tmp/{uuid.uuid4()}_{new_filename}"
        ok = reencode_video(local_temp, local_out)
        if not ok:
            logger.error("Re-encode failed; aborting move to permanent bucket.")
            return None, None

        # Upload to permanent bucket
        perm_key = f"{uuid.uuid4()}_{new_filename}"
        logger.info(f"Uploading to s3://{PERM_BUCKET}/{perm_key}")
        s3_client.upload_file(
            local_out,
            PERM_BUCKET,
            perm_key,
            ExtraArgs={"Metadata": metadata, "ContentType": "video/mp4"},
        )
        # Clean up temp
        s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)

        presigned_url = generate_presigned_get(PERM_BUCKET, perm_key)
        return perm_key, presigned_url
    except Exception as e:
        logger.exception(f"Error moving/re-encoding file: {e}")
        return None, None

def moderate_video(temp_key: str, filename: str, metadata: Dict[str, str], callback):
    try:
        approved = False

        if is_video(filename):
            logger.info(f"Starting Rekognition content moderation (video) for key: {temp_key}")
            response = rekognition.start_content_moderation(
                Video={"S3Object": {"Bucket": TEMP_BUCKET, "Name": temp_key}},
                MinConfidence=90,
            )
            job_id = response["JobId"]
            while True:
                result = rekognition.get_content_moderation(JobId=job_id)
                status = result.get("JobStatus")
                if status in ["SUCCEEDED", "FAILED"]:
                    logger.info(f"Rekognition job {job_id} finished with status: {status}")
                    break
                time.sleep(5)
            labels = result.get("ModerationLabels", [])
            approved = not labels  # approve if no labels
        elif is_image(filename):
            logger.info(f"Running Rekognition detect_moderation_labels (image) for key: {temp_key}")
            result = rekognition.detect_moderation_labels(
                Image={"S3Object": {"Bucket": TEMP_BUCKET, "Name": temp_key}},
                MinConfidence=90,
            )
            approved = not result.get("ModerationLabels")
        else:
            logger.info("Non-video/image file; auto-approving.")
            approved = True

        if approved:
            perm_key, presigned_url = approve_and_move(temp_key, filename, metadata)
            callback(success=bool(perm_key and presigned_url), video_url=presigned_url)
        else:
            logger.info("Content rejected by moderation; deleting temp object.")
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
            callback(success=False, video_url=None)
    except Exception as e:
        logger.exception(f"Moderation error: {e}")
        try:
            s3_client.delete_object(Bucket=TEMP_BUCKET, Key=temp_key)
        except Exception:
            pass
        callback(success=False, video_url=None)

# -------------------------
# Endpoints
# -------------------------
@app.get("/test")
def test():
    ffmpeg_status = check_ffmpeg()
    return {"status": "ok", "message": "Server live", "ffmpeg_installed": ffmpeg_status}

@app.post("/get-upload-url")
def get_upload_url(req: UploadRequest):
    if not req.filename or not req.email or not req.videoType or not req.consent:
        raise HTTPException(status_code=400, detail="Missing required fields")
    temp_key = f"{uuid.uuid4()}_{req.filename}"
    content_type = guess_content_type(req.filename)
    try:
        logger.info(f"Generating presigned PUT for {temp_key} (Content-Type: {content_type})")
        presigned_url = s3_client.generate_presigned_url(
            "put_object",
            Params={"Bucket": TEMP_BUCKET, "Key": temp_key, "ContentType": content_type},
            ExpiresIn=3600,
        )
        return {
            "status": "success",
            "upload_url": presigned_url,
            "temp_key": temp_key,
            "required_headers": {"Content-Type": content_type},
        }
    except Exception as e:
        logger.exception("Error generating upload URL")
        raise HTTPException(status_code=500, detail="Failed to generate upload URL")

@app.post("/confirm-upload")
def confirm_upload(req: ConfirmUploadRequest):
    metadata = {"email": req.email, "videoType": req.videoType, "comments": req.comments}
    result_data: Dict[str, Optional[str]] = {}

    def callback(success: bool, video_url: Optional[str]):
        result_data["success"] = success
        result_data["video_url"] = video_url

    logger.info(f"Confirming upload temp_key={req.temp_key} filename={req.filename}")
    thread = threading.Thread(target=moderate_video, args=(req.temp_key, req.filename, metadata, callback))
    thread.start()
    thread.join()

    if result_data.get("success"):
        return {"status": "success", "video_url": result_data["video_url"]}
    else:
        raise HTTPException(status_code=400, detail="Video failed moderation or processing")

@app.get("/list-temp-files")
def list_temp_files() -> List[str]:
    try:
        response = s3_client.list_objects_v2(Bucket=TEMP_BUCKET)
        files = response.get("Contents", [])
        return [generate_presigned_get(TEMP_BUCKET, obj["Key"]) for obj in files if obj]
    except Exception as e:
        logger.exception("Error listing temp files")
        raise HTTPException(status_code=500, detail="Failed to list temp files")

@app.get("/list-perm-files")
def list_perm_files() -> List[str]:
    try:
        response = s3_client.list_objects_v2(Bucket=PERM_BUCKET)
        files = response.get("Contents", [])
        return [generate_presigned_get(PERM_BUCKET, obj["Key"]) for obj in files if obj]
    except Exception as e:
        logger.exception("Error listing perm files")
        raise HTTPException(status_code=500, detail="Failed to list perm files")