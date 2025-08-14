from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import os
import stripe
import boto3
from botocore.exceptions import ClientError
import uuid

from flasgger import Swagger, swag_from

app = Flask(__name__)
CORS(app)
Swagger(app)

# -------------------------
# Environment & API Setup
# -------------------------
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

rekognition_client = boto3.client(
    "rekognition",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# -------------------------
# Root Test Page
# -------------------------
@app.route("/")
def index():
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>HHF Test Page</title>
    </head>
    <body>
        <h2>HHF Backend Test Page</h2>
        <p>Use these buttons to test endpoints:</p>
        <button onclick="testVideo()">Test Video Endpoint</button>
        <button onclick="testPayment()">Test Stripe Payment</button>
        <pre id="output"></pre>
        
        <script>
            function testVideo() {
                fetch('/test-video')
                    .then(response => response.json())
                    .then(data => {
                        document.getElementById('output').innerText = JSON.stringify(data, null, 2);
                    });
            }
            function testPayment() {
                fetch('/test-payment')
                    .then(response => response.json())
                    .then(data => {
                        document.getElementById('output').innerText = JSON.stringify(data, null, 2);
                    });
            }
        </script>
    </body>
    </html>
    """
    return render_template_string(html)

# -------------------------
# Test Video Endpoint
# -------------------------
@app.route("/test-video")
def test_video():
    return jsonify({
        "status": "success",
        "message": "Video endpoint reachable!"
    })

# -------------------------
# Test Stripe Payment Endpoint
# -------------------------
@app.route("/test-payment")
def test_payment():
    try:
        intent = stripe.PaymentIntent.create(
            amount=1000,  # $10
            currency='usd',
            payment_method_types=['card']
        )
        return jsonify({"status": "success", "payment_intent": intent})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

# -------------------------
# Upload & Verify Video Endpoint
# -------------------------
@app.route("/upload-video", methods=["POST"])
def upload_video():
    """
    Upload a video to S3, verify with AWS Rekognition,
    and handle approval/rejection automatically.
    ---
    consumes:
      - multipart/form-data
    parameters:
      - name: file
        in: formData
        type: file
        required: true
        description: Video file to upload
      - name: user_id
        in: formData
        type: string
        required: true
        description: ID of the uploading user
      - name: paid
        in: formData
        type: boolean
        required: false
        description: Is this a paying user? Default false
    responses:
      200:
        description: Video uploaded and processed
      400:
        description: Error occurred
    """
    file = request.files.get("file")
    user_id = request.form.get("user_id")
    paid = request.form.get("paid", "false").lower() == "true"

    if not file or not user_id:
        return jsonify({"status": "error", "message": "Missing file or user_id"}), 400

    # Generate unique filename
    file_ext = os.path.splitext(file.filename)[1]
    file_key = f"videos/{user_id}/{uuid.uuid4()}{file_ext}"

    # Upload to S3 temporarily
    try:
        s3_client.upload_fileobj(file, S3_BUCKET, file_key)
    except ClientError as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    # Rekognition moderation check
    try:
        response = rekognition_client.detect_moderation_labels(
            Video={
                "S3Object": {"Bucket": S3_BUCKET, "Name": file_key}
            },
            MinConfidence=70
        )
        labels = response.get("ModerationLabels", [])
    except ClientError as e:
        # Delete temp file if Rekognition fails
        s3_client.delete_object(Bucket=S3_BUCKET, Key=file_key)
        return jsonify({"status": "error", "message": "Rekognition failed: " + str(e)}), 500

    # If any unsafe content is detected, reject
    if labels:
        s3_client.delete_object(Bucket=S3_BUCKET, Key=file_key)
        return jsonify({
            "status": "rejected",
            "message": "Video rejected due to inappropriate content",
            "labels": labels
        })

    # Video accepted
    video_url = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{file_key}"

    # Optionally handle Stripe payment if paid user
    payment_intent = None
    if paid:
        try:
            payment_intent = stripe.PaymentIntent.create(
                amount=1000,  # Example $10 payment
                currency='usd',
                payment_method_types=['card'],
                metadata={"user_id": user_id, "video_key": file_key}
            )
        except Exception as e:
            return jsonify({"status": "error", "message": f"Stripe payment failed: {str(e)}"}), 500

    # Return success
    return jsonify({
        "status": "accepted",
        "video_url": video_url,
        "payment_intent": payment_intent
    })

# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
