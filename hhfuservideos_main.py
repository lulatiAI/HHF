from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import os
import stripe
import boto3
import uuid

app = Flask(__name__)
CORS(app)

# -------------------------
# Environment & API Setup
# -------------------------
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")
aws_region = os.getenv("AWS_REGION", "us-east-1")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

rekognition_client = boto3.client(
    "rekognition",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
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
        "message": "Video endpoint reachable!",
        "video_url": "https://example.com/test_video.mp4"
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
# Upload & Moderation Endpoint
# -------------------------
@app.route("/upload", methods=["POST"])
def upload_file():
    user_id = request.form.get("user_id", "guest")
    paid_user = request.form.get("paid_user", "false").lower() == "true"

    if "file" not in request.files:
        return jsonify({"status": "error", "message": "No file uploaded"}), 400

    file = request.files["file"]
    file_extension = file.filename.split(".")[-1]
    file_key = f"{user_id}/{uuid.uuid4()}.{file_extension}"

    # Upload to S3
    try:
        s3_client.upload_fileobj(file, s3_bucket_name, file_key)
    except Exception as e:
        return jsonify({"status": "error", "message": f"S3 upload failed: {str(e)}"}), 500

    # Run Rekognition moderation
    try:
        response = rekognition_client.detect_moderation_labels(
            Video={'S3Object': {'Bucket': s3_bucket_name, 'Name': file_key}},
            MinConfidence=75
        )
        labels = response.get("ModerationLabels", [])
        if labels:
            # Rejected content: delete from S3
            s3_client.delete_object(Bucket=s3_bucket_name, Key=file_key)
            return jsonify({
                "status": "rejected",
                "message": "Content rejected by moderation",
                "labels": labels
            })
    except Exception as e:
        return jsonify({"status": "error", "message": f"Rekognition failed: {str(e)}"}), 500

    # Accepted: construct URL
    file_url = f"https://{s3_bucket_name}.s3.{aws_region}.amazonaws.com/{file_key}"

    # Optionally, handle Stripe payment for paid users
    if paid_user:
        try:
            intent = stripe.PaymentIntent.create(
                amount=1000,  # Example $10
                currency='usd',
                payment_method_types=['card'],
                metadata={"user_id": user_id, "file_key": file_key}
            )
        except Exception as e:
            return jsonify({"status": "error", "message": f"Stripe payment failed: {str(e)}"}), 500
        return jsonify({
            "status": "accepted",
            "file_url": file_url,
            "payment_intent": intent
        })

    return jsonify({"status": "accepted", "file_url": file_url})

# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
