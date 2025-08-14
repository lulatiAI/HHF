from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
from flasgger import Swagger, swag_from
import os
import stripe
import boto3

app = Flask(__name__)
CORS(app)
Swagger(app)

# -------------------------
# Environment & API Setup
# -------------------------
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")
aws_region = os.getenv("AWS_REGION", "us-east-1")
s3_bucket_name = os.getenv("AWS_S3_BUCKET", "your-s3-bucket-name")

s3_client = boto3.client(
    "s3",
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
# Upload Video & Capture Payment
# -------------------------
@app.route("/upload-video", methods=["POST"])
@swag_from({
    'parameters': [
        {
            'name': 'file',
            'in': 'formData',
            'type': 'file',
            'required': True,
            'description': 'Video file to upload'
        },
        {
            'name': 'stripe_payment_method_id',
            'in': 'formData',
            'type': 'string',
            'required': True,
            'description': 'Stripe Payment Method ID for paying users'
        }
    ],
    'responses': {
        200: {'description': 'Video accepted and payment processed'},
        400: {'description': 'Video rejected or error'}
    }
})
def upload_video():
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file provided"}), 400

    file = request.files['file']
    filename = file.filename
    stripe_payment_method_id = request.form.get("stripe_payment_method_id")

    # 1️⃣ Upload to S3
    try:
        s3_client.upload_fileobj(file, s3_bucket_name, filename)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    # 2️⃣ Run moderation (simulate acceptance)
    accepted = True  # replace with AWS Rekognition moderation logic

    if not accepted:
        # Delete from S3 if rejected
        s3_client.delete_object(Bucket=s3_bucket_name, Key=filename)
        return jsonify({"status": "rejected", "message": "Video rejected by moderation"}), 400

    # 3️⃣ Capture payment via Stripe
    try:
        intent = stripe.PaymentIntent.create(
            amount=1000,  # $10 per video
            currency='usd',
            payment_method=stripe_payment_method_id,
            confirm=True
        )
        payment_status = intent.status
    except Exception as e:
        # Delete video if payment fails
        s3_client.delete_object(Bucket=s3_bucket_name, Key=filename)
        return jsonify({"status": "error", "message": f"Payment failed: {str(e)}"}), 400

    video_url = f"https://{s3_bucket_name}.s3.{aws_region}.amazonaws.com/{filename}"
    return jsonify({
        "status": "success",
        "video_url": video_url,
        "payment_status": payment_status
    })

# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
