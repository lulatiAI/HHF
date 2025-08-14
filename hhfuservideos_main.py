from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import os
import stripe
import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)
CORS(app)

# -------------------------
# Environment & API Setup
# -------------------------
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")
aws_region = os.getenv("AWS_REGION", "us-east-1")
s3_bucket = os.getenv("S3_BUCKET_NAME")

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

        <h3>Upload Video/Image</h3>
        <form id="uploadForm">
            <input type="file" name="file" id="fileInput" required />
            <input type="text" name="user_id" placeholder="User ID (optional)" />
            <label>
                Paid User?
                <input type="checkbox" id="paidUser" />
            </label>
            <button type="submit">Upload</button>
        </form>

        <pre id="output"></pre>

        <script>
            function testVideo() {
                fetch('/test-video')
                    .then(res => res.json())
                    .then(data => { document.getElementById('output').innerText = JSON.stringify(data, null, 2); });
            }

            function testPayment() {
                fetch('/test-payment')
                    .then(res => res.json())
                    .then(data => { document.getElementById('output').innerText = JSON.stringify(data, null, 2); });
            }

            document.getElementById('uploadForm').onsubmit = function(e) {
                e.preventDefault();
                const formData = new FormData();
                const file = document.getElementById('fileInput').files[0];
                formData.append('file', file);
                formData.append('user_id', e.target.user_id.value || 'guest');
                formData.append('paid_user', document.getElementById('paidUser').checked);

                fetch('/upload', { method: 'POST', body: formData })
                    .then(res => res.json())
                    .then(data => { document.getElementById('output').innerText = JSON.stringify(data, null, 2); })
                    .catch(err => { document.getElementById('output').innerText = err; });
            };
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
# Upload Endpoint (Real Videos/Images)
# -------------------------
@app.route("/upload", methods=["POST"])
def upload_file():
    file = request.files.get("file")
    user_id = request.form.get("user_id", "guest")
    paid_user = request.form.get("paid_user", "false") == "true"

    if not file:
        return jsonify({"status": "error", "message": "No file uploaded"}), 400

    file_key = f"uploads/{user_id}/{file.filename}"

    try:
        # Upload to S3 first
        s3_client.upload_fileobj(file, s3_bucket, file_key)
        
        # Call Rekognition (content moderation)
        rekog_response = rekognition_client.detect_moderation_labels(
            Image={'S3Object': {'Bucket': s3_bucket, 'Name': file_key}},
            MinConfidence=80
        )
        
        # If moderation labels exist, reject
        if rekog_response.get("ModerationLabels"):
            # Delete file if rejected
            s3_client.delete_object(Bucket=s3_bucket, Key=file_key)
            return jsonify({"status": "rejected", "message": "Content not allowed"}), 400

        # Otherwise, accepted
        s3_url = f"https://{s3_bucket}.s3.{aws_region}.amazonaws.com/{file_key}"

        # TODO: Trigger Stripe payment if needed for paid users
        return jsonify({
            "status": "accepted",
            "s3_url": s3_url,
            "paid_user": paid_user
        })

    except ClientError as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
