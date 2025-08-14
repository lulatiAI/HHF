from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import os
import stripe
import boto3
import ffmpeg
from flasgger import Swagger, swag_from

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
@swag_from({
    "responses": {
        200: {
            "description": "Video endpoint reachable",
            "examples": {
                "application/json": {
                    "status": "success",
                    "message": "Video endpoint reachable!",
                    "video_url": "https://example.com/test_video.mp4"
                }
            }
        }
    }
})
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
@swag_from({
    "responses": {
        200: {
            "description": "Stripe PaymentIntent created",
        },
        400: {
            "description": "Stripe error",
        }
    }
})
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
# Video Generation Endpoint
# -------------------------
@app.route("/generate-video", methods=["POST"])
@swag_from({
    "parameters": [
        {
            "name": "body",
            "in": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "prompt_text": {"type": "string"},
                    "prompt_image": {"type": "string"}
                },
                "required": ["prompt_text", "prompt_image"]
            }
        }
    ],
    "responses": {
        200: {
            "description": "Generated video",
            "examples": {
                "application/json": {
                    "status": "success",
                    "prompt_text": "Sample text",
                    "prompt_image": "https://example.com/sample.png",
                    "video_url": "https://example.com/generated_video.mp4"
                }
            }
        }
    }
})
def generate_video():
    data = request.get_json()
    prompt_text = data.get("prompt_text")
    prompt_image = data.get("prompt_image")
    
    # TODO: integrate your AWS S3 + video processing logic
    return jsonify({
        "status": "success",
        "prompt_text": prompt_text,
        "prompt_image": prompt_image,
        "video_url": prompt_image  # For testing, returns the same video/image URL
    })

# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
