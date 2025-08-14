from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import os
import stripe
import boto3
import ffmpeg
from flasgger import Swagger

app = Flask(__name__)
CORS(app)

# Initialize Swagger
swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": "apispec",
            "route": "/apispec.json",
            "rule_filter": lambda rule: True,
            "model_filter": lambda tag: True,
        }
    ],
    "swagger_ui": True,
    "specs_route": "/docs/"
}
swagger = Swagger(app, config=swagger_config)

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
# Root Page with Video Preview
# -------------------------
@app.route("/")
def index():
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>HHF Video Generator</title>
    </head>
    <body>
        <h2>HHF Video Generator</h2>

        <label>Prompt Text:</label>
        <input type="text" id="prompt_text" value="A cat riding a skateboard"><br><br>

        <label>Image URL:</label>
        <input type="text" id="prompt_image" value="https://example.com/cat.png"><br><br>

        <button onclick="generateVideo()">Generate Video</button><br><br>

        <video id="video_player" width="480" height="270" controls autoplay>
            <source id="video_source" src="" type="video/mp4">
            Your browser does not support the video tag.
        </video>

        <pre id="output"></pre>

        <script>
            async function generateVideo() {
                const prompt_text = document.getElementById('prompt_text').value;
                const prompt_image = document.getElementById('prompt_image').value;

                const response = await fetch('/generate-video', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({prompt_text, prompt_image})
                });

                const data = await response.json();
                document.getElementById('output').innerText = JSON.stringify(data, null, 2);

                if (data.status === 'success') {
                    const video_url = data.video_url;
                    const videoSource = document.getElementById('video_source');
                    videoSource.src = video_url;
                    document.getElementById('video_player').load();
                }
            }
        </script>

        <p>Swagger docs available at <a href="/docs/">/docs/</a></p>
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
# Video Generation Endpoint
# -------------------------
@app.route("/generate-video", methods=["POST"])
def generate_video():
    data = request.get_json()
    prompt_text = data.get("prompt_text")
    prompt_image = data.get("prompt_image")

    # TODO: replace with real AI video generation logic
    # For now, return a dummy video URL for testing
    video_url = "https://example.com/generated_video.mp4"

    return jsonify({
        "status": "success",
        "prompt_text": prompt_text,
        "prompt_image": prompt_image,
        "video_url": video_url
    })

# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
