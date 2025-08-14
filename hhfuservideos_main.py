# hhfuservideos_main.py

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import boto3
import stripe
import ffmpeg
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Stripe API key
stripe.api_key = os.getenv("STRIPE_API_KEY")

# AWS S3 setup
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# Safe root route
@app.route("/")
def root():
    return "Test environment — nothing to see here.", 200

# Example API route: video generation
@app.route("/generate-video", methods=["POST"])
def generate_video():
    try:
        data = request.get_json()
        prompt_text = data.get("prompt_text")
        prompt_image = data.get("prompt_image")

        # Your video generation logic goes here
        output_filename = "output.mp4"
        # Example placeholder: ffmpeg.input(prompt_image).output(output_filename).run()

        return jsonify({"video_url": f"https://example.com/{output_filename}"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Example API route: Stripe payment intent
@app.route("/create-payment-intent", methods=["POST"])
def create_payment_intent():
    try:
        data = request.get_json()
        amount = data.get("amount")

        intent = stripe.PaymentIntent.create(
            amount=amount,
            currency="usd",
        )

        return jsonify({"client_secret": intent.client_secret}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Catch-all route for any other URL
@app.errorhandler(404)
def page_not_found(e):
    return "Nothing to see here — invalid URL.", 404

# Run app (for local testing)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
