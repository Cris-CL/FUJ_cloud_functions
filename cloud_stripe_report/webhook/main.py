import json
import os
import stripe
import threading
from flask import Flask, jsonify, request
from process_report import process_report  # Import the function from process_report.py

# This is your Stripe CLI webhook secret for testing your endpoint locally.
endpoint_secret = os.environ.get("web_secret")
api_key_local = os.environ.get("stripe_key")

app = Flask(__name__)


@app.route("/webhook", methods=["POST"])
def webhook(request):
    event = None
    payload = request.data
    sig_header = request.headers["STRIPE_SIGNATURE"]
    payout_bucket = os.environ.get("STRIPE_PAYOUT_BUCKET")
    bucket_name = os.environ.get("STRIPE_BUCKET")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, endpoint_secret)
    except ValueError as e:
        # Invalid payload
        return jsonify({"error": "Invalid payload"}), 400
    except stripe.error.SignatureVerificationError as e:
        # Invalid signature
        return jsonify({"error": "Invalid signature"}), 400

    # Immediately return success response
    if event["type"] == "reporting.report_run.succeeded":
        threading.Thread(
            target=process_report,
            args=(event["data"]["object"], bucket_name, api_key_local),
        ).start()

    elif event["type"] == "payout.paid":
        pay_event = event["data"]["object"]
        arrival_date = pay_event["arrival_date"]
        print(f"Payout with arrival date: {arrival_date} arrived")

    else:
        print(f'Unhandled event type {event["type"]}')

    return jsonify(success=True)
