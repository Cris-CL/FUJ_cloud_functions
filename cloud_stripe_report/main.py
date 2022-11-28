import json
import os
import stripe
import requests
from google.cloud import storage
from flask import Flask, jsonify, request

# This is your Stripe CLI webhook secret for testing your endpoint locally.

endpoint_secret = os.environ.get('web_secret')
api_key_local = os.environ.get('stripe_key')

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook(request):
    event = None
    payload = request.data
    sig_header = request.headers['STRIPE_SIGNATURE']

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, endpoint_secret
        )
    except ValueError as e:
        # Invalid payload
        raise e
    except stripe.error.SignatureVerificationError as e:
        # Invalid signature
        raise e

    # Handle the event
    if event['type'] == 'reporting.report_run.succeeded':
        report_run = event['data']['object']
    # ... handle other event types
        file_name = report_run["result"]["filename"]
        url_report = report_run["result"]["url"]

        file_api_rep = requests.get(url_report,auth=(api_key_local,api_key_local)).text

        storage_client = storage.Client()
        bucket_name = 'fujiorg-stripe-raw'
        if report_run['report_type'] != "balance_change_from_activity.itemized.2":
            bucket_name = 'fujiorg-sales-data'
        bucket = storage_client.list_buckets().client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(file_api_rep,content_type = 'csv')
    else:
      print('Unhandled event type {}'.format(event['type']))

    return jsonify(success=True)
