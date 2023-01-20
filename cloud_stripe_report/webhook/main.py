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
    payout_bucket = os.environ.get('STRIPE_PAYOUT_BUCKET')
    bucket_name = os.environ.get('STRIPE_BUCKET')
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
        file_name_var = report_run["result"]["filename"]
        url_report = report_run["result"]["url"]

        file_api_rep = requests.get(url_report,auth=(api_key_local,api_key_local)).text

        storage_client = storage.Client()

        if report_run['report_type'] == 'payout_reconciliation.itemized.5':
            file_name = f'PAY_{file_name_var}'
        if report_run['report_type'] == "balance_change_from_activity.itemized.2":
            file_name = f'BAL_{file_name_var}'
        bucket = storage_client.list_buckets().client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(file_api_rep,content_type = 'csv/txt')

    elif event['type'] == 'payout.paid':
        from payout_report import report_request
        pay_event = event['data']['object']
        arrival_date = pay_event['arrival_date']
        print(f"Payout with arrival date: {arrival_date} requested")
        report_request(arrival_date)
        return jsonify(success=True)

    else:
      print('Unhandled event type {}'.format(event['type']))

    return jsonify(success=True)
