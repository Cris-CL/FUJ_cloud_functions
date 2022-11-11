import os
import pandas as pd
from google.cloud import storage
import stripe
import datetime as dtm

def stripe_to_gcs(fillers_1, filename):
    stripe.api_key = os.getenv('stripe_key')
    charges = stripe.Charge.list(limit=100)
    stripe_charges = pd.json_normalize(charges['data'])
    stripe_charges['created'] = stripe_charges['created'].apply(lambda x: dtm.datetime.fromtimestamp(x))

    storage_client = storage.Client()
    bucket = storage_client.list_buckets().client.bucket('test-bucket-function-cc')
    blob = bucket.blob(filename)
    blob.upload_from_string(stripe_charges.to_csv(index = False),content_type = 'csv')

def main(data,context):
    now = dtm.datetime.now().strftime("%Y_%m_%d")
    file_name = f'stripe_{now}.csv'
    stripe_to_gcs('hopefully_works',file_name)
