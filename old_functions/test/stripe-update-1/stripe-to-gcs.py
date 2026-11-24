import os
import pandas as pd
from google.cloud import storage
import datetime as dtm
import stripe

def stripe_to_gcs(fillers_1, filename):

    stripe.api_key = os.environ.get('stripe_key')
    charges = stripe.Charge.list(limit=100)
    data = pd.json_normalize(charges['data'])
    data['created'] = data['created'].apply(lambda x: dtm.datetime.fromtimestamp(x))

    storage_client = storage.Client()
    bucket = storage_client.list_buckets().client.bucket('test-bucket-function-cc')
    blob = bucket.blob(filename)
    blob.upload_from_string(data.to_csv(index = False),content_type = 'csv')

def main(data,context):
    now = dtm.datetime.now().strftime("%Y_%m_%d")
    name = now + '_stripe.csv'
    stripe_to_gcs('hopefully_works',name)
