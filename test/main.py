import requests
import pandas as pd
from google.cloud import storage

def api_to_gcs(url, endpoint, filename):
    data = requests.get(url)
    json = data.json()
    df = pd.DataFrame(json[endpoint])
    storage_client = storage.Client()
    bucket = storage_client.list_buckets().client.bucket('test-bucket-function-cc')
    blob = bucket.blob(filename)
    blob.upload_from_string(df.to_csv(index = False),content_type = 'csv')

def main(data,context):
  api_to_gcs('https://fantasy.premierleague.com/api/bootstrap-static/','teams','teams.csv')
