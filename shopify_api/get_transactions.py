# requirements.txt
# pandas>=1.2.2
# google-cloud-storage==1.44.0
# google-cloud-bigquery>=3.3.5
# pandas-gbq>=0.17.9

import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, date

apikey = os.environ.get("SHOPIFY_KEY")
api_version = os.environ.get("API_VERSION")
password = os.environ.get("SHOPIFY_PASS")
bucket_name = os.environ.get("BUCKET")
project_name = os.environ.get("PROJECT_NAME")
table_name = os.environ.get("TABLE_NAME")
shop_name = os.environ.get("SHOP_NAME")

def get_all_payouts(last_order_id):
    """
    Function that calls the api, when provided last_order_id, loops throught all the orders
    since that id, and appends them to a dataframe
    """

    last = last_order_id  ##first order_id 2270011949127

    limit = 250

    transactions = pd.DataFrame()

    print(f"First order_id: {last}")
    while True:
        url = f"https://{apikey}:{password}@{shop_name}.myshopify.com/admin/api/{api_version}/shopify_payments/balance/transactions.json?limit={limit}&since_id={last}"
        response_in = requests.get(url)
        df = pd.json_normalize(response_in.json()["transactions"])

        # Function finishes with no new info
        if len(df) < 1:
            print("Api didnt provide new data")
            return last

        transactions = pd.concat([transactions, df], ignore_index=True)
        last = df["id"].iloc[-1]

        if len(df) < limit:
            print(f"Last order_id: {last}")
            print(len(transactions))
            break
    return transactions

def main(data, context):
    """
    whole process from check the last payout, to make api calls until the data is updated, and save that data as a
    csv file in the bucket while uploading the same data to the orders table in BigQuery
    """
    ## Delete pending of payment transactions before getting the new paid ones



    query_0 = f"""DELETE FROM `test-bigquery-cc.Shopify.{table_name}` Where payout_status in ('pending','in_transit')"""

    try:
        with bigquery.Client() as BQ:
            query_job_0 = BQ.query(query_0)
            query_job_0.result()
    except:
        print("Couldnt remove pending transactions")


    ## query select the id correspondig to the last order in the table
    query = f"""
    select distinct id
    FROM `{project_name}.Shopify.{table_name}`
    Where id = (SELECT max(id) FROM `{project_name}.Shopify.{table_name}`)
    """

    try:
        bigquery_client = bigquery.Client()
        query_job = bigquery_client.query(query)  # Make an API request.
        rows = query_job.result()  # Waits for query to finish
        result = list(rows)[0]["id"]  ## last id registered in pay table
    except:
        print("starting from first transaction")
        result = 0
    df = get_all_payouts(result)  ## 2270011949127 --> Reference id that works

    if type(df) != type(pd.DataFrame()):
        return print("No new data to add")
    # Clean data

    df = df.astype(
    {
    "payout_id":"str",
    "source_id":"str",
    "source_order_id":"str",
    "source_order_transaction_id":"str",

    "amount":"float64",
    "fee":"float64",
    "net":"float64",

    "processed_at":"datetime64[ns]"
                     },)

    for col in df.columns:
      df[col] = df[col].map(lambda x: None if x in ["nan", "", "None", "null","NaN","NAN"] else x)
      df[col] = df[col].apply(lambda x: x.replace(".0","") if type(x) ==type("") else x)

    ## Add time of creation
    df["UPDATED_FROM_API"] = datetime.utcnow()

    today_date = date.today().strftime("%Y_%m_%d")
    file_name = f"SHOPIFY_PAYOUTS_{today_date}_{result}_RAW.csv"

    ## Upload to BQ
    df.to_gbq(
        destination_table=f"Shopify.{table_name}",
        project_id=project_name,
        progress_bar=False,
        if_exists="append",
    )  ### should be append

    ## Upload to bucket

    storage_client = storage.Client()
    bucket = storage_client.list_buckets().client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(df.to_csv(index=False), content_type="csv/txt")
    return
