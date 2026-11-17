# requirements.txt
# pandas>=1.2.2
# google-cloud-storage==1.44.0
# google-cloud-bigquery>=3.3.5
# pandas-gbq>=0.17.9
# numpy==1.23.4
# ShopifyAPI==12.5.0

import os
import requests
import pandas as pd
from google.cloud import bigquery, storage
from google.api_core import retry as api_retry
from datetime import datetime, date
import shopify
import json

SHOPIFY_KEY = os.environ.get("SHOPIFY_KEY")
API_VERSION = os.environ.get("API_VERSION")
SHOPIFY_PASS = os.environ.get("SHOPIFY_PASS")
BUCKET = os.environ.get("BUCKET")
PROJECT_NAME = os.environ.get("PROJECT_NAME")
TABLE_NAME_PAY = os.environ.get("TABLE_NAME_PAY")
STORE_NAME = os.environ.get("STORE_NAME")


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
        url = f"https://{SHOPIFY_KEY}:{SHOPIFY_PASS}@{STORE_NAME}.myshopify.com/admin/api/{API_VERSION}/shopify_payments/balance/transactions.json?limit={limit}&since_id={last}"
        try:
            response_in = requests.get(url, timeout=30)
            df = pd.json_normalize(response_in.json()["transactions"])
        except Exception as e:
            print(f"get_all_payouts: failed to fetch transactions for since_id={last}. Error: {e}")
            return transactions if len(transactions) > 0 else last

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


def get_new_payouts(last_id):
    shop_url = f"https://{SHOPIFY_KEY}:{SHOPIFY_PASS}@fuji-organics.myshopify.com/admin/api/{API_VERSION}/"
    shopify.ShopifyResource.set_site(shop_url)
    shop = shopify.Shop.current
    limit_payout = 250
    payouts_response = shopify.Balance.get(
        limit=limit_payout, since_id=last_id, method_name="transactions"
    )
    payout_df = pd.DataFrame(payouts_response)

    return payout_df


def update_shopify_payout(data=None, context=None):
    """
    whole process from check the last payout, to make api calls until the data is updated, and save that data as a
    csv file in the bucket while uploading the same data to the orders table in BigQuery
    """
    ## Delete pending of payment transactions before getting the new paid ones

    query_0 = f"""
    DELETE
        `{PROJECT_NAME}.Shopify.{TABLE_NAME_PAY}`
    WHERE
        CAST(id AS INTEGER) >= (
    SELECT
        MIN(CAST(id AS INTEGER))
    FROM
        `{PROJECT_NAME}.Shopify.{TABLE_NAME_PAY}`
    WHERE
        payout_status = 'pending'
    OR
        payout_status = 'in_transit'
    )
    """

    try:
        with bigquery.Client() as BQ:
            query_job_0 = BQ.query(query_0)
            query_job_0.result()
        print("Removed pending transactions")
    except Exception as e:
        print(f"update_shopify_payout: failed to remove pending transactions. Error: {e}")

    ## query select the id correspondig to the last order in the table
    query = f"""
    ------ QUERY to get the max id in the payments data, after deleting the
    ------ pending or in transit transactions
    SELECT
        MAX(CAST(id AS INT64)) AS id
    FROM
    `{PROJECT_NAME}.Shopify.{TABLE_NAME_PAY}`
    ------  WHERE
    ------  CAST(id AS INTEGER) = (
    ------  SELECT
    ------      MAX(CAST(id AS INTEGER))
    ------  FROM
    ------  `{PROJECT_NAME}.Shopify.{TABLE_NAME_PAY}`)
    """

    try:
        bigquery_client = bigquery.Client()
        query_job = bigquery_client.query(query)  # Make an API request.
        rows = query_job.result()  # Waits for query to finish
        result = list(rows)[0]["id"]  ## last id registered in pay table
    except Exception as e:
        print(f"update_shopify_payout: failed to get last transaction id, starting from 0. Error: {e}")
        result = 0
    df = get_all_payouts(result)  ## 2270011949127 --> Reference id that works

    if not isinstance(df, pd.DataFrame):
        return print("No new data to add")
    if not isinstance(df, pd.DataFrame):
        return (
            json.dumps({"status": "no_update", "message": "No new data to add"}),
            200,
            {"Content-Type": "application/json"},
        )

    # Clean data

    # pandas 2.x: pre-convert timezone-aware datetime columns before astype
    df["processed_at"] = pd.to_datetime(df["processed_at"], utc=True).dt.tz_convert(None)

    df = df.astype(
        {
            "payout_id": "str",
            "source_id": "str",
            "source_order_id": "str",
            "source_order_transaction_id": "str",
            "amount": "float64",
            "fee": "float64",
            "net": "float64",
            "processed_at": "datetime64[ns]",
            "adjustment_order_transactions": "str",
            "adjustment_reason": "str",
        },
    )

    for col in df.columns:
        df[col] = df[col].map(
            lambda x: (
                None
                if isinstance(x, str)
                and x.lower() in ["nan", "none", "null", "na", "<na>", "<nan>", ""]
                else x
            )
        )
        df[col] = df[col].apply(
            lambda x: x.replace(".0", "") if isinstance(x, str) else x
        )

    ## Add time of creation
    df["UPDATED_FROM_API"] = datetime.now()

    today_date = date.today().strftime("%Y_%m_%d")
    file_name = f"SHOPIFY_PAYOUTS_{today_date}_{result}_RAW.csv"

    ## Upload to BQ
    try:
        bq_client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")  ### should be append
        job = bq_client.load_table_from_dataframe(
            df,
            f"{PROJECT_NAME}.Shopify.{TABLE_NAME_PAY}",
            job_config=job_config,
        )
        job.result(timeout=120)
        print("Data uploaded to BigQuery")
    except Exception as e:
        print(f"update_shopify_payout: failed to upload to BigQuery. Error: {e}, type: {type(e)}")
        return (
            json.dumps(
                {"status": "failure", "message": f"Upload failed with error {e}"}
            ),
            500,
            {"Content-Type": "application/json"},
        )
    ## Upload to bucket

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(file_name)
    blob.upload_from_string(
        df.to_csv(index=False),
        content_type="text/csv",
        retry=api_retry.Retry(deadline=60),
    )

    return (
        json.dumps({"status": "success", "message": f"Uploaded {len(df)} records"}),
        200,
        {"Content-Type": "application/json"},
    )
