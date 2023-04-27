# pandas==1.5.1
# google-cloud-storage==1.44.0
# google-cloud-bigquery==3.3.5
# pandas-gbq==0.17.9

import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from datetime import date,datetime
from refunds_utils import *


apikey = os.environ.get("API_KEY")
password = os.environ.get("API_PASS")
shop_name = os.environ.get("SHOP_NAME")
api_version = os.environ.get("API_VER")
bucket_name = os.environ.get("BUCKET_NAME")
REFUND_TABLE = os.environ.get("REFUND_TABLE")
ORDERS_TABLE = os.environ.get("ORDERS_TABLE")
PROJECT_ID = os.environ.get("PROJECT_ID")


def get_paginated_results(endpoint, params):
    # set the initial URL to make the request to
    url = f"https://{apikey}:{password}@{shop_name}.myshopify.com/admin/api/{api_version}/orders.json"

    # create an empty list to store the results
    results = []

    # make a request to the initial URL to get the first page of results
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    # add the first page of results to the list
    results.extend(data["orders"])

    # check if there are more pages of results
    while "link" in response.headers:

        # get the URL for the next page of results from the link header
        try:
            url = response.links["next"]["url"]
            url = url.replace("https://", f"https://{apikey}:{password}@")
        except Exception as err:
            print(f"Error {err} with type {type(err)} occurred")
            break

        # make a request to the next page of results
        response = requests.get(url, params={})
        response.raise_for_status()
        data = response.json()

        # add the next page of results to the list
        results.extend(data["orders"])

    # return the complete list of results
    return results


def get_last_refund():

    with bigquery.Client() as bq_client:
        q_tmp = f"""-- getting last refunded order
            SELECT distinct id FROM `{ORDERS_TABLE}`
            WHERE name = (SELECT distinct name
            from `Shopify.refunds_master`
            where id = (SELECT MAX(id) FROM `{PROJECT_ID}.{REFUND_TABLE}`))
            """
        try:
            q_job = bq_client.query(q_tmp)  # Make an API request.
            q_job.result()
            result = list(q_job)[0]["id"]
            return result
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            print("Couldn find the last refund id")
            return 0


par_list = [
    "id",
    "name",
    "email",
    "refunds",
    "created-at",
    "cancelled-at",
    "cancel-reason",
    "financial-status",
    "fulfillment-status",
    "payment-gateway-names",
    "total_shipping_price_set",
    "source-identifier",
    "subtotal-price",
    "source-name",
]

fields = ",".join(par_list)
#### do something to get the last id and work from there
last_id = get_last_refund()

params_api = {
    "fields": fields,
    "limit": 250,
    "status": "any",
    "since_id": f"{last_id}",
    "financial_status": "partially_refunded,refunded",
}


def get_refunds(data, context):
    ##API call
    response_gpt = get_paginated_results("orders", params_api)
    # Filter values to a dataframe
    full_ref_ship = pd.DataFrame(response_gpt)[
        [
            "name",
            "refunds",
            "financial_status",
            "cancelled_at",
            "total_shipping_price_set",
            "created_at",
        ]
    ]
    ### Edit shipping info

    full_ref_ship["shipping"] = full_ref_ship["total_shipping_price_set"].map(get_shipp)
    #     df = pd.DataFrame()
    #     line_ini = full_ref_ship.copy(deep=True)

    ref_line = pd.DataFrame()
    line_ini = full_ref_ship.copy(deep=True)
    ##INI
    for num in range(len(full_ref_ship)):
        line = line_ini.iloc[num]
        if len(line["refunds"]) < 1:
            print(f"{num} is wrong")
            continue

        for ref_num in range(len(line["refunds"])):

            new_df = pd.DataFrame(line["refunds"][ref_num]["refund_line_items"])

            new_df["name"] = line["name"]
            new_df["order_date"] = line["created_at"]
            new_df["cancel_date"] = line["cancelled_at"]
            new_df["financial_status"] = line["financial_status"]

            new_df["shipping_amount"] = line["shipping"]

            ref_line = pd.concat([new_df, ref_line], ignore_index=True)

    ref_line["fulfillment_status"] = ref_line["line_item"].map(get_fulfilment)
    ref_line["product_name"] = ref_line["line_item"].map(get_name)
    ref_line["sku"] = ref_line["line_item"].map(get_sku)
    ref_line["line_price"] = ref_line["line_item"].map(get_price)
    ref_line["line_discount"] = ref_line["line_item"].map(get_discount)
    ref_line.drop(columns=["subtotal_set", "line_item", "total_tax_set"], inplace=True)

    df = ref_line.copy(deep=True)
    df = df.astype(
        {
            "id": "int64",
            "line_item_id": "int64",
            "location_id": "string",
            "quantity": "float64",
            "restock_type": "string",
            "subtotal": "float64",
            "total_tax": "float64",
            "name": "string",
            "order_date": "datetime64[ns]",
            "cancel_date": "datetime64[ns]",
            "shipping_amount": "float64",
        }
    )
    today_date = date.today().strftime("%Y_%m_%d")
    df["DATA_CREATED_AT"] = datetime.utcnow()
    try:
        df.to_gbq(
            destination_table=REFUND_TABLE,
            project_id=PROJECT_ID,
            progress_bar=False,
            if_exists="append",
        )  ### should be append
    except Exception as e:
        print(e)
        print("Saving data to bucket")
        storage_client = storage.Client()
        bucket = storage_client.list_buckets().client.bucket(bucket_name)
        file_name = f"SH_problem_data_{today_date}_{last_id}_RAW.csv"
        blob = bucket.blob(file_name)
        blob.upload_from_string(df.to_csv(index=False), content_type="csv/txt")
