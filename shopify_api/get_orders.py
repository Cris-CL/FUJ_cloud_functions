# requirements.txt
# pandas==1.5.1
# google-cloud-storage==1.44.0
# google-cloud-bigquery>=3.3.5
# pandas-gbq==0.17.9

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
shop_name = os.environ.get("STORE_NAME")


def get_all_orders(last_order_id):
    """
    Main Function, when provided last_order_id, loops throught all the orders
    since that id, and appends them to a dataframe
    """

    last = last_order_id  ##first order_id 2270011949127

    limit = 250
    status = "any"
    orders = pd.DataFrame()
    fields = ",".join(
        [
            "id",
            "name",
            "email",
            "created-at",
            "cancelled-at",
            "confirmed",
            "cancel-reason",
            "buyer-accepts-marketing",
            "contact-email",
            "current-subtotal-price",
            "current-total-price",
            "total-outstanding",
            "current-total-tax",
            "current-total-discounts",
            "discount-codes",
            "financial-status",
            "fulfillment-status",
            "refunds",  ### added 18/01/23 for get the refund amounts
            "note",
            "number",
            "order-number",
            "payment-gateway-names",
            "processed-at",
            "processing-method",
            "reference",
            "source-identifier",
            "source-name",
            "line-items",
            "updated-at",
            # "gateway",
            # "cart-token", #### deprecated
            # "checkout-token", #### deprecated
            # "token", #### deprecated
            "note-attributes",
            "checkout-id",
            "tags",
            "refering-site",
            "totat-line-items-price",
            "total_shipping_price_set",
            "subtotal-price",
            "total-price",
            "total-tax",
            "shipping-address",
            "total-discounts",  ### Added total discounts field
        ]
    )
    print(f"First order_id: {last}")
    while True:
        url = f"https://{apikey}:{password}@{shop_name}.myshopify.com/admin/api/{api_version}/orders.json?limit={limit}&fields={fields}&status={status}&since_id={last}"
        response_in = requests.get(url)
        df = pd.json_normalize(response_in.json()["orders"])

        # Function finishes with no new info
        if len(df) < 1:
            print("Api didnt provide new data")
            return last

        orders = pd.concat([orders, df], ignore_index=True)
        last = df["id"].iloc[-1]

        if len(df) < limit:
            print(f"Last order_id: {last}")
            print(len(orders))
            break
    return orders


def clean_row(row):
    signs = "{}[]'"
    row = str(row)
    for sign in signs:
        row = row.replace(sign, "").replace("\n", "")
    return row


def line_map(df):
    ## expand lines for each product
    df = df.explode("line_items").reset_index(drop=True)

    ## Split each part of the line_items col
    df["lineitem_quantity"] = df["line_items"].apply(
        lambda x: x["quantity"] if len(x) > 0 else None
    )
    df["lineitem_name"] = df["line_items"].apply(
        lambda x: x["name"] if len(x) > 0 else None
    )
    df["lineitem_price"] = df["line_items"].apply(
        lambda x: x["price"] if len(x) > 0 else None
    )
    df["lineitem_sku"] = df["line_items"].apply(
        lambda x: x["sku"] if len(x) > 0 else None
    )

    ## Cleaning the shipping amounts
    df.columns = [
        col.replace("total_shipping_price_set.", "shipping_") for col in df.columns
    ]
    df.drop(
        columns=[
            "line_items",
            "shipping_shop_money.currency_code",
            "shipping_presentment_money.currency_code",
        ],
        inplace=True,
    )
    df.columns = [col.replace("money.", "") for col in df.columns]
    df.columns = [col.replace("shipping_address.", "ship_") for col in df.columns]
    return df


def discount_process(df):

    df["discount_code"] = df["discount_codes"].apply(
        lambda x: x[0]["code"] if len(x) > 0 else None
    )
    df["discount_amount"] = df["discount_codes"].apply(
        lambda x: x[0]["amount"] if len(x) > 0 else None
    )
    df["discount_type"] = df["discount_codes"].apply(
        lambda x: x[0]["type"] if len(x) > 0 else None
    )

    # clean note_attributes and gateway_names row
    df["note_attributes"] = df["note_attributes"].apply(
        lambda x: clean_row(x) if len(x) > 0 else None
    )
    df["note"] = df["note"].apply(lambda x: str(x))
    df["payment_gateway_names"] = df["payment_gateway_names"].apply(
        lambda x: clean_row(x) if len(x) > 0 else None
    )

    df.drop(columns="discount_codes", inplace=True)
    return df


def refund_clean(val):
    tmp = 0
    for x in val:
        if isinstance(x, list):
            continue
        elif isinstance(x, dict):
            if len(x.get("refund_line_items", 0)) < 1:
                continue
            tmp = tmp + float(x.get("refund_line_items", 0)[0].get("subtotal", 0))
    return tmp


def refunds(df):

    df["refund"] = df["refunds"].map(lambda x: refund_clean(x) if len(x) > 0 else 0)
    df.drop(columns="refunds", inplace=True)
    return df


def get_transactions(id_list):

    transactions = pd.DataFrame()
    for order_id in set(id_list):

        url = f"https://{apikey}:{password}@{shop_name}.myshopify.com/admin/api/{api_version}/orders/{order_id}/transactions.json"
        response_in = requests.get(url)
        df = pd.json_normalize(response_in.json()["transactions"][0])
        transactions = pd.concat([transactions, df], ignore_index=True)

    trans_filter = transactions[["order_id", "authorization"]]
    return trans_filter


def join_orders_transactions(orders_df, transactions_df):
    transactions_df = transactions_df.astype({"order_id": "string"})
    try:
        new_df = pd.merge(
            orders_df,
            transactions_df,
            left_on="id",
            right_on="order_id",
            how="left",
            validate="many_to_one",
        )
        new_df["reference"] = new_df["reference"].fillna(new_df["authorization"])
        new_df = new_df.drop(columns=["order_id", "authorization"])

        assert new_df.shape == orders_df.shape

    except Exception as err:
        print("Error join_orders_transactions function")
        print(f"Unexpected {err=}, {type(err)=}")
        print(new_df.shape, orders_df.shape)
        new_df = orders_df

    return new_df


def type_change(df):
    deprecated = [
        "gateway",
        "cart_token",  #### deprecated
        "checkout_token",  #### deprecated
        "token",  #### deprecated
    ]
    dict_types = {
        "id": "str",
        "buyer_accepts_marketing": "bool",
        "cancel_reason": "str",
        "cancelled_at": "datetime64[ns]",
        "cart_token": "str",
        "checkout_id": "str",
        "checkout_token": "str",
        "confirmed": "str",
        "contact_email": "str",
        "created_at": "datetime64[ns]",
        "current_subtotal_price": "float64",
        "current_total_discounts": "float64",
        "current_total_price": "float64",
        "current_total_tax": "str",
        "email": "str",
        "financial_status": "str",
        "fulfillment_status": "str",
        "refund": "float64",
        "name": "str",
        "note": "str",
        "note_attributes": "str",
        "number": "str",
        "order_number": "str",
        "payment_gateway_names": "str",
        "processed_at": "datetime64[ns]",
        "processing_method": "str",
        "reference": "str",
        "source_identifier": "str",
        "source_name": "str",
        "tags": "str",
        "token": "str",
        "total_outstanding": "float64",
        "updated_at": "datetime64[ns]",
        "shipping_shop_amount": "float64",
        "shipping_presentment_amount": "float64",
        "lineitem_quantity": "float64",
        "lineitem_name": "str",
        "lineitem_price": "float64",
        "lineitem_sku": "str",
        "discount_code": "str",
        "discount_amount": "float64",
        "discount_type": "str",
        "subtotal_price": "float64",
        "total_price": "float64",
        "total_tax": "float64",
        "UPDATED_FROM_API": "datetime64[ns]",
        "ship_address1": "str",
        "ship_phone": "str",
        "ship_city": "str",
        "ship_zip": "str",
        "ship_province": "str",
        "ship_country": "str",
        "ship_first_name": "str",
        "ship_last_name": "str",
        "ship_address2": "str",
        "ship_company": "str",
        "ship_name": "str",
        "ship_latitude": "float64",
        "ship_longitude": "float64",
        "ship_country_code": "str",
        "ship_province_code": "str",
        "total_discounts": "float64",
    }

    for cols_db in dict_types.keys():
        if cols_db not in df.columns:
            if cols_db in deprecated:
                df[cols_db] = "DEPRECATED"
            else:
                df[cols_db] = None

    df = df.astype(dict_types)
    ## Delete nan strings or empty values
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: None if x in ["nan", "", "None", "null"] else x
        )
        if col not in dict_types.keys():
            ##### dropping columns that shouldnt appear
            df.drop(
                columns=[col],
                inplace=True,
            )
    df["checkout_id"] = df["checkout_id"].apply(
        lambda x: str(int(float(x))) if isinstance(x, str) else x
    )
    return df.copy()


def stripe_process(df):
    try:
        stripe_list = set(df[df["payment_gateway_names"] == "stripe"]["id"])
        transactions = get_transactions(stripe_list)
        df = join_orders_transactions(df, transactions)
    except Exception as e:
        print(e, type(e))
        print("Stripe process failed")
    return df.copy()


def upload_to_bq(df, today_date, result):
    try:
        df.to_gbq(
            destination_table=table_name,
            project_id=project_name,
            progress_bar=False,
            if_exists="append",  ### should be append
        )
        print("Data uploaded to BigQuery")
    except Exception as e:
        print("Couldn't upload data to BigQuery table, with error:")
        print(e, type(e))
        print("Saving data to bucket")
        storage_client = storage.Client()
        bucket = storage_client.list_buckets().client.bucket(bucket_name)
        file_name = f"SH_problem_data_{today_date}_{result}_RAW.csv"
        blob = bucket.blob(file_name)
        blob.upload_from_string(df.to_csv(index=False), content_type="csv/txt")
    return


def main(data, context):
    """
    whole process from check the last order, to make api calls until the data is updated, and save that data as a
    csv file in the bucket while uploading the same data to the orders table in BigQuery
    """
    dt = datetime.now()
    if dt.weekday() == 0:  ##corrected df for dt
        ## Delete the previous 2 weeks every monday to get rid of the pending

        bq_cl_tmp = bigquery.Client()
        ## Changed to better select the last order in the previous 2 weeks
        q_tmp = f"""
        -- deleting last 2 weeks get_orders function
        DELETE `{project_name}.{table_name}`
        Where
            CAST(order_number as int64) > (
                SELECT max(CAST(order_number as int64))
                FROM `{project_name}.{table_name}`
                WHERE created_at < CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AS TIMESTAMP)
                -- today minus 14 days
            )
            """
        print("cleaning pending orders")
        try:
            del_job = bq_cl_tmp.query(q_tmp)  # Make an API request.
            del_job.result()
            print("Deleted last 2 weeks of orders")
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            print("Couldn'd delete the last 2 weeks orders")

    ## Get data
    bigquery_client = bigquery.Client()

    ## query select the id correspondig to the last order in the table
    query = f"""
    select distinct id
    FROM `{project_name}.{table_name}`
    Where CAST(order_number as integer) = (SELECT max(CAST(order_number as integer)) FROM `{project_name}.{table_name}`)
    """
    ##### previously the max(name) caused problems because it was an string and the max string was #9999 and since then the orders were duplicated

    try:
        query_job = bigquery_client.query(query)  # Make an API request.
        rows = query_job.result()  # Waits for query to finish
        result = list(rows)[0]["id"]  ## last id registered in orders_master table

    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        print("starting from first order")
        result = 2270011949127

    df = get_all_orders(result)  ## 2270011949127 --> Reference id that works

    if not isinstance(
        df, pd.DataFrame
    ):  ### if the df is not a dataframe, it means that there is no new data
        return print("No new data to add")
    # Clean data

    df = refunds(df)  ## new step for the refund column
    df = line_map(df)
    df = discount_process(df)

    ## Add time of creation
    df["UPDATED_FROM_API"] = datetime.utcnow()

    # Change datetime using Series.dt.tz_localize() according to pandas doc
    # ref https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.astype.html
    df = type_change(df)
    df = stripe_process(df)
    today_date = date.today().strftime("%Y_%m_%d")
    file_name = f"SHOPIFY_ORDERS_{today_date}_{result}_RAW.csv"

    ## Upload to BQ
    upload_to_bq(df, today_date, result)

    if dt.weekday() == 0:
        print("Saving to bucket last 2 weeks of orders")
        storage_client = storage.Client()
        bucket = storage_client.list_buckets().client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(df.to_csv(index=False), content_type="csv/txt")
    return
