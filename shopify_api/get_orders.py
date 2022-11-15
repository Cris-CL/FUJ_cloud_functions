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

apikey = os.environ.get('SHOPIFY_KEY')
api_version = os.environ.get('API_VERSION')
password = os.environ.get('SHOPIFY_PASS')
bucket_name = os.environ.get('BUCKET')
project_name = os.environ.get('PROJECT_NAME')
table_name = os.environ.get('TABLE_NAME')

def get_all_orders(last_order_id):
    """
    Main Function, when provided last_order_id, loops throught all the orders
    since that id, and appends them to a dataframe
    """

    last=last_order_id ##first order_id 2270011949127

    limit = 250
    status = "any"
    shop_name = 'fuji-organics'
    orders=pd.DataFrame()
    fields = ",".join([
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
        # "gateway",
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
        "cart-token",
        "checkout-token",
        "note-attributes",
        "token",
        "checkout-id",
        "tags",
        "refering-site",
        "totat-line-items-price",
        "total_shipping_price_set",
        "subtotal-price",
        "total-price",
        "total-tax",

    ])
    print(f"First order_id: {last}")
    while True:
        url = f"https://{apikey}:{password}@{shop_name}.myshopify.com/admin/api/{api_version}/orders.json?limit={limit}&fields={fields}&status={status}&since_id={last}"
        response_in = requests.get(url)
        df=pd.json_normalize(response_in.json()['orders'])


        #Function finishes with no new info
        if len(df)<1:
            print("Api didnt provide new data")
            return last

        orders=pd.concat([orders,df],ignore_index=True)
        last=df['id'].iloc[-1]

        if len(df)<limit:
            print(f"Last order_id: {last}")
            print(len(orders))
            break
    return orders

def clean_row(row):
    signs = "{}[]'"
    row = str(row)
    for sign in signs:
        row=row.replace(sign,"").replace("\n","")
    return row


def line_map(df):
    ## expand lines for each product
    df = df.explode("line_items").reset_index(drop=True)

    ## Split each part of the line_items col
    df["lineitem_quantity"] = df["line_items"].apply(lambda x: x["quantity"] if len(x)>0 else None)
    df["lineitem_name"] = df["line_items"].apply(lambda x: x["name"] if len(x)>0 else None)
    df["lineitem_price"] = df["line_items"].apply(lambda x: x["price"] if len(x)>0 else None)
    df["lineitem_sku"] = df["line_items"].apply(lambda x: x["sku"] if len(x)>0 else None)

    ## Cleaning the shipping amounts
    df.columns = [col.replace("total_shipping_price_set.","shipping_") for col in df.columns]
    df.drop(columns=["line_items",
                           "shipping_shop_money.currency_code",
                           "shipping_presentment_money.currency_code",
                          ],inplace=True)
    df.columns = [col.replace("money.","") for col in df.columns]
    return df

def discount_process(df):

    df["discount_code"] = df["discount_codes"].apply(lambda x: x[0]["code"] if len(x)>0 else None)
    df["discount_amount"] = df["discount_codes"].apply(lambda x: x[0]["amount"] if len(x)>0 else None)
    df["discount_type"] = df["discount_codes"].apply(lambda x: x[0]["type"] if len(x)>0 else None)

    #clean note_attributes and gateway_names row
    df["note_attributes"] = df["note_attributes"].apply(lambda x: clean_row(x) if len(x)>0 else None)
    df["note"] = df["note"].apply(lambda x: str(x))
    df["payment_gateway_names"] = df["payment_gateway_names"].apply(lambda x: clean_row(x) if len(x)>0 else None)


    df.drop(columns="discount_codes",inplace=True)
    return df

def main(data,context):
    """
    whole process from check the last order, to make api calls until the data is updated, and save that data as a
    csv file in the bucket while uploading the same data to the orders table in BigQuery
    """
    ## Get data
    bigquery_client = bigquery.Client()

    ## query select the id correspondig to the last order in the table
    query = """
    select distinct id
    FROM `test-bigquery-cc.Shopify.orders_master`
    Where name = (SELECT max(name) FROM `test-bigquery-cc.Shopify.orders_master`)
    """
    query_job = bigquery_client.query(query)  # Make an API request.
    rows = query_job.result()  # Waits for query to finish

    result = list(rows)[0]["id"] ## last id registered in orders_master table

    df = get_all_orders(result) ## 2270011949127 --> Reference id that works

    if type(df) != type(pd.DataFrame()):
        return print("No new data to add")
    # Clean data
    df = line_map(df)
    df = discount_process(df)
    ## Add time of creation
    df["UPDATED_FROM_API"] = datetime.utcnow()

    # Change datetime using Series.dt.tz_localize() according to pandas doc
    # ref https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.astype.html

    df = df.astype({
        'id':'str',
        'buyer_accepts_marketing':'bool',
        'cancel_reason':'str',
        'cancelled_at':'datetime64[ns]',
        'cart_token':'str',
        'checkout_id':'str',
        'checkout_token':'str',
        'confirmed':'str',
        'contact_email':'str',
        'created_at':'datetime64[ns]',
        'current_subtotal_price':'float64',
        'current_total_discounts':'float64',
        'current_total_price':'float64',
        'email':'str',
        'financial_status':'str',
        'fulfillment_status':'str',
        # 'gateway':'str',
        'name':'str',
        'note':'str',
        'note_attributes':'str',
        'number':'str',
        'order_number':'str',
        'payment_gateway_names':'str',
        'processed_at':'datetime64[ns]',
        'processing_method':'str',
        'reference':'str',
        'source_identifier':'str',
        'source_name':'str',
        'tags':'str',
        'token':'str',
        'total_outstanding':'float64',
        'updated_at':'datetime64[ns]',
        'shipping_shop_amount':'float64',
        'shipping_presentment_amount':'float64',
        'lineitem_quantity':'float64',
        'lineitem_name':'str',
        'lineitem_price':'float64',
        'lineitem_sku':'str',
        'discount_code':'str',
        'discount_amount':'float64',
        'discount_type':'str',
        "subtotal_price":'float64',
        "total_price":'float64',
        "total_tax":'float64',
        'UPDATED_FROM_API':'datetime64[ns]'
        })

    today_date = date.today().strftime("%Y_%m_%d")
    file_name = f"SHOPIFY_ORDERS_{today_date}_{result}_RAW.csv"

    ## Upload to BQ
    df.to_gbq(destination_table=table_name,
                        project_id=project_name,
                        progress_bar=False,
                        if_exists="append") ### should be append

    ## Upload to bucket

    storage_client = storage.Client()
    bucket = storage_client.list_buckets().client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(df.to_csv(index=False),content_type = 'csv')
    return
