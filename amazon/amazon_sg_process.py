# functions-framework==3.*
# pandas==1.5.1
# google-cloud-bigquery>=3.3.5
# google-cloud-storage>=2.5.0
# fsspec==2022.11.0
# gcsfs==2022.11.0
# pyarrow==9.0.0

import os
import pandas as pd
from google.cloud import bigquery,storage
import functions_framework

table_1 = os.environ.get('TABLE_1')
table_2 = os.environ.get('TABLE_2')
dataset = os.environ.get('DATASET_ID')
project = os.environ.get('PROJECT_ID')
new_bucket = os.environ.get('NEW_BUCKET')


year = '2023' #### Changed year to current one
rep_classifier = {
    'tv':{
        'destination_table':table_2,
        'prefix':'TV',
        'folder':f'transaction_view/settlement_{year}' ### Modify year when it changes
        },
    'oc':{
        'destination_table':table_1,
        'prefix':'OC',
        'folder':f'order_central/sales_{year}' ### Modify year when it changes
    },
}
data_types = {
    'tv':{
        'settlement_id': 'float',
        'settlement_start_date': 'datetime64',
        'settlement_end_date': 'datetime64',
        'deposit_date': 'datetime64',
        'total_amount': 'float',
        'currency': 'str',
        'transaction_type': 'str',
        'order_id': 'str',
        'merchant_order_id': 'str',
        'adjustment_id': 'str',
        'shipment_id': 'str',
        'marketplace_name': 'str',
        'amount_type': 'str',
        'amount_description': 'str',
        'amount': 'float',
        'fulfillment_id': 'str',
        'posted_date': 'datetime64',
        'posted_date_time': 'datetime64',
        'order_item_code': 'str',
        'merchant_order_item_id': 'str',
        'merchant_adjustment_item_id': 'float',
        'sku': 'str',
        'quantity_purchased': 'float',
        "promotion_id":"str"

    },
    'oc':{
        "amazon_order_id": "str",
        "merchant_order_id": "str",
        "purchase_date": "datetime64",
        "last_updated_date": "datetime64",
        "order_status": "str",
        "fulfillment_channel": "str",
        "sales_channel": "str",
        "order_channel": "str",
        "url": "str",
        "ship_service_level": "str",
        "product_name": "str",
        "sku": "str",
        "asin": "str",
        "item_status": "str",
        "quantity": "int64",
        "currency": "str",
        "item_price": "float",
        "item_tax": "float",
        "shipping_price": "float",
        "shipping_tax": "float",
        "gift_wrap_price": "float",
        "gift_wrap_tax": "float",
        "item_promotion_discount": "float",
        "ship_promotion_discount": "float",
        "ship_city": "str",
        "ship_state": "str",
        "ship_postal_code": "str",
        "ship_country": "str",
        "promotion_ids":"str"
    }
}


def get_list_reports(dataset,table):
    """
    Given a table name, returns a list with the file names that were already uploaded to bq
    """

    client = bigquery.Client()

    query = f"""SELECT DISTINCT file
            FROM `{dataset}.{table}`"""

    query_job = client.query(query)

    rows = query_job.result()
    list_reports_uploaded = [row.file for row in rows]

    return list_reports_uploaded

def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
    """Moves a blob from one bucket to another with a new name."""

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
    source_bucket.delete_blob(blob_name)


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def amazon_sg_process(cloud_event):

   # first the function loads the file and classify it on
   # transaction view or order central
   # second process the file to fit the destination table
   # third uploads the file
   # fourth move the file to the processed data folder

    data = cloud_event.data
    event_type = cloud_event["type"]
    bucket = data["bucket"]
    name = data["name"]

    ## URI from uploaded file to be loaded
    uri = f"gs://{bucket}/{name}"
    if 'tv' in name.lower():
        df = pd.read_table(uri,header=7)
        report_type = 'tv'
        if 'promotion-id' not in df.columns and report_type == 'tv':
            df['promotion-id'] = None
        print(report_type)
    elif 'oc' in name.lower():
        df = pd.read_table(uri)
        report_type = 'oc'
        print(report_type)

    rep_dest = rep_classifier[report_type]
    try:
        list_uploaded = get_list_reports(dataset,rep_dest["destination_table"])
    except:
        print("Error in the query, the file will be uploaded")
        list_uploaded = []

    if name in list_uploaded:
        print(f"{name} is already on the table")

        bucket_name = bucket
        blob_name = name
        destination_bucket_name = new_bucket
        folder_name = f'Amazon_SG/repeated_files'
        new_name = f'{rep_classifier[report_type]["prefix"]}_{blob_name}'
        destination_blob_name = f'{folder_name}/{new_name}'
        move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name)

    ## TODO process in this case (deleting old data and replace with new)

        return print("finish whitout changes,file moved to repeated_files folder")
    ## Rename columns
    df.columns = df.columns.map(lambda x: x.lower().strip().replace("-","_"))

    df = df.astype(data_types[report_type])

    for col in df.columns:
    ### remove empty strings and nan values
        df[col] = df[col].map(
            lambda x: None if type(x) == type("") and x in ["nan","NaN","NAN","Null","null",""] else x
            )
    df[['file']] = name

    #### UPLOAD TO BQ
    print("Uploading start")
    table_id = f'{project}.{dataset}.{rep_classifier[report_type]["destination_table"]}'

    dtypes_dict = {

        "object": "STRING",
        "int64": "INTEGER",
        "float64": "FLOAT",
        "datetime64[ns]": "DATETIME",
    }
    disposition = "WRITE_APPEND"

    job_config = bigquery.LoadJobConfig(
    schema=[
        eval(
            f"bigquery.SchemaField('{col}', bigquery.enums.SqlTypeNames.{dtypes_dict[str(df[col].dtypes)]})"
        ) for col in df.columns
    ]
    ,
    write_disposition=disposition,)

    client = bigquery.Client()
    job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config)  # Make an API request.
    job.result()
    print("Upload finished")


    #### Move the file
    print("Moving file to processed data bucket")

    bucket_name = bucket
    blob_name = name
    destination_bucket_name = new_bucket
    folder_name = f'Amazon_SG/{rep_classifier[report_type]["folder"]}'
    new_name = f'{blob_name}'
    destination_blob_name = f'{folder_name}/{new_name}'

    ### Call the function
    move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name)
