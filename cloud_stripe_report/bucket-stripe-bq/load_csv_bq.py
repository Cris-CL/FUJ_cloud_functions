# functions-framework==3.*
# google-cloud-bigquery>=3.3.5
# google-cloud-storage>=2.5.0
# pandas==1.5.1
# fsspec==2022.11.0
# gcsfs==2022.11.0
# pyarrow==9.0.0


import os
import functions_framework
from google.cloud import bigquery,storage

file_classifier = {
    'PAY':'fujiorg-sales-data/Shopify/stripe/payments/',
    'BAL':'fujiorg-sales-data/Shopify/stripe/balance/',
    'AMA':'fujiorg-sales-data/Shopify/amazon_pay/',
}

# def move_blob(origin_bucket_name, origin_blob_name, destination_bucket_name, destination_blob_name):
#     """Moves a blob from one bucket to another with a new name."""
#     from google.cloud import storage

#     storage_client = storage.Client()

#     source_bucket = storage_client.bucket(origin_bucket_name)
#     source_blob = source_bucket.blob(origin_blob_name)
#     destination_bucket = storage_client.bucket(destination_bucket_name)

#     blob_copy = source_bucket.copy_blob(
#         source_blob, destination_bucket, destination_blob_name
#     )
#     source_bucket.delete_blob(origin_blob_name)
# return

def upload_ama(df,dic,table_id):
    job_config_ama = bigquery.LoadJobConfig(
        schema=[
            eval(
                f"bigquery.SchemaField('{col}', bigquery.enums.SqlTypeNames.{dic[str(df[col].dtypes)]})"
            ) for col in df.columns
        ]
        ,write_disposition="WRITE_APPEND",)
    client_ama = bigquery.Client()
    job_ama = client_ama.load_table_from_dataframe(
    df, table_id, job_config=job_config_ama)  # Make an API request.
    job_ama.result()
    return


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def upload_stripe_bq(cloud_event):
    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]


# Construct a BigQuery client object.
    client = bigquery.Client()
    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('DATASET_ID')
    table_name_bal = os.environ.get('TABLE_ID')
    table_name_pay = os.environ.get('TABLE_ID_PAY')
    table_name_ama = os.environ.get('TABLE_ID_AMA')

    if name[:3] == 'BAL':
        print("BALANCE")
        job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("balance_transaction_id", "STRING"),
            bigquery.SchemaField("created_utc", "DATETIME"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("gross", "FLOAT"),
            bigquery.SchemaField("fee", "FLOAT"),
            bigquery.SchemaField("net", "FLOAT"),
            bigquery.SchemaField("reporting_category", "STRING"),
            bigquery.SchemaField("source_id", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("customer_facing_amount", "FLOAT"),
            bigquery.SchemaField("customer_facing_currency", "STRING"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("customer_email", "STRING"),
            bigquery.SchemaField("customer_description", "STRING"),
            bigquery.SchemaField("charge_id", "STRING"),
            bigquery.SchemaField("payment_intent_id", "STRING"),
            bigquery.SchemaField("invoice_id", "STRING"),
            bigquery.SchemaField("payment_method_type", "STRING"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        write_disposition='WRITE_APPEND',)
        table_id = f'{project_id}.{dataset_id}.{table_name_bal}'

    elif name[:3] == 'PAY':
        print("PAYOUT")
        job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("automatic_payout_id", "STRING"),
            bigquery.SchemaField("automatic_payout_effective_at", "DATETIME"),
            bigquery.SchemaField("balance_transaction_id", "STRING"),
            bigquery.SchemaField("created_utc", "DATETIME"),
            bigquery.SchemaField("created", "DATETIME"),
            bigquery.SchemaField("available_on_utc", "DATETIME"),
            bigquery.SchemaField("available_on", "DATETIME"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("gross", "FLOAT"),
            bigquery.SchemaField("fee", "FLOAT"),
            bigquery.SchemaField("net", "FLOAT"),
            bigquery.SchemaField("reporting_category", "STRING"),
            bigquery.SchemaField("source_id", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("customer_facing_amount", "FLOAT"),
            bigquery.SchemaField("customer_facing_currency", "STRING"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("customer_email", "STRING"),
            bigquery.SchemaField("charge_id", "STRING"),
            bigquery.SchemaField("payment_intent_id", "STRING"),
            bigquery.SchemaField("charge_created_utc", "DATETIME"),
            bigquery.SchemaField("invoice_id", "STRING"),
            bigquery.SchemaField("order_id", "STRING"),
            bigquery.SchemaField("payment_method_type", "STRING"),
            bigquery.SchemaField("connected_account_id", "STRING"),
            bigquery.SchemaField("connected_account_name", "STRING"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        write_disposition='WRITE_APPEND',)
        table_id = f'{project_id}.{dataset_id}.{table_name_pay}'

    uri = f"gs://{bucket}/{name}"

    if name[:3] == 'AMA':

        from amazon_pay_txt_process import dtypes_dict, clean_txt
        df_ama = clean_txt(uri)
        table_upload = f'{project_id}.{dataset_id}.{table_name_ama}'
        upload_ama(df_ama,dtypes_dict,table_upload)


        filename = f'Shopify/amazon_pay/{name[:-4]}.csv'
        storage_client = storage.Client()
        bucket = storage_client.list_buckets().client.bucket('fujiorg-sales-data')
        blob = bucket.blob(filename)
        blob.upload_from_string(df_ama.to_csv(index = False),content_type = 'csv')


    return


    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
