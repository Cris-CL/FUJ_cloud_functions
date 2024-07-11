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


def stripe_csv_bq(uri,table_id,job_config):
    client = bigquery.Client()
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config)  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    return




# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def upload_stripe_bq(cloud_event):
    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]


# Construct a BigQuery client object.
    # client = bigquery.Client()
    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('DATASET_ID')
    table_name_bal = os.environ.get('TABLE_ID')
    table_name_pay = os.environ.get('TABLE_ID_PAY')
    table_name_ama = os.environ.get('TABLE_ID_AMA')
    prefix = name[:3]


    uri = f"gs://{bucket}/{name}"

    if prefix == 'BAL':
        print("BALANCE")
        job_config_bal = bigquery.LoadJobConfig(
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
        table_bal = f'{project_id}.{dataset_id}.{table_name_bal}'
        stripe_csv_bq(uri,table_bal,job_config_bal)


    elif prefix == 'PAY':
        print("PAYOUT")
        job_config_pay = bigquery.LoadJobConfig(
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
        table_pay = f'{project_id}.{dataset_id}.{table_name_pay}'

        stripe_csv_bq(uri,table_pay,job_config_pay)


    elif prefix == 'AMA':
        from amazon_pay_txt_process import clean_txt,upload_ama

        df_ama = clean_txt(uri)
        table_upload = f'{project_id}.{dataset_id}.{table_name_ama}'

        upload_ama(df_ama,table_upload)

        ama_file_name = name[:-4]
        year = ama_file_name[4:8]

        filename = f'Shopify/amazon_pay/{year}/{ama_file_name}.csv'
        storage_client = storage.Client()
        bucket_ama = storage_client.list_buckets().client.bucket('fujiorg-sales-data')
        blob = bucket_ama.blob(filename)
        blob.upload_from_string(df_ama.to_csv(index = False),content_type = 'csv')
        try:
            source_bucket = storage_client.bucket(bucket)
            source_bucket.delete_blob(name)
        except Exception as e:
            print(e,type(e))
        return
