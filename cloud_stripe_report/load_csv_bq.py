import os
import functions_framework
from google.cloud import bigquery

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
    table_name = os.environ.get('TABLE_ID')

    print(project_id)

    table_id = f'{project_id}.{dataset_id}.{table_name}'

    print(table_id)

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
        write_disposition='WRITE_APPEND',
    )
    uri = f"gs://{bucket}/{name}"
    print(uri)


    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
