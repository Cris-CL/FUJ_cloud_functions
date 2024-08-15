# functions-framework==3.*
# pandas>=1.2.2
# google-cloud-bigquery>=3.3.5
# google-cloud-storage>=2.5.0

import os
from datetime import datetime
import pandas as pd
from google.cloud import bigquery, storage
import functions_framework
from dict_utils_ama import data_types

TABLE_1 = os.environ.get("TABLE_1")
TABLE_2 = os.environ.get("TABLE_2")
DATASET_ID = os.environ.get("DATASET_ID")
PROJECT_ID = os.environ.get("PROJECT_ID")
NEW_BUCKET = os.environ.get("NEW_BUCKET")


def classify_dict(year):
    rep_classifier = {
        "tv": {
            "destination_table": TABLE_2,
            "prefix": "TV",
            "folder": f"transaction_view/settlement_{year}",  ### Modify year when it changes
        },
        "oc": {
            "destination_table": TABLE_1,
            "prefix": "OC",
            "folder": f"order_central/sales_{year}",  ### Modify year when it changes
        },
    }
    return rep_classifier


def get_list_reports(dataset, table):
    """
    Given a table name, returns a list with the file names that were already uploaded to bq
    """

    client = bigquery.Client()

    query = f"""SELECT DISTINCT FILE_NAME FROM `{DATASET_ID}.{table}`"""

    query_job = client.query(query)

    rows = query_job.result()
    list_reports_uploaded = [row.FILE_NAME for row in rows]

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


def get_dataframe(uri):
    df = pd.DataFrame()
    report_type = ""
    try:
        df = pd.read_table(uri)
        report_type = "tv"
        print(report_type)
    except:
        df = pd.read_table(uri, encoding="ms932")
        report_type = "oc"
        print(report_type)
    return [df, report_type]


def prepare_dataframe(df, df_type, file_name):
    df.columns = df.columns.map(lambda x: x.lower().strip().replace("-", "_"))

    df = df.astype(data_types[df_type])

    for col in df.columns:
        ### remove empty strings and nan values
        df[col] = df[col].map(
            lambda x: None
            if type(x) == type("") and x.lower() in ["nan", "null", "<na>", "", " "]
            else x
        )
    df[["FILE_NAME"]] = file_name
    df[["UPLOADED_DATETIME"]] = datetime.now()
    return df


def upload_bq(df, df_type, rep_classifier):
    if df.empty:
        print("No new data")
    else:
        print("Uploading start")
        table_id = (
            f'{PROJECT_ID}.{DATASET_ID}.{rep_classifier[df_type]["destination_table"]}'
        )

        dtypes_tobq = {
            "object": "STRING",
            "int64": "INTEGER",
            "float64": "FLOAT",
            "datetime64[ns]": "DATETIME",
        }
        disposition = "WRITE_APPEND"

        job_config = bigquery.LoadJobConfig(
            schema=[
                eval(
                    f"bigquery.SchemaField('{col}', bigquery.enums.SqlTypeNames.{dtypes_tobq[str(df[col].dtypes)]})"
                )
                for col in df.columns
            ],
            write_disposition=disposition,
        )

        client = bigquery.Client()
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()
        print("Upload finished")
    return


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def amazon_process(cloud_event):

    # first the function loads the file and classify it on
    # transaction view or order central
    # second process the file to fit the destination table
    # third uploads the file
    # fourth move the file to the processed data folder

    data = cloud_event.data
    event_type = cloud_event["type"]
    bucket = data["bucket"]
    name = data["name"]
    year_report = name[:4]

    report_classifier = classify_dict(year_report)

    bucket_name = bucket
    blob_name = name
    destination_bucket_name = NEW_BUCKET
    ## URI from uploaded file to be loaded
    uri = f"gs://{bucket}/{name}"
    data_report = get_dataframe(uri)

    df = data_report[0]
    report_type = data_report[1]
    rep_destination = report_classifier[report_type]

    try:
        list_uploaded = get_list_reports(
            DATASET_ID, rep_destination["destination_table"]
        )
    except:
        print("Error in the query, the file will be uploaded")
        list_uploaded = []

    if name in list_uploaded:
        print(f"{name} is already on the table")
        folder_name = f"Amazon/repeated_files"
        new_name = f'{report_classifier[report_type]["prefix"]}_{blob_name}'
        destination_blob_name = f"{folder_name}/{new_name}"
        print("finish whitout changes,file moved to repeated_files folder")
        df = pd.DataFrame()
        ## TODO process in this case (deleting old data and replace with new)
    else:
        folder_name = f'Amazon/{report_classifier[report_type]["folder"]}'
        new_name = f'{report_classifier[report_type]["prefix"]}_{blob_name}'
        destination_blob_name = f"{folder_name}/{new_name}"
        df = prepare_dataframe(df, df_type=report_type, file_name=name)
        print("Moving file to processed data bucket")

    #### UPLOAD TO BQ
    upload_bq(df, report_type, report_classifier)
    move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name)
    return
