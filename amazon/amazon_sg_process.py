# functions-framework==3.*
# google-cloud-bigquery>=3.3.5
# google-cloud-storage>=2.5.0
# fsspec==2022.11.0
# gcsfs==2022.11.0
# pyarrow==9.0.0
# pandas==1.5.1
# numpy==1.23.4

import os
from datetime import datetime
import pandas as pd
from google.cloud import bigquery,storage
import functions_framework
from config_asg import *

def get_list_reports(dataset,table):
    """
    Given a table name, returns a list with the file names that were already uploaded to bq
    """

    client = bigquery.Client()

    query = f"""SELECT DISTINCT FILE_NAME FROM `{dataset}.{table}`"""

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

def get_dataframe(uri,name):
    df = pd.DataFrame()
    report_type = ''
    if 'tv' in name.lower():
        header_cut = 7
        df = pd.read_table(uri,header=header_cut)
        if 'settlement-id' not in df.columns:
            header_cut = 6
            df = pd.read_table(uri,header=header_cut)
            print('New ver')
        report_type = 'tv'
        if 'promotion-id' not in df.columns and report_type == 'tv':
            df['promotion-id'] = None
        print(report_type)
    elif 'oc' in name.lower():
        df = pd.read_table(uri)
        report_type = 'oc'
        print(report_type)
    else:
        print("incorrect report")
    return [df,report_type]

def prepare_dataframe(df,df_type,file_name):
    df.columns = df.columns.map(lambda x: x.lower().strip().replace("-","_"))

    df = df.astype(data_types[df_type])
    ### date formatting
    for column in datetime_columns[df_type]:
        try:
            df[column] = pd.to_datetime(df[column], format=datetime_format[df_type])
        except:
            print(column)
            df[column] = pd.to_datetime(df[column], format=date_format)
    if df_type == 'tv':
        for column in date_columns[df_type]:
            try:
                df[column] = pd.to_datetime(df[column], format=date_format)
            except:
                print(column)
                df[column] = pd.to_datetime(df[column], format=datetime_format[df_type])

    for col in df.columns:
    ### remove empty strings and nan values
        df[col] = df[col].map(
            lambda x: None if isinstance(x,str) and x.lower() in ["nan","null","<na>",""," "] else x
            )
    df[['FILE_NAME']] = file_name
    df[['UPLOADED_DATETIME']] = datetime.now()
    return df

def upload_bq(df,df_type):
    if df.empty:
        print("No new data")
        return
    else:
        print("Uploading start")
        table_id = f'{project}.{dataset}.{rep_classifier[df_type]["destination_table"]}'

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
    return


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
    bucket_name = bucket
    blob_name = name
    destination_bucket_name = new_bucket

    ## URI from uploaded file to be loaded
    uri = f"gs://{bucket}/{name}"

    data_report = get_dataframe(uri,name)
    df = data_report[0]
    report_type = data_report[1]

    if report_type not in ['tv','oc']:
        print('Wrong report loading, finishing process')
        return

    rep_destination = rep_classifier.get(report_type,'')

    try:
        list_uploaded = get_list_reports(dataset,rep_destination["destination_table"])
    except Exception as e:
        print(e,type(e))
        print("Error in the query, the file will be uploaded")
        list_uploaded = []

    if name in list_uploaded:
        print(f"{name} is already on the table")

        folder_name = f'Amazon_SG/repeated_files'
        new_name = f'{rep_classifier[report_type]["prefix"]}_{blob_name}'
        destination_blob_name = f'{folder_name}/{new_name}'
        print("finish whitout changes,file moved to repeated_files folder")
        df = pd.DataFrame()

    else:
        folder_name = f'Amazon_SG/{rep_classifier[report_type]["folder"]}'
        new_name = f'{blob_name}'
        destination_blob_name = f'{folder_name}/{new_name}'
        print("Moving file to processed data bucket")
        df = prepare_dataframe(df, df_type=report_type, file_name=name)

    move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name)
    #### UPLOAD TO BQ
    upload_bq(df,report_type)
    return
