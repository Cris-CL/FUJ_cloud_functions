# functions-framework==3.*
# pandas>=1.2.2
# google-cloud-bigquery>=3.3.5
# fsspec>=2022.11.0
# gcsfs>=2022.11.0

import os
from datetime import datetime
import time
import random
import functions_framework
import pandas as pd
from google.cloud import bigquery
from columns_names import columns_names_translation, dict_col


def format_rakuten(df):

    for col in df.columns:
       df[col] = df[col].astype(
            {
                f"{col}":f"{dict_col.get(col,'str')}"
            }
             )
       df[col] = df[col].apply(
           lambda x:
               None if x in ["nan",
                             "null",
                             "none"] else x
               )
    return df

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def upload_rakuten_bq(cloud_event):
    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]
    disposition = "WRITE_APPEND"


# Construct a BigQuery client object.
    client = bigquery.Client()
    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('DATASET_ID')
    table_name = os.environ.get('TABLE_ID')

    table_id = f'{project_id}.{dataset_id}.{table_name}'
    dtypes_dict = {
    "object": "STRING",
    "int64": "FLOAT",
    "float64": "FLOAT",
    "datetime64[ns]": "DATETIME",}

    uri = f"gs://{bucket}/{name}"
    df = pd.read_csv(uri,encoding="cp932")
   ######## columns verification
    df.columns = [columns_names_translation.get(col) for col in df.columns] ### rename columns based on the dictionary

    df["UPDATED_AT_UTC"] = datetime.utcnow()
    df["FILE_NAME"] = name
    df = format_rakuten(df)

    with bigquery.Client() as client_r:
        table_c = client_r.get_table(table_id)
        table_rows =  [x.name for x in table_c.schema]

    for col in df.columns:
        if col not in table_rows:
            df.drop(columns=col,inplace=True)

    job_config = bigquery.LoadJobConfig(
        schema=[
            eval(
                f"bigquery.SchemaField('{col}', bigquery.enums.SqlTypeNames.{dtypes_dict[str(df[col].dtypes)]})"
            ) for col in df.columns
        ]
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        ,
        write_disposition=disposition)

    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.

    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    ##### disposition = if want to rewrite the records "WRITE_TRUNCATE"

    # sleep time for not overloading bigquery from 0 to 3 seconds

    time.sleep(1 + int(name[4:6])/3)

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(f"Updated {table_id} now has {table.num_rows} rows and {len(table.schema)} columns")
