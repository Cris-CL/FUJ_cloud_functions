# functions-framework==3.*
# pandas>=1.2.2
# google-cloud-bigquery>=3.3.5
# fsspec>=2022.11.0
# gcsfs>=2022.11.0

import os
import functions_framework
from df_process import load_rak_orders_df
from bq_process import upload_rak_orders


@functions_framework.cloud_event
def upload_rakuten_bq(cloud_event):
    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]
    disposition = "WRITE_APPEND"

    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('DATASET_ID')
    table_name = os.environ.get('TABLE_ID')
    table_id = f'{project_id}.{dataset_id}.{table_name}'

    uri = f"gs://{bucket}/{name}"
    df = load_rak_orders_df(uri,name)
    upload_rak_orders(df,table_id,disposition)
    return
