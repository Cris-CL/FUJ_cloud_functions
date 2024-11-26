from google.cloud import bigquery
import pandas as pd


def get_table_columns(table_id):
    with bigquery.Client() as client:
        table_c = client.get_table(table_id)
        rows_order_database =  [x.name for x in table_c.schema]
    return rows_order_database


def upload_rak_orders(df_orders,table_id,upd_disposition):
    dtypes_dict = {
        "object": "STRING",
        "int64": "FLOAT",
        "float64": "FLOAT",
        "datetime64[ns]": "DATETIME",
        }

    client = bigquery.Client()
    table_rows =  get_table_columns(table_id)

    for col in df_orders.columns:
        if col not in table_rows:
            df_orders.drop(columns=col,inplace=True)

    job_config = bigquery.LoadJobConfig(
        schema=[eval(
            f"bigquery.SchemaField('{col}', bigquery.enums.SqlTypeNames.{dtypes_dict[str(df_orders[col].dtypes)]})"
            ) for col in df_orders.columns],
        write_disposition=upd_disposition)

    job = client.load_table_from_dataframe(df_orders, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"Updated {table_id} now has {table.num_rows} rows and {len(table.schema)} columns")
    return
