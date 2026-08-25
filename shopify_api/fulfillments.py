from orders_parameters import *
from ratelimit import limits
from google.cloud import bigquery
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _session():
    s = requests.Session()
    retries = Retry(
        total=3,
        connect=3,
        read=2,
        backoff_factor=1,
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


@limits(calls=100, period=1)
def get_fullfillments(order_id_list):
    """
    Fetches the fulfillment info from the shopify store, and returns a dataframe
    Using the shopify api, loops throught all the orders_id_list, and returns a pandas
    dataframe with all the fulfillments parameters contained in the fields variable

    Parameters:
    - order_id_list (list): A list of order ids from the

    Returns:
    - fulfillments (pd.DataFrame): A dataframe with all the fulfillments from the shopify store
    """
    # Phase 1: fetch all responses rapidly to keep the connection warm
    raw_data = {}
    session = _session()
    try:
        for order_id in order_id_list:
            url = f"https://{SHOPIFY_KEY}:{SHOPIFY_PASS}@{STORE_NAME}.myshopify.com/admin/api/{API_VERSION}/orders/{order_id}/fulfillments.json"
            try:
                response_in = session.get(url, timeout=30)
                raw_data[order_id] = response_in.json()["fulfillments"]
            except Exception as e:
                print(f"get_fullfillments: failed to fetch fulfillments for order_id={order_id}, skipping. Error: {e}")
    except Exception as e:
        print(f"get_fullfillments: unexpected error during fetch phase. Error: {e}, type: {type(e)}")
        raise e

    # Phase 2: normalize and concat all responses
    fullfillments = pd.DataFrame()
    for order_id, fulfillment_data in raw_data.items():
        try:
            df = pd.json_normalize(fulfillment_data)
            fullfillments = pd.concat([fullfillments, df], ignore_index=True)
        except Exception as e:
            print(f"get_fullfillments: failed to normalize fulfillments for order_id={order_id}, skipping. Error: {e}")

    if len(fullfillments) < 1:
        print("Api didnt provide new fulfillment data")
    return fullfillments


def get_order_ids(orders_df):
    id_list = []
    try:
        id_list = list(set(orders_df["id"].to_list()))
    except Exception as e:
        print(f"Error in get_order_ids {e}")
        raise e
    return id_list


def clean_fulfillments(new_fulf):
    query_text = f"""
    ------ clean_fulfillments Query ------
    DELETE
        `{FULFILLMENT_TABLE}`
    WHERE
        CAST(order_id AS STRING) IN ({new_fulf})
    ------ clean_fulfillments Query ------
    """.replace(
        "[", ""
    ).replace(
        "]", ""
    )
    try:
        bq_client = bigquery.Client()
        query_job = bq_client.query(query_text)
        result_query = query_job.result()
    except Exception as e:
        print(
            f"error in clean_fulfillments, aborting fulfillment process, message: {e}"
        )
        return False
    return True


def get_table_columns(table_id):
    rows_order_database = []
    try:
        with bigquery.Client() as client:
            table_c = client.get_table(table_id)
            rows_order_database = [x.name for x in table_c.schema]
    except Exception as e:
        print(f"get_table_columns: failed to fetch schema for table {table_id}. Error: {e}, type: {type(e)}")
        raise e
    return rows_order_database


def upload_fulfillments(df_fulf):
    if len(df_fulf) < 1:
        print("No data to upload")
        return False
    try:
        colums_df = get_table_columns(FULFILLMENT_TABLE)
        df_fulf = df_fulf[colums_df].copy()
        bq_client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = bq_client.load_table_from_dataframe(
            df_fulf,
            FULFILLMENT_TABLE,
            job_config=job_config,
        )
        job.result(timeout=120)
        print("Uploaded fulfillments")
    except Exception as e:
        print(f"upload_fulfillments: failed to upload to table {FULFILLMENT_TABLE}. Error: {e}, type: {type(e)}")
        raise e
    return True
