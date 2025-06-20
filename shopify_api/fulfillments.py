from orders_parameters import *
from ratelimit import limits
from google.cloud import bigquery
import pandas as pd
import requests


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
    fullfillments = pd.DataFrame()
    try:
        for order_id in order_id_list:
            url = f"https://{SHOPIFY_KEY}:{SHOPIFY_PASS}@{STORE_NAME}.myshopify.com/admin/api/{API_VERSION}/orders/{order_id}/fulfillments.json"
            response_in = requests.get(url)
            try:
                df = pd.json_normalize(response_in.json()["fulfillments"])
            except Exception as e:
                print("get_fullfillments failed with error:")
                print(e, type(e))
                return fullfillments
            fullfillments = pd.concat([fullfillments, df], ignore_index=True)

            # Function finishes with no new info
        if len(fullfillments) < 1:
            print("Api didnt provide new data")
            return fullfillments
    except Exception as e:
        print(f"Error in get_fullfillments")
        raise e
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
        print(f"Error in get_table_columns")
        raise e
    return rows_order_database


def upload_fulfillments(df_fulf):
    if len(df_fulf) < 1:
        print("No data to upload")
        return False
    try:
        colums_df = get_table_columns(FULFILLMENT_TABLE)
        df_fulf = df_fulf[colums_df].copy()
        df_fulf.to_gbq(
            destination_table=FULFILLMENT_TABLE,
            project_id=PROJECT_NAME,
            progress_bar=False,
            if_exists="append",  ### should be append
        )
        print("Uploaded fulfillments")
    except Exception as e:
        print(f"Error in upload_fulfillments")
        raise e
    return True
