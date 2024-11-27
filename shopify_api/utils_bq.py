from google.cloud import bigquery
from orders_parameters import *
from google.cloud import storage
from datetime import datetime

def upload_to_bq(df, today_date, result):
    """ "
    Uploads the dataframe to BigQuery

    This function uploads the dataframe to BigQuery, and if it fails,
    it saves the dataframe to a csv file in the bucket

    Parameters:
    - df (pd.DataFrame): A dataframe with all the orders from the shopify store
    - today_date (str): The current date
    - result (int): The last order id that was registered in the database

    Returns:
    - None
    """

    try:
        df.to_gbq(
            destination_table=TABLE_NAME,
            project_id=PROJECT_NAME,
            progress_bar=False,
            if_exists="append",  ### should be append
        )
        print("Data uploaded to BigQuery")
    except Exception as e:
        print("Couldn't upload data to BigQuery table, with error:")
        print(e, type(e))
        print("Saving data to bucket")
        storage_client = storage.Client()
        bucket = storage_client.list_buckets().client.bucket(BUCKET)
        file_name = f"SH_problem_data_{today_date}_{result}_RAW.csv"
        blob = bucket.blob(file_name)
        blob.upload_from_string(df.to_csv(index=False), content_type="csv/txt")
    return


def delete_pending_orders():
    """ "
    Deletes the pending orders from the BigQuery table

    This function deletes the pending orders from the BigQuery table every monday

    Parameters:
    - None
    Returns:
    - None
    """

    dt = datetime.now()
    if dt.weekday() == 0:  ##corrected df for dt
        ## Delete the previous 2 weeks every monday to get rid of the pending

        bq_cl_tmp = bigquery.Client()
        ## Changed to better select the last order in the previous 2 weeks
        q_tmp = f"""
        -- delete_pending_orders query
        DELETE
            `{PROJECT_NAME}.{TABLE_NAME}`
        WHERE
            CAST(order_number AS int64) > (
                SELECT
                    MAX(CAST(order_number AS int64))
                FROM
                    `{PROJECT_NAME}.{TABLE_NAME}`
                WHERE
                    created_at < CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) AS TIMESTAMP)
                -- today minus 14 days
            )
            """
        print("cleaning pending orders")
        try:
            del_job = bq_cl_tmp.query(q_tmp)  # Make an API request.
            del_job.result()
            print("Deleted last 2 weeks of orders")
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            print("Couldn'd delete the last 2 weeks orders")
    return


def get_last_order_id():
    """
    Gets the last order id from the BigQuery table

    Parameters:
    - None

    Returns:
    - result (int): The last order id that was registered in the database

    """

    ## Get data
    bigquery_client = bigquery.Client()

    ## query select the id correspondig to the last order in the table
    query = f"""
    ---- get_last_order_id query ----
    SELECT DISTINCT
        id
    FROM
        `{PROJECT_NAME}.{TABLE_NAME}`
    WHERE
        CAST(order_number AS integer) = (
            SELECT
                MAX(CAST(order_number AS integer))
            FROM
                `{PROJECT_NAME}.{TABLE_NAME}`
                )
    """
    ##### previously the max(name) caused problems because it was an string and the max string was #9999 and since then the orders were duplicated

    try:
        query_job = bigquery_client.query(query)  # Make an API request.
        rows = query_job.result()  # Waits for query to finish
        result = list(rows)[0]["id"]  ## last id registered in orders_master table

    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        print("starting from first order")
        # result = 2270011949127
        result = 9999999999999
    return result


def backup_deleted_orders(file_name_deleted,df_deleted):
    print("Saving to bucket last 2 weeks of orders")
    storage_client = storage.Client()
    bucket = storage_client.list_buckets().client.bucket(BUCKET)
    blob = bucket.blob(file_name_deleted)
    blob.upload_from_string(df_deleted.to_csv(index=False), content_type="csv/txt")
    return
