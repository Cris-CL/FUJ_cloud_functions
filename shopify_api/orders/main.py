# requirements.txt
# pandas==1.5.1
# google-cloud-storage==1.44.0
# google-cloud-bigquery>=3.3.5
# pandas-gbq==0.17.9

import pandas as pd
from datetime import datetime, date
import pytz
from orders_parameters import *
from utils_bq import *
from utils_df import *
from orders_process import get_all_orders, other_payments_process
from fulfillments import *
from flask import jsonify


def main(data=None, context=None):
    """
    whole process from check the last order, to make api calls until the data is updated, and save that data as a
    csv file in the bucket while uploading the same data to the orders table in BigQuery
    """
    dt = datetime.now()
    delete_pending_orders()
    result = get_last_order_id()

    df = get_all_orders(result)  ## 2270011949127 --> Reference id that works

    ### if the df is not a dataframe, it means that there is no new data
    if not isinstance(df, pd.DataFrame):
        print("No new data to add")
        return jsonify({"status": "success"}), 200
    elif len(df) < 1:
        print("No new data to add")
        return jsonify({"status": "success"}), 200
    # Clean data

    df = refunds(df)  ## new step for the refund column
    df = line_map(df)
    df = discount_process(df)

    ## Add time of creation
    df["UPDATED_FROM_API"] = datetime.now()

    # Change datetime using Series.dt.tz_localize() according to pandas doc
    # ref https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.astype.html
    df = type_change(df)
    df = other_payments_process(df)

    today_date = date.today().strftime("%Y_%m_%d")
    file_name = f"SHOPIFY_ORDERS_{today_date}_{result}_RAW.csv"

    ## Upload to BQ
    upload_to_bq(df, today_date, result)
    if dt.weekday() == 0:
        backup_deleted_orders(file_name, df)

    try:
        new_order_ids = get_order_ids(df)
        fulf_df = get_fullfillments(new_order_ids)
        clean_fulfillments(new_order_ids)
        fulf_df["UPLOADED_DATETIME"] = datetime.now(pytz.timezone("Asia/Tokyo"))
        upload_fulfillments(fulf_df)
    except Exception as e:
        print(f"Error in fulfillment process: {e}")

    http_status = jsonify({"status": "success"}), 200

    return http_status
