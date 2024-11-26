# requirements.txt
# pandas==1.5.1
# google-cloud-storage==1.44.0
# google-cloud-bigquery>=3.3.5
# pandas-gbq==0.17.9

import pandas as pd
from datetime import datetime, date

from orders_parameters import *
from utils_bq import *
from utils_df import *
from orders_process import *


def main(data, context):
    """
    whole process from check the last order, to make api calls until the data is updated, and save that data as a
    csv file in the bucket while uploading the same data to the orders table in BigQuery
    """
    dt = datetime.now()
    delete_pending_orders()
    result = get_last_order_id()

    df = get_all_orders(result)  ## 2270011949127 --> Reference id that works

    if not isinstance(
        df, pd.DataFrame
    ):  ### if the df is not a dataframe, it means that there is no new data
        return print("No new data to add")
    elif len(df) < 1:
        return print("No new data to add")
    # Clean data

    df = refunds(df)  ## new step for the refund column
    df = line_map(df)
    df = discount_process(df)

    ## Add time of creation
    df["UPDATED_FROM_API"] = datetime.now()

    # Change datetime using Series.dt.tz_localize() according to pandas doc
    # ref https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.astype.html
    df = type_change(df)
    df = stripe_process(df)

    today_date = date.today().strftime("%Y_%m_%d")
    file_name = f"SHOPIFY_ORDERS_{today_date}_{result}_RAW.csv"

    ## Upload to BQ
    upload_to_bq(df, today_date, result)
    if dt.weekday() == 0:
        backup_deleted_orders(file_name,df)
    return
