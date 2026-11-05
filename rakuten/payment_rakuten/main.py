# functions-framework==3.*
# pandas==1.5.1
# google-cloud-bigquery>=3.3.5
# fsspec>=2022.11.0
# gcsfs>=2022.11.0
# pandas-gbq>=0.17.9
# numpy>=1.23.4

import os
import functions_framework
import pandas as pd
import numpy as np
from google.cloud import bigquery


def get_complete_dataframe(uri):
    complete_df = pd.read_csv(uri, header=3)

    rep_col = list(complete_df.columns)
    rep_col[-1] = "report_id"
    complete_df.columns = rep_col

    complete_df.astype({"report_id": "str"})

    complete_df[complete_df.columns[-1]] = complete_df[complete_df.columns[-1]].map(
        lambda x: None if str(x) in ["nan", np.nan] else str(x).replace(".0", "")
    )
    div_index = []
    ini = 0
    for i in range(len(complete_df)):
        if complete_df["連番"].iloc[i] == "連番":
            ini += i
        if complete_df["連番"].iloc[i] == "合計":
            div_index.append(i)
    return [complete_df, div_index, ini]


def process_main_df(complete_df, div_index):
    df_payment = complete_df.iloc[: div_index[0]].copy()
    df_payment.columns = [
        "number",
        "order_confirmation_date",
        "order_number",
        "payment_number",
        "payment_institution_link_number",
        "onquiry_number",
        "payment_confirmation_date",
        "rakuten_pay_money",
        "payment_method",
        "summary",
        "report_id",
    ]
    for col_name in df_payment.columns:
        df_payment[col_name] = df_payment[col_name].map(
            lambda x: x.replace(",", "").replace(".", "") if isinstance(x, str) else x
        )
    df_payment = df_payment.astype(
        {
            "order_confirmation_date": "datetime64[ns]",
            "rakuten_pay_money": "float64",
            "payment_confirmation_date": "datetime64[ns]",
        }
    )
    df_payment.reset_index(inplace=True, drop=True)
    df_payment.drop(columns=["number"], inplace=True)

    return df_payment


def process_coupon_df(complete_df, ini):
    df_coupon = complete_df.iloc[ini:-1].copy()
    new_col = df_coupon.iloc[0]
    df_coupon.columns = new_col
    df_coupon = df_coupon.iloc[1:].copy()

    df_coupon.reset_index(inplace=True, drop=True)
    coupon_transaltion = {
        "連番": "number",
        "注文確認日": "order_confirmation_date",
        "課税資産譲渡日": "order_confirmation_date",
        "受注番号": "order_number",
        "クーポン利用確定日": "coupon_confirmation_date",
        "クーポン支払額確定日": "coupon_confirmation_date",
        "クーポン利用額": "coupon_amount",
        "クーポン名": "coupon_name",
    }

    df_coupon = df_coupon.rename(
        columns={col: coupon_transaltion.get(col, "error") for col in df_coupon.columns}
    )

    new_col_2 = [
        "number",
        "order_number",
        "order_confirmation_date",
        "coupon_name",
        "coupon_amount",
        "coupon_confirmation_date",
        "report_id",
    ]

    rep_col2 = list(df_coupon.columns)
    rep_col2[-1] = "report_id"
    df_coupon.columns = rep_col2

    df_coupon = df_coupon[new_col_2]

    df_coupon.drop(columns=["number"], inplace=True)

    for col_name in df_coupon.columns:
        df_coupon[col_name] = df_coupon[col_name].map(
            lambda x: x.replace(",", "").replace(".", "") if isinstance(x, str) else x
        )
    df_coupon = df_coupon.astype(
        {
            "report_id": "str",
            "coupon_amount": "float64",
            "order_confirmation_date": "datetime64[ns]",
            "coupon_confirmation_date": "datetime64[ns]",
        }
    )
    return df_coupon


def upload_df(df, df_name):

    dataset_id = os.environ.get("DATASET_ID")
    table_1 = os.environ.get("TABLE_ID_1")
    table_2 = os.environ.get("TABLE_ID_2")

    df_destination = {
        "main": f"{dataset_id}.{table_1}",
        "coupon": f"{dataset_id}.{table_2}",
    }
    try:
        df.to_gbq(
            destination_table=df_destination[df_name],
            if_exists="append",
            progress_bar=False,
        )
        print(f"{df_name} dataframe correctly uploaded")
    except Exception as err:
        print(f"Error {err} with type {type(err)} occurred")
        raise TypeError(f"{df_name} dataframe incorrect or missing")
    return


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def update_payment(cloud_event):
    data = cloud_event.data
    bucket = data["bucket"]
    name = data["name"]
    print(name)

    uri = f"gs://{bucket}/{name}"

    complete_df = get_complete_dataframe(uri)
    df_comp = complete_df[0]
    div_index = complete_df[1]
    ini = complete_df[2]

    try:
        df_main = process_main_df(df_comp, div_index)
    except Exception as err:
        print(f"Error {err} with type {type(err)} occurred")
        raise TypeError("main dataframe incorrect or missing")

    try:
        df_coupon = process_coupon_df(df_comp, ini)
    except Exception as err:
        print(f"Error {err} with type {type(err)} occurred")
        df_coupon = pd.DataFrame()
        # raise TypeError("coupon dataframe incorrect or missing")

    upload_df(df_main, "main")
    upload_df(df_coupon, "coupon")
    print("End upload process")
    return
