# functions-framework==3.*
# pandas>=1.2.2
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

dataset_id = os.environ.get('DATASET_ID')
table_1 = os.environ.get('TABLE_ID_1')
table_2 = os.environ.get('TABLE_ID_2')

def upload_rak_pay(df):
    rep_col = list(df.columns)
    rep_col[-1] = "report_id"
    df.columns = rep_col

    df.astype(
        {
            "report_id": "str",
        })

    df[df.columns[-1]] = df[df.columns[-1]].map(
        lambda x: None if str(x) in ['nan',np.nan] else str(x).replace('.0','')
                                            )
    div_index = []
    ini = 0
    for i in range(len(df)):
        if df['連番'].iloc[i] == '連番':
            ini += i
        if df['連番'].iloc[i] == '合計':
            div_index.append(i)

    ### df big
    df_1 = df.iloc[:div_index[0]].copy()
    df_1.columns = ['number',
                    'order_confirmation_date',
                    'order_number',
                    'payment_number',
                    'payment_institution_link_number',
                    'onquiry_number',
                    'payment_confirmation_date',
                    'rakuten_pay_money',
                    'payment_method',
                    'summary',
                    'report_id']

    df_1 = df_1.astype({
        'order_confirmation_date':'datetime64[ns]',
        'rakuten_pay_money':'float64',
        'payment_confirmation_date':'datetime64[ns]',
                    })
    df_1.reset_index(inplace=True,drop=True)
    df_1.drop(columns=['number'],inplace=True)

    ### df small
    df_2 = df.iloc[ini:-1].copy()
    new_col = df_2.iloc[0]
    df_2.columns = new_col
    df_2 = df_2.iloc[1:].copy()

    df_2.reset_index(inplace=True,drop=True)
    print(df_2.columns)
    df_2 = df_2.rename(columns={
        '連番':'number',
        '注文確認日':'order_confirmation_date',
        '受注番号':'order_number',
        'クーポン利用確定日':'coupon_confirmation_date',
        'クーポン利用額':'coupon_amount',
        'クーポン名':'coupon_name'
    })
    new_col_2 = [
                'number',
                'order_number',
                'order_confirmation_date',
                'coupon_name',
                'coupon_amount',
                'coupon_confirmation_date',
                'report_id'
                ]

    rep_col2 = list(df_2.columns)
    rep_col2[-1] = "report_id"
    df_2.columns = rep_col2

    df_2 = df_2[new_col_2]

    df_2.drop(columns=['number'],inplace=True)
    df_2 = df_2.astype({
        'report_id':'str',
        'coupon_amount':'float64',
        'order_confirmation_date':'datetime64[ns]',
        'coupon_confirmation_date':'datetime64[ns]',
    })

    df_1.to_gbq(destination_table=f'{dataset_id}.{table_1}',if_exists='append',progress_bar=False)
    df_2.to_gbq(destination_table=f'{dataset_id}.{table_2}',if_exists='append',progress_bar=False)

    return 'success'

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def update_payment(cloud_event):
    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]

    uri = f"gs://{bucket}/{name}"

    df = pd.read_csv(uri,header=3)
    upload_rak_pay(df)
