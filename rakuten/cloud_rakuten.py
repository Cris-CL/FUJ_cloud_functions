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

col_list = ['order_number',
 'status',
 'substatus_id',
 'sub_status',
 'order_date_time',
 'order_date',
 'order_time',
 'cancellation_deadline_date',
 'order_confirmation_date',
 'order_confirmed_date',
 'shipment_instruction_date_and_time',
 'shipment_completion_report_date',
 'payment_method_name',
 'credit_card_payment_method',
 'number_of_credit_card_payments',
 'shipping_method',
 'shipping_category',
 'order_type',
 'multiple_destination_flag',
 'destination_match_flag',
 'remote_island_flag',
 'rakuten_confirmation_flag',
 'warning_display_type',
 'rakuten_member_flag',
 'purchase_history_modification_flag',
 'product_total_price',
 'total_consumption_tax',
 'shipping_total',
 'cash_on_delivery_total',
 'billing_amount',
 'total_amount',
 'amount_of_points_used',
 'total_coupon_usage',
 'store_issued_coupon_usage_amount',
 'amount_of_coupons_issued_by_rakuten',
 'orderer_zip_code_1',
 'orderer_postal_code_2',
 'orderer_address_prefecture',
 "orderers_address",
 'orderer_address_subsequent_address',
 'last_name_of_orderer',
 'orderer_name',
 'orderer_surname_kana',
 "orderers_name_kana",
 'orderer_phone_number_1',
 'orderer_phone_number_2',
 'orderer_phone_number_3',
 'orderer_email_address',
 'gender_of_orderer',
 'application_number',
 'application_delivery_times',
 'recipient_id',
 'destination_postage',
 'delivery_address',
 'destination_consumption_tax_total',
 'total_amount_of_goods_to_be_sent_to',
 'destination_total_amount',
 'noshi',
 'destination_postal_code_1',
 'mailing_address_postal_code_2',
 'shipping_address_prefecture',
 'mailing_address_county_city',
 'shipping_address_subsequent_address',
 'mailing_to_last_name',
 'destination_name',
 'mailing_address_last_name_kana',
 'destination_name_kana',
 'destination_phone_number_1',
 'mailing_address_phone_number_2',
 'mailing_address_phone_number_3',
 'product_details_id',
 'product_id',
 'product_name',
 'item_number',
 'merchandise_control_number',
 'unit_price',
 'quantity',
 'shipping_fee_not_included',
 'tax_not_included',
 'cod_fee_not_included',
 'item_choice',
 'point_multiplier',
 'delivery_date_information',
 'inventory_type',
 'wrapping_title_1',
 'wrapping_name_1',
 'wrapping_fee_1',
 'lapping_including_tax_1',
 'wrapping_type_1',
 'wrapping_title_2',
 'wrapping_name_2',
 'wrapping_fee_2',
 'wrapping_tax_included_2',
 'wrapping_type_2',
 'delivery_time_zone',
 'specify_delivery_date',
 'manager',
 'note',
 'mail_insert_message_to_customer',
 'request_for_gift_delivery',
 'comment',
 'terminal_used',
 'mail_carrier_code',
 'tomorrow_raku_hope_flag',
 'pharmaceutical_order_flag',
 'rakuten_super_deal_product_order_flag',
 'membership_program_order_type',
 'settlement_fee',
 "orderers_contribution_total",
 'total_store_charges',
 'excluded_tax_total',
 'settlement_fee_tax_rate',
 'wrapping_tax_rate_1',
 'wrapping_tax_amount_1',
 'wrapping_tax_rate_2',
 'wrapping_tax_amount_2',
 'total_destination_tax',
 'destination_postage_tax_rate',
 'shipping_destination_cod_tax_rate',
 'commodity_tax_rate',
 'tax_included_price_for_each_product',
 'tax_rate_10_percent',
 'billed_amount_10_percent',
 'tax_on_billed_amount_10_percent',
 'total_amount_10_percent',
 'settlement_fee_10_percent',
 'coupon_discount_amount_10_percent',
 'usage_points_10_percent',
 'tax_rate_8_percent',
 'billed_amount_8_percent',
 'tax_on_billed_amount_8_percent',
 'total_amount_8_percent',
 'settlement_fee_8_percent',
 'coupon_discount_amount_8_percent',
 'points_used_8_percent',
 'tax_rate_0_percent',
 'billed_amount_0_percent',
 'tax_on_invoice_amount_0_percent',
 'total_amount_0_percent',
 'payment_fee_0_percent',
 'coupon_discount_amount_0_percent',
 'usage_points_0_percent',
 'single_item_delivery_flag',
 'delivery_company_at_the_time_of_purchase',
 'number_of_receipts_issued',
 'first_issue_date_and_time_of_receipt',
 'date_and_time_of_last_issue_of_receipt',
 'payment_due_date',
 'deadline_for_changing_payment_method',
 'deadline_for_refund_procedure',
 'store_issued_coupon_code',
 'store_issued_coupon_name',
 'rakuten_issued_coupon_code',
 'rakuten_issued_coupon_name',
 'warning_display_type_details',
           ]

def format_rakuten(df):
    dict_col = {
        "order_date_time":"datetime64[ns]",
        "order_date":"datetime64[ns]",
        "order_time":"datetime64[ns]",
        "cancellation_deadline_date":"datetime64[ns]",
        "order_confirmation_date":"datetime64[ns]",
        "order_confirmed_date":"datetime64[ns]",
        "shipment_instruction_date_and_time":"datetime64[ns]",
        "shipment_completion_report_date":"datetime64[ns]",
        "first_issue_date_and_time_of_receipt":"datetime64[ns]",
        "date_and_time_of_last_issue_of_receipt":"datetime64[ns]",
        "payment_due_date":"datetime64[ns]",
        "deadline_for_changing_payment_method":"datetime64[ns]",
        "deadline_for_refund_procedure":"datetime64[ns]",


        'product_total_price':"float64",
        'number_of_credit_card_payments':"float64",

        'total_consumption_tax':"float64",
        'shipping_total':"float64",
        'cash_on_delivery_total':"float64",
        'billing_amount':"float64",
        'total_amount':"float64",
        'amount_of_points_used':"float64",
        'total_coupon_usage':"float64",
        'store_issued_coupon_usage_amount':"float64",
        'amount_of_coupons_issued_by_rakuten':"float64",
        'total_amount_of_goods_to_be_sent_to':"float64",
        'destination_total_amount':"float64",
        'unit_price':"float64",
        'quantity':"float64",
        'settlement_fee':"float64",
        'total_store_charges':"float64",
        'excluded_tax_total':"float64",
        'mail_insert_message_to_customer':"str"
    }
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
    df.columns = col_list
    df["UPDATED_AT_UTC"] = datetime.utcnow()
    df["FILE_NAME"] = name
    df = format_rakuten(df)

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
