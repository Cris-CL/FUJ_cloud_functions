import os

table_1 = os.environ.get("TABLE_1")
table_2 = os.environ.get("TABLE_2")
dataset = os.environ.get("DATASET_ID")
project = os.environ.get("PROJECT_ID")
new_bucket = os.environ.get("NEW_BUCKET")


year = "2024"  #### Changed year to current one

rep_classifier = {
    "tv": {
        "destination_table": table_2,
        "prefix": "TV",
        "folder": f"transaction_view/settlement_{year}",  ### Modify year when it changes
    },
    "oc": {
        "destination_table": table_1,
        "prefix": "OC",
        "folder": f"order_central/sales_{year}",  ### Modify year when it changes
    },
}

data_types = {
    "tv": {
        "settlement_id": "float",
        "total_amount": "float",
        "currency": "str",
        "transaction_type": "str",
        "order_id": "str",
        "merchant_order_id": "str",
        "adjustment_id": "str",
        "shipment_id": "str",
        "marketplace_name": "str",
        "amount_type": "str",
        "amount_description": "str",
        "amount": "float",
        "fulfillment_id": "str",
        "order_item_code": "str",
        "merchant_order_item_id": "str",
        "merchant_adjustment_item_id": "float",
        "sku": "str",
        "quantity_purchased": "float",
        "promotion_id": "str",
    },
    "oc": {
        "amazon_order_id": "str",
        "merchant_order_id": "str",
        "order_status": "str",
        "fulfillment_channel": "str",
        "sales_channel": "str",
        "order_channel": "str",
        "url": "str",
        "ship_service_level": "str",
        "product_name": "str",
        "sku": "str",
        "asin": "str",
        "item_status": "str",
        "quantity": "int64",
        "currency": "str",
        "item_price": "float",
        "item_tax": "float",
        "shipping_price": "float",
        "shipping_tax": "float",
        "gift_wrap_price": "float",
        "gift_wrap_tax": "float",
        "item_promotion_discount": "float",
        "ship_promotion_discount": "float",
        "ship_city": "str",
        "ship_state": "str",
        "ship_postal_code": "str",
        "ship_country": "str",
        "promotion_ids": "str",
    },
}

datetime_format = {"tv": "%d.%m.%Y %H:%M:%S %Z", "oc": "%Y-%m-%dT%H:%M:%S%z"}

date_format = "%d.%m.%Y"

datetime_columns = {
    "tv": [
        "settlement_start_date",  # datetime
        "settlement_end_date",  # datetime
        "deposit_date",  # datetime
        "posted_date",  # date
        "posted_date_time",  # datetime
    ],
    "oc": [
        "purchase_date",
        "last_updated_date",
    ],
}

date_columns = {"tv": ["posted_date"]}

dtypes_dict = {
    "object": "STRING",
    "int64": "INTEGER",
    "float64": "FLOAT",
    "datetime64[ns]": "DATETIME",
    "datetime64[ns, UTC]": "DATETIME",
    "datetime64[ns, pytz.FixedOffset(540)]": "DATETIME",
}

disposition = "WRITE_APPEND"
