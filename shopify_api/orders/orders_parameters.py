import os

SHOPIFY_KEY = os.environ.get("SHOPIFY_KEY")
API_VERSION = os.environ.get("API_VERSION")
SHOPIFY_PASS = os.environ.get("SHOPIFY_PASS")
BUCKET = os.environ.get("BUCKET")
PROJECT_NAME = os.environ.get("PROJECT_NAME")
TABLE_NAME = os.environ.get("TABLE_NAME")
STORE_NAME = os.environ.get("STORE_NAME")
FULFILLMENT_TABLE = os.environ.get("FULFILLMENT_TABLE")

fields_api = [
    "id",
    "name",
    "email",
    "created-at",
    "cancelled-at",
    "confirmed",
    "cancel-reason",
    "buyer-accepts-marketing",
    "contact-email",
    "current-subtotal-price",
    "current-total-price",
    "total-outstanding",
    "current-total-tax",
    "current-total-discounts",
    "discount-codes",
    "financial-status",
    "fulfillment-status",
    "refunds",  ### added 18/01/23 for get the refund amounts
    "note",
    "number",
    "order-number",
    "payment-gateway-names",
    "processed-at",
    "reference",
    "source-identifier",
    "source-name",
    "line-items",
    "updated-at",
    # "gateway",
    # "processing-method", #### deprecated
    # "cart-token", #### deprecated
    # "checkout-token", #### deprecated
    # "token", #### deprecated
    "note-attributes",
    "checkout-id",
    "tags",
    "refering-site",
    "totat-line-items-price",
    "total_shipping_price_set",
    "subtotal-price",
    "total-price",
    "total-tax",
    "shipping-address",
    "total-discounts",  ### Added total discounts field
]

deprecated = [
    "gateway",
    "cart_token",  #### deprecated
    "checkout_token",  #### deprecated
    "token",  #### deprecated
    "processing_method",
]

dict_types = {
    "id": "str",
    "buyer_accepts_marketing": "bool",
    "cancel_reason": "str",
    "cancelled_at": "datetime64[ns]",
    "cart_token": "str",
    "checkout_id": "str",
    "checkout_token": "str",
    "confirmed": "str",
    "contact_email": "str",
    "created_at": "datetime64[ns]",
    "current_subtotal_price": "float64",
    "current_total_discounts": "float64",
    "current_total_price": "float64",
    "current_total_tax": "str",
    "email": "str",
    "financial_status": "str",
    "fulfillment_status": "str",
    "refund": "float64",
    "name": "str",
    "note": "str",
    "note_attributes": "str",
    "number": "str",
    "order_number": "str",
    "payment_gateway_names": "str",
    "processed_at": "datetime64[ns]",
    "processing_method": "str",
    "reference": "str",
    "source_identifier": "str",
    "source_name": "str",
    "tags": "str",
    "token": "str",
    "total_outstanding": "float64",
    "updated_at": "datetime64[ns]",
    "shipping_shop_amount": "float64",
    "shipping_presentment_amount": "float64",
    "lineitem_quantity": "float64",
    "lineitem_name": "str",
    "lineitem_price": "float64",
    "lineitem_sku": "str",
    "discount_code": "str",
    "discount_amount": "float64",
    "discount_type": "str",
    "subtotal_price": "float64",
    "total_price": "float64",
    "total_tax": "float64",
    "UPDATED_FROM_API": "datetime64[ns]",
    "ship_address1": "str",
    "ship_phone": "str",
    "ship_city": "str",
    "ship_zip": "str",
    "ship_province": "str",
    "ship_country": "str",
    "ship_first_name": "str",
    "ship_last_name": "str",
    "ship_address2": "str",
    "ship_company": "str",
    "ship_name": "str",
    "ship_latitude": "float64",
    "ship_longitude": "float64",
    "ship_country_code": "str",
    "ship_province_code": "str",
    "total_discounts": "float64",
    "lineitem_discount": "float64",
}
