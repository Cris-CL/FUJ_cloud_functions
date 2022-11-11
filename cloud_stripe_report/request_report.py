from google.cloud import storage
import stripe
import os
from datetime import datetime, date, time,timedelta

# def stripe_weekly():
#   """
#   Function generates a request for a report that covers from friday one week ago to saturday 2 days ago
#   assuming the function is triggered every monday
#   """

#   api_key_local = os.environ.get('stripe_key')
#   stripe.api_key = api_key_local

#   # today starting from 00:00
#   today = datetime.combine(date.today(), time())

#   start_time = today - timedelta(days=9)
#   start_var = str(int(start_time.timestamp()))

#   end_time = today - timedelta(days=2,seconds=1)
#   end_var = str(int(end_time.timestamp()))

#   ## Type of report to create
#   ## ref: https://stripe.com/docs/reports/report-types
#   report_var = "balance_change_from_activity.itemized.2"

#   report_dict = stripe.reporting.ReportRun.create(
#     report_type = report_var,
#     parameters={
#       'interval_start': start_var,
#       'interval_end': end_var,
#       'columns': [
#         'balance_transaction_id',
#         'created_utc',
#         'currency',
#         'gross',
#         'fee',
#         'net',
#         'reporting_category',
#         'source_id',
#         'description',
#         'customer_facing_amount',
#         'customer_facing_currency',
#         'customer_id',
#         'customer_email',
#         'customer_description',
#         'charge_id',
#         'payment_intent_id',
#         'invoice_id',
#         'payment_method_type',
#         ]
#     },)

# def main(data,context):
#   stripe_weekly()

print(date.today().strftime("%Y_%m_%d"))
