# from google.cloud import bigquery
# import stripe
# import os
# from datetime import datetime, date, time,timedelta

from google.cloud import bigquery
import stripe
import os
from datetime import datetime, date, time,timedelta

def payout_request():

    """
    Function generates a request for a report that covers from friday one week ago to saturday 2 days ago
    assuming the function is triggered every monday
    """

    api_key_local = os.environ.get('stripe_key')
    table_q_p = os.environ.get('TABLE_QUERY_PAY')
    stripe.api_key = api_key_local

    ## Query to get last transaction in bq
    query =f"""SELECT max(charge_created_utc) FROM `{table_q_p}`""" ## ORIGINAL
    client_q = bigquery.Client()
    query_job = client_q.query(query)  # Make an API request.
    rows = query_job.result()
    result = list(rows)[0]
    last_result_timestamp = int(datetime.timestamp(result[0])) + 1
    ## Start period from last payment in table
    start_var = last_result_timestamp

    ## end_time is at 23:59:59 hrs 1 day before the execution of the funciton

    today = datetime.combine(date.today(), time())
    end_time = today - timedelta(days=1,hours=5,seconds=1)
    end_var = str(int(end_time.timestamp()))

    ## Type of report to create
    ## ref: https://stripe.com/docs/reports/report-types
    report_var = "payout_reconciliation.itemized.5"

    report_dict = stripe.reporting.ReportRun.create(
    report_type = report_var,
    parameters={
      'interval_start': start_var,
      'interval_end': end_var,
      'columns': [
        'automatic_payout_id',
        'automatic_payout_effective_at',
        'balance_transaction_id',
        'created_utc',
        'created',
        'available_on_utc',
        'available_on',
        'currency',
        'gross',
        'fee',
        'net',
        'reporting_category',
        'source_id',
        'description',
        'customer_facing_amount',
        'customer_facing_currency',
        'customer_id',
        'customer_email',
        'charge_id',
        'payment_intent_id',
        'charge_created_utc',
        'invoice_id',
        'order_id',
        'payment_method_type',
        'connected_account_id',
        'connected_account_name',
        ]
    },)

    return
