import stripe
import os
from datetime import datetime, date, time,timedelta

def payout_request():

    """
    Function generates a request for a report that covers from one second after the last report until 2 days ago
    """

    api_key_local = os.environ.get('stripe_key')
    stripe.api_key = api_key_local
    report_var = "payout_reconciliation.itemized.5"

    ## end_time is at 23:59:59 hrs 1 day before the execution of the funciton
    reportes_stripe = stripe.reporting.ReportRun.list(limit=10)
    max_timestamp = 0

    for rep in reportes_stripe["data"]:
        if rep["report_type"] == report_var:
            max_timestamp = max(max_timestamp,rep["parameters"]["interval_end"])

    start_var = max_timestamp + 1

    today = datetime.combine(date.today(), time())
    end_time = today - timedelta(days=1,hours=0,seconds=1) ## today date minus 1 day and 1 second --> At 23:59:59 2 days ago from execution time
    end_var = str(int(end_time.timestamp()))

    ## Type of report to create
    ## ref: https://stripe.com/docs/reports/report-types


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
