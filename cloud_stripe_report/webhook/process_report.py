import requests
from google.cloud import storage


def process_report(report_run, bucket_name, api_key_local):
    file_name_var = report_run["result"]["filename"]
    url_report = report_run["result"]["url"]

    # Stream the content directly without converting to text
    with requests.get(
        url_report, auth=(api_key_local, api_key_local), stream=True
    ) as r:
        r.raise_for_status()
        content = r.content

    storage_client = storage.Client()

    if report_run["report_type"] == "payout_reconciliation.itemized.5":
        file_name = f"PAY_{file_name_var}"
    elif report_run["report_type"] == "balance_change_from_activity.itemized.2":
        file_name = f"BAL_{file_name_var}"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Upload the content as is (CSV)
    blob.upload_from_string(content, content_type="text/csv")
