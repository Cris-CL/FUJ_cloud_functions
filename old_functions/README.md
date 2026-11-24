# Retired / not deployed function code

Kept for reference only. None of this is deployed in `test-bigquery-cc` (checked against `gcloud functions list` on
2026-11-18), none has a `deploy.sh`, and `make` does not know about it. Moved here with `git mv`, so `git log --follow`
shows the history at the old path.

| Path | Old path | What it was | Why retired | Last known output |
|---|---|---|---|---|
| `shopify_refunds/` | `shopify_api/refunds/` | `get_refunds(data, context)`: Shopify refunds pull | deprecated (`deprecated.md`); refunds are handled in the orders flow (`refunds(df)` in `shopify_api/orders/utils_df.py`) | `Shopify.refunds_master` (2023) |
| `stripe_charges_test/` | `stripe/` | `main(data, context)`: last 100 Stripe charges to CSV | test | `gs://test-bucket-function-cc/<date>_stripe.csv` |
| `test/` | `test/` | early experiments: `main.py` (public API to CSV), `1208/` (hello-world HTTP + blob move helper), `stripe-update-1/` (Stripe charges to GCS) | test | `gs://test-bucket-function-cc/` |
| `webhook_hello/` | `webhook/` | `hello_get` hello-world HTTP function + `how_to.txt` deploy notes | test | none |
| `payout_wh.py` | `cloud_stripe_report/webhook/payout_wh.py` | older copy of the Stripe payout report request | duplicate of `payout_request()` in `cloud_stripe_report/create_report_stripe/payouts.py`; never in the deployed stripe-webhook source | none (requests a Stripe report) |
| `stripe-payout-report.py` | (already here) | `payout(data, context)`: earlier weekly Stripe payout report requester | superseded by stripe-requests-reps | none (requests a Stripe report) |
