# stripe-webhook

HTTP endpoint that Stripe calls when a report run finishes. It downloads the report CSV and stores it in
`fujiorg-stripe-raw`, which triggers [bucket-stripe-bq](../bucket-stripe-bq/README.md). The reports are requested by
[stripe-requests-reps](../create_report_stripe/README.md).

| | |
|---|---|
| Status | active (`Shopify.stripe_api` / `Shopify.stripe_payouts` last modified 2026-10-11) |
| Folder | `cloud_stripe_report/webhook/` |
| Runtime / entry point | python312 / `webhook` in `main.py` (HTTP) |
| Trigger | HTTP, public (`allUsers` has `roles/run.invoker`); URL `https://us-central1-test-bigquery-cc.cloudfunctions.net/stripe-webhook` |
| Reads | Stripe webhook events; the report file at `result.url` (Stripe API, basic auth with the API key) |
| Writes | `gs://fujiorg-stripe-raw/PAY_<file>` and `gs://fujiorg-stripe-raw/BAL_<file>` |

## How it works

1. Verifies the `Stripe-Signature` header against the webhook secret (`stripe.Webhook.construct_event`); invalid payload
   or signature -> HTTP 400.
2. `reporting.report_run.succeeded`: starts a background thread running `process_report.process_report`, and returns
   `{"success": true}` immediately. The thread downloads the report and uploads it to `STRIPE_BUCKET`:
   - `payout_reconciliation.itemized.5` -> `PAY_<stripe file name>`
   - `balance_change_from_activity.itemized.2` -> `BAL_<stripe file name>`
3. `payout.paid`: only logs the arrival date. Any other event type: logged as unhandled.

The URL is registered in Stripe for `reporting.report_run.succeeded`; renaming or moving the function breaks it.

## Dedup / reruns

Object names come from Stripe's report file name, so a re-delivered event overwrites the same object (and triggers
bucket-stripe-bq again, which appends duplicates; see its README).

## Configuration

Env vars (values in `deploy.sh`): `STRIPE_BUCKET` (`fujiorg-stripe-raw`, where reports are written),
`STRIPE_PAYOUT_BUCKET` and `TABLE_QUERY` (set live, `STRIPE_PAYOUT_BUCKET` is read but not used, `TABLE_QUERY` is not read).
Secrets: `stripe_key` = Secret Manager `stripe` version 1, `web_secret` = `web_hook_stripe` version 1.

## Deploy

```
make check stripe-webhook
make deploy stripe-webhook      # DRY_RUN=1 prints the command only
make describe stripe-webhook
```

`deploy.sh` passes `--allow-unauthenticated` to keep the live public invoker binding; `make check` verifies it.

## Known issues / open questions

- Upgraded from python310 (deprecated) to python312 on 2026-11-18 (Flask 2.2 -> 3.1, google-cloud-storage 1.44 -> 3.17,
  stripe pinned to 16.1.0), tested offline with signed test events on both the old and the new stack.
- The report is processed in a thread after the HTTP response is sent. On Cloud Functions Gen 2 / Cloud Run, CPU may
  be throttled after the response, so the upload can be slow or lost; a failure there is only visible in the logs
  (Stripe has already received 200 and does not retry).
- `requirements.txt` now lists `requests` (imported by `process_report.py`, previously only installed via `stripe`) and
  no longer lists `pytz` and `google-cloud-bigquery`, which nothing imports.
- The older unused payout request code `payout_wh.py` was moved to `old_functions/` on 2026-11-18 (it was never in the
  deployed source).
