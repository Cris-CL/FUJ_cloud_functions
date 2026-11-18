# stripe-requests-reps

Weekly job that asks the Stripe Reporting API to generate two reports. It writes nothing itself: when Stripe finishes a
report it calls [stripe-webhook](../webhook/README.md), which stores the CSV, and
[bucket-stripe-bq](../bucket-stripe-bq/README.md) loads it into BigQuery.

| | |
|---|---|
| Status | active (`Shopify.stripe_api` / `Shopify.stripe_payouts` last modified 2026-10-11) |
| Folder | `cloud_stripe_report/create_report_stripe/` |
| Runtime / entry point | python310 (deprecated runtime) / `request_reports` in `main.py` (background function `(data, context)`) |
| Trigger | Pub/Sub `weekly_stripe_report`, published by Cloud Scheduler `stripe_weekly_report` (`0 8 * * 1` Asia/Tokyo, Mondays 08:00), no retry |
| Reads | `MAX(created_utc)` from `TABLE_QUERY` (`Shopify.stripe_api`); the last 10 Stripe report runs |
| Writes | Stripe report runs (no BigQuery or GCS writes) |

## How it works

1. `stripe_weekly()` (`main.py`): requests `balance_change_from_activity.itemized.2` from `MAX(created_utc)` in
   `Shopify.stripe_api` + 1 s to 23:59:59 three days before the run date (`midnight - 2 days - 1 s`).
2. `payout_request()` (`payouts.py`): lists the last 10 report runs, takes the latest `interval_end` of a
   `payout_reconciliation.itemized.5` run, and requests that report from there + 1 s to 23:59:59 two days before the
   run date (`midnight - 1 day - 1 s`). If the start is after the end, it logs
   "End time is before start time, aborting request" and requests nothing.

Dates are computed with `date.today()` in the function's server time (Cloud Functions runs in UTC).

## Dedup / reruns

The balance report starts after the latest row already in BigQuery, and the payout report after the latest payout
report run, so a rerun the same day requests (almost) nothing new. If the payout report run is not among the last
10 runs, `max_timestamp` falls back to 0 and the payout report would start at the Unix epoch.

## Configuration

Env vars (values in `deploy.sh`): `TABLE_QUERY` (table read for the last loaded balance row), `TABLE_QUERY_PAY`
(set live but not read by the code). Secret: `stripe_key` = Secret Manager `stripe` version 1.

## Deploy

```
make check stripe-requests-reps
make deploy stripe-requests-reps      # DRY_RUN=1 prints the command only
make describe stripe-requests-reps
```

## Known issues / open questions

- `python310` is deprecated; a redeploy may be refused or warned.
- The repo was synced on 2026-11-18 with the deployed source: the start > end guard in `payouts.py` was only in the
  deployed version; the repo's comment-only query label (`---- stripe_weekly query ----`) was dropped to match production.
