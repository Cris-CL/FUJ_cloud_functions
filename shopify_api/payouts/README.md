# FUJ_shopify_payouts

Weekly pull of Shopify Payments balance transactions into BigQuery.

| | |
|---|---|
| Status | active (`Shopify.shopify_payouts_api` last modified 2026-10-12; table recreated 2025-10-06) |
| Folder | `shopify_api/payouts/` |
| Runtime / entry point | python312 / `update_shopify_payout` in `main.py` (background signature `(data, context)`) |
| Trigger | Pub/Sub `shopify_payouts_weekly`, published by Cloud Scheduler `shopify_weekly_payments` (`0 11 * * 1` Asia/Tokyo, Mondays 11:00), no retry |
| Reads | Shopify Admin REST `shopify_payments/balance/transactions.json` (`since_id` paging, limit 250); max id in the table |
| Writes | `Shopify.shopify_payouts_api` (delete + WRITE_APPEND); `SHOPIFY_PAYOUTS_<date>_<id>_RAW.csv` in `gs://fujiorg-shopify-payouts-raw/` |

## How it works

1. Deletes every row from the oldest `pending` / `in_transit` transaction onward, so unsettled rows are re-pulled.
2. Reads `MAX(id)` of what is left (0 if the query fails) and pages through all newer balance transactions.
3. Casts the columns, blanks `nan` / `none` / empty strings, adds `UPDATED_FROM_API`, appends to
   `Shopify.<TABLE_NAME_PAY>` and saves the same data as a CSV in `BUCKET`.

## Dedup / reruns

Incremental by transaction id after the delete of unsettled rows, so a rerun re-pulls only those and anything newer.

## Configuration

Env vars (values in `deploy.sh`): `API_VERSION`, `BUCKET`, `PROJECT_NAME`, `STORE_NAME`, `TABLE_NAME_PAY` (table name
inside dataset `Shopify`, hard-coded in the queries). Secrets: `SHOPIFY_KEY` = Secret Manager `SH_PAY_API_KEY:1`,
`SHOPIFY_PASS` = `SH_PAY_PASS:1`. The git-ignored `shopify_api/.env` is a local test config
(`TABLE_NAME_PAY=shopify_payouts_api_update`), not used by the deployed function.

## Deploy

```
make check FUJ_shopify_payouts
make deploy FUJ_shopify_payouts      # DRY_RUN=1 prints the command only
make describe FUJ_shopify_payouts
```

The repo equals the deployed source (python312, current dependency pins), which makes it the safest first function to
deploy through `make deploy` to prove the pipeline.

## Known issues / open questions

- `get_new_payouts` (ShopifyAPI package) is defined but not called; the pull uses plain `requests`.
- Shopify credentials are embedded in the request URL.
