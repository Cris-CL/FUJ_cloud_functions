# FUJ_shopify_orders

Daily pull of new Shopify orders (and their fulfilments) from the Shopify Admin REST API into BigQuery.

| | |
|---|---|
| Status | active (`Shopify.orders_master` / `Shopify.fulfillments` modified 2026-10-16) |
| Folder | `shopify_api/orders/` |
| Runtime / entry point | python312 / `main` in `main.py` (background signature `(data, context)`, returns a Flask `jsonify(...)`, 200) |
| Trigger | Pub/Sub `shopify_orders`, published by Cloud Scheduler `shopify_daily_orders` (`0 10 * * *` Asia/Tokyo, daily 10:00), no retry |
| Reads | Shopify Admin REST: `orders.json` (paged by `since_id`), `orders/{id}/transactions.json` (Stripe / Komoju gateway orders), `orders/{id}/fulfillments.json`; max order id in `Shopify.orders_master` |
| Writes | `Shopify.orders_master` (WRITE_APPEND), `Shopify.fulfillments` (delete + append for the new orders); CSVs in `gs://fujiorg-shopify-orders-raw/` |

## How it works

1. `delete_pending_orders()` (`utils_bq.py`): on Mondays only, deletes every order whose `order_number` is above the
   highest one created more than 14 days ago, so the last two weeks are re-pulled with their final status.
2. Reads the last loaded order id and pulls every newer order (`orders_process.get_all_orders`).
3. Cleans the data (`utils_df.py`: refunds column, line items, discounts, types) and joins the `sale` transactions of
   Stripe / Komoju gateway orders (`other_payments_process`).
4. Appends to `TABLE_NAME` (`Shopify.orders_master`). If that upload fails, the data is written to
   `SH_problem_data_<date>_<id>_RAW.csv` in `BUCKET`. On Mondays the pulled orders are also saved as
   `SHOPIFY_ORDERS_<date>_<id>_RAW.csv` in `BUCKET`.
5. Fetches the fulfilments of the new orders, deletes their old rows in `FULFILLMENT_TABLE` and appends the new ones
   (`UPLOADED_DATETIME` in Asia/Tokyo). A failure here is logged and does not fail the run.

"Monday" is `datetime.now().weekday() == 0` in server time (UTC). The scheduler fires at 10:00 Asia/Tokyo = 01:00 UTC,
so the Monday run in Tokyo is also Monday in UTC.

## Dedup / reruns

Incremental from the highest loaded order (`since_id`), so a rerun the same day only adds orders created since.
The Monday delete + re-pull refreshes the last 14 days. Commit `9165893` joins only `sale` transactions to avoid
duplicated reference ids.

## Configuration

Env vars (values in `deploy.sh`): `API_VERSION`, `BUCKET`, `FULFILLMENT_TABLE`, `PROJECT_NAME`, `STORE_NAME`,
`TABLE_NAME`. Secrets: `SHOPIFY_KEY` = Secret Manager `api_key_shop:latest`, `SHOPIFY_PASS` = `shopy_password:latest`.
The git-ignored `shopify_api/.env` is a local test config (`TABLE_NAME=Shopify.orders_api_update`, a table that does
not exist), not used by the deployed function.

## Deploy

```
make check FUJ_shopify_orders
make deploy FUJ_shopify_orders      # DRY_RUN=1 prints the command only
make describe FUJ_shopify_orders
```

## Known issues / open questions

- **Repo ahead of deployed (not deployed yet).** The deployed source (2026-06-02, edited in the console) predates the
  repo: `fulfillments.py` and the transactions fetch in `orders_process.py` now use a retrying HTTP session
  (`urllib3` `Retry`: 3 total, backoff 1 s, GET only) and fetch everything first, then normalise, so one failing order
  is skipped instead of aborting. `make check FUJ_shopify_orders` therefore reports SOURCE DRIFT on those two files
  until the owner deploys it.
- Shopify credentials are embedded in the request URL (`https://key:pass@store...`).
