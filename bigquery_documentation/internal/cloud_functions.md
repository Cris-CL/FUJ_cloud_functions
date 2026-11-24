# Cloud Functions, loaders and Apps Scripts

Documented as of 2026-10-16.

Paths are relative to the `cloud_functions` repo root. The Komoju project is a sibling folder and is written
as `../komoju/...`. GCP project everywhere: `test-bigquery-cc`.

Every Cloud Function in this repo now lives alone in its own folder with a `deploy.sh` (the live configuration) and a
`README.md` (what it does, trigger, tables, dedup, open questions). The function sections below are pointers to those
READMEs; the index is in the [root README](../../README.md). Deploy and drift-check from the repo root with
`make deploy <function>` / `make check <function>` (updated 2026-11-18 from `gcloud functions describe`).
Secret values are never written here (see [security_notes.md](security_notes.md)).

## Known buckets

| Bucket | Role |
|---|---|
| `fujiorg-sales-data` | Processed archive. `Amazon/order_central/sales_<year>/`, `Amazon/transaction_view/settlement_<year>/`, `Amazon/repeated_files/`, `Shopify/amazon_pay/<year>/`, `Shopify/amazon_pay/RAW/`, `Shopify/stripe/payments/`, `Shopify/stripe/balance/`. |
| `test-bucket-function-cc` | Shopify function scratch / backups (`SH_problem_data_*`, `SHOPIFY_ORDERS_*`, `SHOPIFY_PAYOUTS_*`), old Stripe test output. |
| `komoju_stage` | Komoju webhook output (`payloads/`, `SET_<ref>.csv`) and trigger bucket of the Komoju BigQuery loader. |
| `fuj_backup` | Target of the backup notebook (`notebooks/backup_database.ipynb`, CSV exports of `free.*` under `Reports/`). |
| `amazon-raw` | Trigger bucket of `upload-amazon` (Amazon JP reports). |
| `amazon_sg` | Trigger bucket of `amazon_sg` (Amazon SG reports, legacy). |
| `rakuten_orders_raw` | Trigger bucket of `rakuten-update` (RMS order exports). |
| `rakuten_payment_raw` | Trigger bucket of `payment-rakuten` (楽天ペイ payment CSVs and Billpay statements). |
| `fujiorg-stripe-raw` | `STRIPE_BUCKET` of `stripe-webhook` (`BAL_` / `PAY_` Stripe reports) and trigger bucket of `bucket-stripe-bq` (also `AMA_` Amazon Pay files). |
| `stripe-payouts` | `STRIPE_PAYOUT_BUCKET` of `stripe-webhook`; set but not used by the code. |
| `fujiorg-shopify-orders-raw`, `fujiorg-shopify-payouts-raw` | `BUCKET` of the two Shopify functions (raw CSV backups). |

## Summary

| # | Flow | Entry point | Trigger | Writes | Status |
|---|---|---|---|---|---|
| 1 | Amazon JP loader (`upload-amazon`) | `amazon_process` | GCS finalize on `amazon-raw` | `Amazon.order_central_master`, `Amazon.transaction_view_master` | active |
| 2 | Amazon SG loader (`amazon_sg`) | `amazon_sg_process` | GCS finalize on `amazon_sg` | `Amazon.oc_sg`, `Amazon.tv_sg` | legacy |
| 3 | Shopify orders + fulfilments (`FUJ_shopify_orders`) | `main` | Pub/Sub `shopify_orders`, scheduler daily 10:00 JST | `Shopify.orders_master`, `Shopify.fulfillments` | active |
| 4 | Shopify payouts (`FUJ_shopify_payouts`) | `update_shopify_payout` | Pub/Sub `shopify_payouts_weekly`, scheduler Mon 11:00 JST | `Shopify.shopify_payouts_api` | active (table updated 2026-10-12) |
| 5 | Shopify refunds | `get_refunds` | n/a (not deployed) | refunds table | retired, `old_functions/shopify_refunds/` |
| 6 | Komoju webhook | `webhook` | HTTP (KOMOJU `settlement.created`) | `gs://komoju_stage` | active |
| 7 | Komoju BigQuery loader | `load_csv` | GCS finalize on `komoju_stage` | `Shopify.komoju_payments` | active |
| 8 | Stripe report request (`stripe-requests-reps`) | `request_reports` | Pub/Sub `weekly_stripe_report`, scheduler Mon 08:00 JST | Stripe Reporting API run | active |
| 9 | Stripe webhook (`stripe-webhook`) | `webhook` | HTTP (Stripe webhook) | `gs://fujiorg-stripe-raw` | active |
| 10 | Stripe / Amazon Pay bucket loader (`bucket-stripe-bq`) | `upload_stripe_bq` | GCS finalize on `fujiorg-stripe-raw` | `Shopify.stripe_api`, `Shopify.stripe_payouts`, Amazon Pay table | active (Stripe tables updated 2026-10-11; Amazon Pay last 2025-01-10) |
| 11 | Rakuten orders loader (`rakuten-update`) | `upload_rakuten_bq` | GCS finalize on `rakuten_orders_raw` | `Rakuten.orders` | active |
| 12 | Rakuten payments and statements loader (`payment-rakuten`) | `update_payment` | GCS finalize on `rakuten_payment_raw` | `Rakuten.payment_main`, `Rakuten.payment_coupon`, `Rakuten.statement`, `free.rakuten_cost_full` | active |
| 13 | Apps Script nomenclature loader | `use_local_nomenclature22` | manual, in Sheets | `temp_nomenclature.nomenclature_temp` | legacy |
| 14 | Other Apps Scripts / local tools | various | manual | various | mixed |

---

## 1. Amazon JP loader (active)

`upload-amazon`: see [amazon/upload_amazon/README.md](../../amazon/upload_amazon/README.md).

## 2. Amazon SG legacy loader (legacy)

`amazon_sg` (still deployed): see [amazon/amazon_sg/README.md](../../amazon/amazon_sg/README.md).

## 3. Shopify orders + fulfilments (active)

`FUJ_shopify_orders`: see [shopify_api/orders/README.md](../../shopify_api/orders/README.md) (repo ahead of the deployed
source: retrying HTTP session, not deployed yet).

## 4. Shopify payouts (active)

`FUJ_shopify_payouts`: see [shopify_api/payouts/README.md](../../shopify_api/payouts/README.md).

## 5. Shopify refunds (legacy)

Retired, not deployed: moved to `old_functions/shopify_refunds/` (see [old_functions/README.md](../../old_functions/README.md)).
Refunds are handled inside the orders flow (`refunds(df)` in `shopify_api/orders/utils_df.py`). Old output: `Shopify.refunds_master` (2023).

## 6. Komoju webhook (active)

- **Path:** `../komoju/functions/webhook/` (`main.py`, `komoju_handler_v3.py`, `settlement_report_v3.py`, `.env.yaml`).
  Deployment docs: `../komoju/docs/DEPLOY.md`, `../komoju/docs/CODE_OVERVIEW.md`; deploy with `../komoju/scripts/deploy.sh webhook`.
- **Entry point:** `webhook(request)`, `@functions_framework.http`. Function `komoju-settlement-webhook`, gen2, python312,
  asia-northeast1, `--allow-unauthenticated` (protected by HMAC-SHA256 check of `X-Komoju-Signature`).
- **Trigger:** HTTP, called by KOMOJU on `settlement.created`.
- **Env vars:** `WEBHOOK_SECRET_TOKEN` (secret), `BUCKET_NAME`, `KOMOJU_SECRET_KEY` (secret). Values redacted.
- **External API:** KOMOJU REST `GET /api/v1/settlements/<id>/csv` (Basic auth, API version header 2025-01-28). The download
  URLs inside the payload are ignored (they were incomplete).
- **Bucket out:** `gs://komoju_stage/payloads/<event_type>_<event_id>.json` (raw event) and `gs://komoju_stage/SET_<reference>.csv`
  (clean settlement CSV, reference like `DGC-27670-464471`; lower-case columns, `payment_id` column added, blank `transfer_date`
  filled with the most frequent value).
- **Dedup:** deterministic object names, so KOMOJU retries overwrite the same objects. Errors return 5xx so KOMOJU retries.
- **Status:** active (deployed 2026-08). Superseded versions in `../komoju/old/`.

## 7. Komoju BigQuery loader (active)

- **Path:** `../komoju/functions/bq_loader/main.py` (+ `.env.yaml`). Function `komoju-bq-loader`.
- **Entry point:** `load_csv(cloud_event)`, `@functions_framework.cloud_event`.
- **Trigger:** Eventarc GCS finalize on `komoju_stage` (bucket location US, function in asia-northeast1, `--retry`). Only `SET_*.csv` objects are processed.
- **Env vars:** `BQ_TABLE`.
- **Destination table:** `Shopify.komoju_payments` (WRITE_APPEND load job, columns reordered to the table schema). Last modified 2026-09-30.
- **Dedup:** skips the file if `payment_id = <reference>` already exists in the table.
- **Downstream:** `FUJ_scheduled.komoju_rep()` -> `free.sh_komoju` is still a **manual** step (see scheduled_queries.md).
- **Status:** active. Prototype notebook `../komoju/komoju_process.ipynb` wrote to `Shopify.komoju_payments_temp` (legacy).

## 8-10. Stripe and Amazon Pay flows (`cloud_stripe_report/`)

Chain: `stripe-requests-reps` (weekly) asks Stripe for two reports -> Stripe calls `stripe-webhook`, which stores
`BAL_` / `PAY_` CSVs in `fujiorg-stripe-raw` -> `bucket-stripe-bq` loads them into `Shopify.stripe_api` /
`Shopify.stripe_payouts` (and hand-uploaded `AMA_` Amazon Pay files into `Shopify.amazon_pay`).
Shared env file `cloud_stripe_report/.env` only holds `KEY_TEST` (Stripe test key, value redacted).

### 8. Stripe report request

`stripe-requests-reps`: see [cloud_stripe_report/create_report_stripe/README.md](../../cloud_stripe_report/create_report_stripe/README.md).

### 9. Stripe webhook

`stripe-webhook`: see [cloud_stripe_report/webhook/README.md](../../cloud_stripe_report/webhook/README.md).

### 10. Stripe / Amazon Pay bucket loader

`bucket-stripe-bq`: see [cloud_stripe_report/bucket-stripe-bq/README.md](../../cloud_stripe_report/bucket-stripe-bq/README.md).

### Older Stripe code (legacy / test)
- `old_functions/stripe_charges_test/main.py` (was `stripe/main.py`) `main(data, context)`: pulls the last 100 Stripe charges to CSV in `test-bucket-function-cc`. Test.
- `old_functions/stripe-payout-report.py` and `old_functions/payout_wh.py`: earlier payout report requesters. Legacy.
- `util/amazon_pay_process.py` (git-ignored): earlier version of `amazon_pay_txt_process.py`. Legacy.

## 11. Rakuten orders loader (active)

`rakuten-update`: see [rakuten/upload_rakuten/README.md](../../rakuten/upload_rakuten/README.md).
Related local script (legacy, not a function): `rakuten/rakuten.py` reads local CSVs from the owner's Documents folder
and appends to `Rakuten.rakuten_orders` (a table that no longer exists); runs on import.

## 12. Rakuten payments and statements loader (active)

`payment-rakuten`: see [rakuten/payment_rakuten/README.md](../../rakuten/payment_rakuten/README.md) (upload procedure,
statement / cost path, archive). Manual alternative: `rakuten/update.ipynb` (git-ignored, imports the same functions).
See also rakuten.md sections 1, 4, 7.

## 13. Apps Script nomenclature loader (legacy)

- **Path:** `appscript/use_local_nomenclature.js`, function `use_local_nomenclature22()` (Sheets-bound, run by hand).
- **Logic:** loads the sheet tab "Nomenclature" into `temp_nomenclature.nomenclature_temp` (WRITE_TRUNCATE) and runs the old Amazon
  -> freee report query into a new sheet.
- **Status:** legacy. `Amazon.nomenclature` (98 rows, last modified 2026-08-17) is the single source of truth, read by
  `FUJ_scheduled.amazon_report()`; new Amazon fee types are added to it by hand. `temp_nomenclature.nomenclature_temp` is only read by
  the legacy SG routine `FUJ_scheduled.ASG_report()`.

## 14. Other Apps Scripts and local tools

| Path | What | Status |
|---|---|---|
| `appscript/update_report_sa.js`, `update_report_sh.js`, `update_report_ss.js` | `load_report_sa/ss`: load a reviewed report sheet (named by settlement id) into `apps_script.<sheet name>`. `update_report_sh.js` also defines `load_report_ss` (copy/paste duplicate). | legacy (2022-23) |
| `../appscripts/fifo.js` (sibling folder) | `fifoMonthlyBalances()`: FIFO inventory costing from sheets `overview`, `ecom_orders_bq`, `initial_values` -> `fifo_calc`. Fed by `FUJ_sheets.inventory_out_fifo()` (see dashboard.md). | active-looking (owner to confirm) |
| `../freee/free_script_2.js` (sibling folder; `free_script.js` is the older one) | `exportJournalEntries()`, menu "freee エクスポート": month-end inventory -> freee 仕訳 import CSV (期末 商品 / 期末商品棚卸高 / 期首). | active-looking |
| `inventory/fill_material_purchase.py` (+ `material_reference.py`, untracked, new 2026-10) | CLI: fills Cost / Unit / ASIN (Logiless) columns in a freee material-purchase export using a description -> JAN reference. | new, manual |
| `notebooks/backup_database.ipynb` | Exports `free.*` tables as dated CSVs to `gs://fuj_backup/Reports/`. | manual / prototype |
| `old_functions/webhook_hello/` (was `webhook/`), `old_functions/test/` (was `test/`) | Hello-world HTTP functions and early tests, not deployed. | retired |

---

## Open questions for the owner

Taken from the repository inventory (items 2-9), updated with what BigQuery shows on 2026-10-16 and with
`gcloud functions describe` / `gcloud scheduler jobs list` on 2026-11-18.

1. **Deploy configuration.** Closed (2026-11-18): every function has a `deploy.sh` reproducing the live configuration;
   `make check <function>` detects drift. See the [root README](../../README.md).
2. **`.env` files pointing at test targets.** Deployed side answered (2026-11-18): the live env writes
   `Shopify.orders_master`, `Shopify.shopify_payouts_api` and dataset `Amazon` (values in each `deploy.sh`). Open: are
   the local `shopify_api/.env` (`Shopify.orders_api_update` / `shopify_payouts_api_update`) and `amazon/.env`
   (`DATASET_ID` = `z_test_tables`) just test configs that can stay as they are?
3. **Shopify schedule.** Answered: `FUJ_shopify_orders` is published by scheduler `shopify_daily_orders`
   (`0 10 * * *` Asia/Tokyo) and `FUJ_shopify_payouts` by `shopify_weekly_payments` (`0 11 * * 1` Asia/Tokyo). The
   Monday re-pull uses server time (UTC); 10:00 JST is 01:00 UTC, so it happens on the Monday JST run. Open: is running
   payouts at Mon 11:00 JST, before or around `FUJ_Shopify` (Mon 11:05 JST), intended?
4. **Shopify refunds function.** Closed: not deployed (checked 2026-11-18); code moved to `old_functions/shopify_refunds/`.
5. **Stripe flow still scheduled?** Answered for Stripe: scheduler `stripe_weekly_report` (`0 8 * * 1` Asia/Tokyo)
   -> `stripe-requests-reps` -> `stripe-webhook` -> `bucket-stripe-bq`, all deployed. Open: is Amazon Pay still used
   (`Shopify.amazon_pay` last updated 2025-01-10, yet `FUJ_Shopify_Amazon_freee` runs three times a week)?
6. **Amazon JP vs SG deployment.** Closed: `upload-amazon` is triggered by `amazon-raw`, `amazon_sg` by bucket
   `amazon_sg`; both are deployed. Open: should the legacy `amazon_sg` function be deleted?
7. **Landing bucket names.** Closed: see "Known buckets" above.
8. **Which Rakuten loader is live?** Answered: both deployed functions are live, `rakuten-update` (orders, bucket
   `rakuten_orders_raw` -> `Rakuten.orders`) and `payment-rakuten` (payments and statements, bucket
   `rakuten_payment_raw`). `rakuten/update.ipynb` and `rakuten/rakuten.py` are local tools. The old SQL mentions
   `Rakuten.test_raku` / `rak_2020`, which are not present any more.
9. **Komoju in the dashboard.** `FUJ_scheduled.dashboard()` does not read `free.sh_komoju` (only `stripe_freee_full` rows whose
   supplier contains "komoju"). Are Komoju sales since the new pipeline (2025-12) missing from `free.dashboard_full`?
10. **`amazon_process` duplicate branch.** Closed: the deployed source has the same active "already on the table" branch
    (repo synced with the deployed source on 2026-11-18).
