# Cloud Functions, loaders and Apps Scripts

Documented as of 2026-10-16.

Paths are relative to the `cloud_functions` repo root. The Komoju project is a sibling folder and is written
as `../komoju/...`. GCP project everywhere: `test-bigquery-cc`.

There is no deploy script, Makefile or YAML for anything under `cloud_functions/` (only `../komoju/scripts/deploy.sh`
exists). Function names, regions, runtimes and triggers below are therefore inferred from the code
(decorators, signatures, comments) unless marked otherwise. Env var **names** are listed; values are never
written here (several are secrets, see [security_notes.md](security_notes.md)).

## Known buckets

| Bucket | Role |
|---|---|
| `fujiorg-sales-data` | Processed archive. `Amazon/order_central/sales_<year>/`, `Amazon/transaction_view/settlement_<year>/`, `Amazon/repeated_files/`, `Shopify/amazon_pay/<year>/`, `Shopify/amazon_pay/RAW/`, `Shopify/stripe/payments/`, `Shopify/stripe/balance/`. |
| `test-bucket-function-cc` | Shopify function scratch / backups (`SH_problem_data_*`, `SHOPIFY_ORDERS_*`, `SHOPIFY_PAYOUTS_*`), old Stripe test output. |
| `komoju_stage` | Komoju webhook output (`payloads/`, `SET_<ref>.csv`) and trigger bucket of the Komoju BigQuery loader. |
| `fuj_backup` | Target of the backup notebook (`notebooks/backup_database.ipynb`, CSV exports of `free.*` under `Reports/`). |
| Amazon landing bucket | Trigger bucket of `amazon_process`. **Name not in code** (open question). |
| Rakuten landing bucket(s) | Trigger bucket(s) of the two Rakuten loaders. **Names not in code** (open question). |
| `STRIPE_BUCKET`, `STRIPE_PAYOUT_BUCKET` | Stripe report buckets, env vars only. **Names not in code** (open question). |

## Summary

| # | Flow | Entry point | Trigger | Writes | Status |
|---|---|---|---|---|---|
| 1 | Amazon JP loader | `amazon_process` | GCS finalize (inferred) | `Amazon.order_central_master`, `Amazon.transaction_view_master` | active |
| 2 | Amazon SG loader | `amazon_sg_process` | GCS finalize (inferred) | SG tables | legacy |
| 3 | Shopify orders + fulfilments | `main` | scheduler (inferred, type unknown) | `Shopify.orders_master`, `Shopify.fulfillments` | active |
| 4 | Shopify payouts | `update_shopify_payout` | scheduler (inferred) | `Shopify.shopify_payouts_api` | active (table updated 2026-10-12) |
| 5 | Shopify refunds | `get_refunds` | n/a | refunds table | legacy (deprecated.md) |
| 6 | Komoju webhook | `webhook` | HTTP (KOMOJU `settlement.created`) | `gs://komoju_stage` | active |
| 7 | Komoju BigQuery loader | `load_csv` | GCS finalize on `komoju_stage` | `Shopify.komoju_payments` | active |
| 8 | Stripe report request | `request_reports` | Pub/Sub / scheduler (inferred) | Stripe Reporting API run | unclear, data still arriving |
| 9 | Stripe webhook | `webhook` | HTTP (Stripe webhook) | `STRIPE_BUCKET` | unclear, data still arriving |
| 10 | Stripe / Amazon Pay bucket loader | `upload_stripe_bq` | GCS finalize (inferred) | `Shopify.stripe_api`, `Shopify.stripe_payouts`, Amazon Pay table | unclear (Stripe tables updated 2026-10-11; Amazon Pay last 2025-01-10) |
| 11 | Rakuten orders loader | `upload_rakuten_bq` | GCS finalize (inferred) | `Rakuten.orders` (expected) | active (which path is live: open question) |
| 12 | Rakuten payments loader | `update_payment` | GCS finalize (inferred) | `Rakuten.payment_main`, `Rakuten.payment_coupon` | active (also loaded by notebook) |
| 13 | Apps Script nomenclature loader | `use_local_nomenclature22` | manual, in Sheets | `temp_nomenclature.nomenclature_temp` | legacy |
| 14 | Other Apps Scripts / local tools | various | manual | various | mixed |

---

## 1. Amazon JP loader (active)

- **Path:** `amazon/amazon_process.py` (+ `amazon/dict_utils_ama.py` column dtypes, `amazon/.env`, notebook `amazon/amazon_update.ipynb`).
- **Entry point:** `amazon_process(cloud_event)`, `@functions_framework.cloud_event`.
- **Trigger:** GCS object finalize on the Amazon landing bucket (inferred, not in code; bucket name unknown).
- **Env vars:** `TABLE_1` (order central table), `TABLE_2` (transaction view table), `PROJECT_ID`, `DATASET_ID`, `NEW_BUCKET` (archive bucket).
  The local `amazon/.env` has `DATASET_ID` = `z_test_tables` (test copy); production must point at `Amazon` (open question 3).
- **External API:** none. Reports are downloaded by hand from Seller Central.
- **Destination tables:** `Amazon.order_central_master` ("oc", Order Central / order report) or `Amazon.transaction_view_master`
  ("tv", settlement / transaction view report). Adds `FILE_NAME` and `UPLOADED_DATETIME`. WRITE_APPEND load job with explicit schema.
- **Bucket in:** landing bucket (unknown). **Bucket out:** `gs://fujiorg-sales-data/`:
  - tv: `Amazon/transaction_view/settlement_<year>/TV_<name>`
  - oc: `Amazon/order_central/sales_<year>/OC_<name>`
  - already-loaded name: `Amazon/repeated_files/<TV|OC>_<name>`
  The source object is copied then deleted (move).
- **File naming convention:**
  - Order report: `YYYYMM_W<n>.txt`, e.g. `202605_W3.txt` (month + week number).
  - Settlement / payout (transaction view): `YYYYMMDD.txt`, e.g. `20240627.txt`.
  - The archive year folder is `name[:4]`, so every file name must start with the 4-digit year.
  - Report type is detected by encoding, not by name: the file is first read with the default (UTF-8) reader and treated
    as **tv**; if that fails it is re-read as `ms932` and treated as **oc**. (An order report that happens to be valid UTF-8
    would be misclassified and then fail on the tv dtypes.)
- **Dedup:** by exact file name. `SELECT DISTINCT FILE_NAME` from the destination table; if the name is present the file is
  not loaded and goes to `repeated_files/`. No replace logic (`## TODO` in that branch). A renamed re-export is loaded again,
  and if the lookup query fails the code proceeds with an empty list and loads the file. See README TODO 3.
  Downstream, `FUJ_extra.AMA_SUB()` only processes settlements whose `settlement_id` is not yet in `free.amazon_report_full`.
- **Status:** active. `Amazon.order_central_master` and `Amazon.transaction_view_master` last modified 2026-10-13.
  Last code change: commit `b7dff95` "latest deployed" (2025-01-21).

## 2. Amazon SG legacy loader (legacy)

- **Path:** `amazon/amazon_sg_process.py` + `amazon/config_asg.py` (year hard-coded `2024`).
- **Entry point:** `amazon_sg_process(cloud_event)`. **Trigger:** GCS finalize (inferred, not in code).
- **Env vars:** same names as the JP loader: `TABLE_1`, `TABLE_2`, `PROJECT_ID`, `DATASET_ID`, `NEW_BUCKET`.
- **Logic:** same as JP, but report type is detected from `tv` / `oc` in the file name and header lines are skipped.
- **Destination:** Amazon Singapore tables (`Amazon.tv_sg`, `Amazon.oc_sg`, last modified 2025-05-08).
- **Status:** legacy, Amazon Singapore is inactive. Whether it is still deployed is an open question (7).

## 3. Shopify orders + fulfilments (active)

- **Function:** `FUJ_shopify_orders` (Gen 2, python312, us-central1). Deploy: `bash shopify_api/orders/deploy.sh`
  (reproduces the live configuration; first deployed from the console).
- **Path:** `shopify_api/orders/`: `main.py` (entry), `orders_process.py`, `orders_parameters.py`, `utils_bq.py`, `utils_df.py`,
  `fulfillments.py`, `requirements.txt`. The local `.env` stays in `shopify_api/`.
- **Entry point:** `main(data=None, context=None)`. Background-style signature but returns a Flask `jsonify(...)`, 200.
- **Trigger:** Pub/Sub topic `shopify_orders` (no retry). Timeout 300 s, 512 MiB.
  `Shopify.orders_master` and `Shopify.fulfillments` were modified 2026-10-16, so it runs at least daily.
- **Env vars:** `SHOPIFY_KEY`, `SHOPIFY_PASS` (Secret Manager `api_key_shop`, `shopy_password`), `API_VERSION`, `BUCKET`,
  `PROJECT_NAME`, `TABLE_NAME`, `FULFILLMENT_TABLE`, `STORE_NAME`.
  The local `shopify_api/.env` sets `TABLE_NAME` to `Shopify.orders_api_update`, a table that does not exist in BigQuery today;
  production evidently writes `Shopify.orders_master` (open question 3).
- **External API:** Shopify Admin REST (`orders.json` paged by `since_id`, `orders/{id}/transactions.json` for Stripe / Komoju
  gateway orders, `orders/{id}/fulfillments.json`). Credentials are embedded in the URL (see security_notes.md).
- **Destination tables:** `Shopify.orders_master` (WRITE_APPEND), `Shopify.fulfillments` (delete + append for the new orders).
- **Bucket out:** `BUCKET` (= `fujiorg-shopify-orders-raw`): `SH_problem_data_<date>_<id>_RAW.csv` when the BigQuery upload fails;
  on Mondays `SHOPIFY_ORDERS_<date>_<id>_RAW.csv` backup of the deleted 2-week window.
- **Dedup / refresh:** incremental from the max loaded order (`since_id`). Every Monday (server time, UTC) it first deletes the last
  ~14 days of orders and re-pulls them so pending / unpaid orders get their final status. Recent change (commit `9165893`):
  only `sale` transactions are joined to avoid duplicated reference ids.
- **Status:** active. The deployed source (2026-06-02) predates the repo version: `fulfillments.py` and the transactions
  fetch in `orders_process.py` gained a retrying HTTP session and fetch-then-normalise (one failing order is skipped), not
  deployed yet.

## 4. Shopify payouts (active)

- **Function:** `FUJ_shopify_payouts` (Gen 2, python312, us-central1). Deploy: `bash shopify_api/payouts/deploy.sh`.
- **Path:** `shopify_api/payouts/`: `main.py`, `requirements.txt` (own requirements, includes `ShopifyAPI`); identical to the
  deployed source.
- **Entry point:** `update_shopify_payout(data=None, context=None)`; returns a JSON string + status.
- **Trigger:** Pub/Sub topic `shopify_payouts_weekly` (no retry). Timeout 60 s, 1 GiB.
- **Env vars:** `SHOPIFY_KEY`, `SHOPIFY_PASS` (Secret Manager `SH_PAY_API_KEY`, `SH_PAY_PASS`, version 1), `API_VERSION`, `BUCKET`, `PROJECT_NAME`, `TABLE_NAME_PAY`, `STORE_NAME`.
  Local `.env` sets `TABLE_NAME_PAY` = `shopify_payouts_api_update` (does not exist in BigQuery); production writes `Shopify.shopify_payouts_api`.
- **External API:** Shopify Admin `shopify_payments/balance/transactions.json` (since_id paging, limit 250). Uses the `shopify`
  package (ShopifyAPI 12.5.0, in `shopify_api/payouts/requirements.txt`).
- **Destination table:** `Shopify.shopify_payouts_api` (last modified 2026-10-12; table recreated 2025-10-06).
- **Bucket out:** `SHOPIFY_PAYOUTS_<date>_<id>_RAW.csv` in `BUCKET`.
- **Dedup / refresh:** deletes from the oldest `pending` / `in_transit` row onward, then appends everything newer, so unsettled
  rows are re-pulled each run.
- **Status:** active.

## 5. Shopify refunds (legacy)

- **Path:** `shopify_api/refunds/refunds.py` (+ `refunds_utils.py`, `deprecated.md`). Entry `get_refunds(data, context)`.
- **Env vars:** `API_KEY`, `API_PASS`, `SHOP_NAME`, `API_VER`, `BUCKET_NAME`, `REFUND_TABLE`, `ORDERS_TABLE`, `PROJECT_ID`.
- **Status:** deprecated; refunds are handled inside the orders flow (`refunds(df)` in `utils_df.py`). Old output: `Shopify.refunds_master` (2023).

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

Shared env file `cloud_stripe_report/.env` only holds `KEY_TEST` (Stripe test key, value redacted). The deployed functions use the
env var names below; their values are not in the repo.

### 8. Stripe report request
- **Path:** `cloud_stripe_report/create_report_stripe/request_report.py` (+ `payouts.py`).
- **Entry point:** `request_reports(data, context)` (background function).
- **Trigger:** Pub/Sub / Cloud Scheduler (inferred, not in code; code comment says weekly, "every monday").
- **Env vars:** `stripe_key` (secret), `TABLE_QUERY`.
- **External API:** Stripe Reporting API. Reads `MAX(created_utc)` from `TABLE_QUERY`, requests `balance_change_from_activity.itemized.2`
  from the last row + 1 s to 2 days ago; `payouts.payout_request()` requests `payout_reconciliation.itemized.5`.

### 9. Stripe webhook
- **Path:** `cloud_stripe_report/webhook/main.py` (+ `process_report.py`; `payout_wh.py` is an older duplicate of the payout request code).
- **Entry point:** `webhook(request)` (mixes a Flask `@app.route` with the functions entry point).
- **Trigger:** HTTP, Stripe webhook `reporting.report_run.succeeded` (signature verified).
- **Env vars:** `stripe_key`, `web_secret` (secrets), `STRIPE_BUCKET`, `STRIPE_PAYOUT_BUCKET`.
- **Bucket out:** `PAY_<file>` (payout reconciliation) and `BAL_<file>` (balance change) CSVs in `STRIPE_BUCKET` (name unknown).

### 10. Stripe / Amazon Pay bucket loader
- **Path:** `cloud_stripe_report/bucket-stripe-bq/load_csv_bq.py` (+ `amazon_pay_txt_process.py`).
- **Entry point:** `upload_stripe_bq(cloud_event)`. **Trigger:** GCS finalize (inferred, not in code).
- **Env vars:** `PROJECT_ID`, `DATASET_ID`, `TABLE_ID`, `TABLE_ID_PAY`, `TABLE_ID_AMA`.
- **Routing by file prefix (`name[:3]`):**
  - `BAL...` -> `TABLE_ID` (= `Shopify.stripe_api`), WRITE_APPEND with explicit schema.
  - `PAY...` -> `TABLE_ID_PAY` (= `Shopify.stripe_payouts`), WRITE_APPEND.
  - `AMA...` -> Amazon Pay settlement txt, cleaned and appended to `TABLE_ID_AMA`; cleaned CSV written to
    `gs://fujiorg-sales-data/Shopify/amazon_pay/<year>/` (year = chars 5-8 of the name, i.e. `AMA_YYYY...`), raw moved to `Shopify/amazon_pay/RAW/`.
    `FUJ_scheduled.SA_report()` reads `Shopify.amazon_pay` (created 2024-06-18); `Shopify.amazon_pay_2` stopped 2024-06-12, so
    `TABLE_ID_AMA` presumably points at `amazon_pay` since then.
- **Dedup:** none in the loader. The Stripe request is incremental (from `MAX(created_utc)`), so a re-delivered report file would
  append duplicates. Downstream `stripe_rep` excludes settlements already in `free.stripe_freee_full`.
- **Status:** unclear in code, but `Shopify.stripe_api` / `Shopify.stripe_payouts` were last modified 2026-10-11, so the chain is
  still running. `Shopify.amazon_pay` last modified 2025-01-10 (no Amazon Pay files since; low volume).

### Older Stripe code (legacy / test)
- `stripe/main.py` `main(data, context)`: pulls the last 100 Stripe charges to CSV in `test-bucket-function-cc`. Env `stripe_key`. Test.
- `old_functions/stripe-payout-report.py`: earlier payout report requester. Legacy.
- `util/amazon_pay_process.py`: earlier version of `amazon_pay_txt_process.py`. Legacy.

## 11. Rakuten orders loader (active)

- **Path:** `rakuten/upload_rakuten/main.py` (+ `df_process.py`, `bq_process.py`, `columns_names.py`, prototype `orders_process.ipynb`).
- **Entry point:** `upload_rakuten_bq(cloud_event)`. **Trigger:** GCS finalize on a Rakuten bucket (inferred, not in code; name unknown).
- **Env vars:** `PROJECT_ID`, `DATASET_ID`, `TABLE_ID` (values not in repo; expected `Rakuten.orders`).
- **External API:** none (manual RMS order CSV export, cp932, Japanese headers mapped by `columns_names.py`).
- **Destination table:** `Rakuten.orders` (expected; last modified 2026-10-06, recreated 2025-04-11). Columns not in the table are dropped. WRITE_APPEND.
- **File naming:** no convention enforced by the code.
- **Dedup:** none. Uploading an overlapping export appends duplicate order rows.
- **Status:** active, but which Rakuten path is the production one is an open question (9).
- **Related local script (legacy):** `rakuten/rakuten.py` reads local CSVs from the owner's Documents folder and appends to
  `Rakuten.rakuten_orders` (a table that no longer exists); runs on import.

## 12. Rakuten payments and statements loader (active)

- **Path:** `rakuten/payment_rakuten/main.py` (moved on 2026-11-05 from `rakuten/payment_rak.py`, history kept), with `requirements.txt` (pinned versions), `deploy.sh`, `.gcloudignore` and `README.md` (upload procedure); manual alternative `rakuten/update.ipynb` (git-ignored, imports the same functions).
- **Entry point:** `update_payment(cloud_event)`. **Trigger:** GCS finalize on bucket `rakuten_payment_raw` (project `test-bigquery-cc`, us-central1, Gen 2, python310, 512Mi, 60s). Routes on the object name: `*_Shop_Billpay.csv` is a 楽天ペイ Billpay statement, anything else a payment CSV.
- **Env vars:** `DATASET_ID=Rakuten`, `TABLE_ID_1=payment_main`, `TABLE_ID_2=payment_coupon`, `TABLE_ID_3=statement`, `COST_DATASET_ID=free`, `COST_TABLE_ID=rakuten_cost_full` (all set by `deploy.sh`).
- **Payment path (unchanged):** Rakuten settlement (楽天ペイ) payment CSV, header on row 4, sections split on 連番 / 合計 rows; the last column of each section is stored as `report_id`. Destination `Rakuten.payment_main`, `Rakuten.payment_coupon` (pandas-gbq `if_exists="append"`), no dedup in the loader; `FUJ_scheduled.rakuten_report()` excludes control numbers already in `free.rakuten_report_full`.
- **Statement path (2026-11):** CP932, header on row 1, 17 columns checked against the expected header; `-` becomes NULL; `report_id` = 精算対象期間終了日 of the single `楽天ﾍﾟｲ_決済金*` line. Raises (nothing written) on a header mismatch, a missing, duplicated or undated deposit line, or a 精算書No already in `Rakuten.statement`. Appends to `Rakuten.statement`, then runs the append query `queries_bq/rakuten_cost/05_materialise_cost_rows.sql` (cost rows of every deposit not yet in `free.rakuten_cost_full`, built by `FUJ_scheduled.rakuten_cost_report()` with `Rakuten.cost_nomenclature`). See rakuten.md sections 1, 4, 7.
- **Deploy:** `bash rakuten/payment_rakuten/deploy.sh`, then `gcloud functions describe payment-rakuten --region us-central1`.
- **Status:** active.

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
| `webhook/main.py` + `how_to.txt`, `test/` | Hello-world HTTP functions and early tests. | test |

---

## Open questions for the owner

Taken from the repository inventory (items 2-9), updated with what BigQuery shows on 2026-10-16.

1. **Deploy configuration.** Nothing under `cloud_functions/` records the deployed function names, regions, runtimes or trigger
   types. Can these be exported (`gcloud functions list`) and recorded, or a deploy script added like `../komoju/scripts/deploy.sh`?
2. **`.env` files pointing at test targets.** `shopify_api/.env` writes to `Shopify.orders_api_update` / `shopify_payouts_api_update`
   (neither exists in BigQuery), and `amazon/.env` has `DATASET_ID` = `z_test_tables` (which holds small 2024-07-01 copies of
   `order_central_master` / `transaction_view_master`). Confirm the deployed env uses `Shopify.orders_master`,
   `Shopify.shopify_payouts_api` and dataset `Amazon`, and whether the local `.env` files are just test configs.
3. **Shopify schedule.** Answered: two functions (`FUJ_shopify_orders`, `FUJ_shopify_payouts`), each with its own folder and
   `deploy.sh`, triggered via Pub/Sub topics `shopify_orders` / `shopify_payouts_weekly`. Open: at what times are those topics published? The Monday re-pull uses server time (UTC), so in JST it
   happens Monday 09:00 or later; is that intended relative to `FUJ_Shopify` (Mon 11:05 JST)?
4. **Shopify refunds function.** `shopify_api/refunds` is marked deprecated. Confirm nothing is still deployed or scheduled.
5. **Stripe flow still scheduled?** `cloud_stripe_report` code dates from 2023-24, but `Shopify.stripe_api` / `stripe_payouts`
   were updated 2026-10-11, so something still runs. Which functions / schedule? And is Amazon Pay still used
   (`Shopify.amazon_pay` last updated 2025-01-10, yet `FUJ_Shopify_Amazon_freee` runs three times a week)?
6. **Amazon JP vs SG deployment.** Which bucket triggers `amazon_process` and which `amazon_sg_process`? Is the SG function still
   deployed? (Both read the same env var names.)
7. **Landing bucket names.** Not in code for: Amazon uploads, Rakuten orders, Rakuten payments, Stripe (`STRIPE_BUCKET`,
   `STRIPE_PAYOUT_BUCKET`). Known: `fujiorg-sales-data`, `test-bucket-function-cc`, `komoju_stage`, `fuj_backup`.
8. **Which Rakuten loader is live?** Candidates: `upload_rakuten_bq` (orders, GCS), `update_payment` (payments, GCS),
   `rakuten/update.ipynb` (payments from local files), `rakuten/rakuten.py` (local orders backfill to the non-existent
   `Rakuten.rakuten_orders`). BigQuery has `Rakuten.orders`, `payment_main`, `payment_coupon`; the old SQL also mentions
   `Rakuten.test_raku` / `rak_2020`, which are not present any more.
9. **Komoju in the dashboard.** `FUJ_scheduled.dashboard()` does not read `free.sh_komoju` (only `stripe_freee_full` rows whose
   supplier contains "komoju"). Are Komoju sales since the new pipeline (2025-12) missing from `free.dashboard_full`?
10. **`amazon_process` duplicate branch.** The "already on the table" branch is active in the repo (see README TODO 3). Is the
    deployed source the same, or was it commented out in the deployed version?
