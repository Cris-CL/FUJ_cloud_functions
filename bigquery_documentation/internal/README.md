# Internal operations notes (Fuji Organics data pipeline)

Documented as of 2026-10-16.

This folder is the owner's private operations notebook for the Fuji Organics sales pipeline
(Amazon JP, Rakuten, Shopify incl. Stripe / Komoju / Amazon Pay) in GCP project `test-bigquery-cc`
(BigQuery location US).

- It is **not shared**. The documentation shared with the coworker (GCP project owner) and his agent is the
  parent folder `bigquery_documentation/`, which must stay self-contained and must never link here.
- Unlike the shared docs, these pages may reference repository paths (relative to the `cloud_functions`
  repo root, e.g. `amazon/amazon_process.py`; the sibling Komoju project is written as `../komoju/...`),
  bucket names, dataset names and the dashboard chain.
- No secret values appear anywhere in this folder. Variables and files are named, values are redacted.

## Index

| File | Content |
|---|---|
| [cloud_functions.md](cloud_functions.md) | Every Cloud Function / loader / Apps Script: path, entry point, trigger, env var names, buckets, file naming, dedup, status. Ends with open questions. |
| [scheduled_queries.md](scheduled_queries.md) | The 7 Fuji BigQuery scheduled queries + the manual `komoju_rep` step; how to troubleshoot a failed or duplicated run. |
| [dashboard.md](dashboard.md) | Dashboard chain: `FUJ_scheduled.dashboard()` -> `free.dashboard_full`, fulfilment dates, costs, FIFO inventory feed, and the legacy `Datastudio.combined_results` chain. |
| [legacy.md](legacy.md) | Every legacy / unused / test dataset, table and routine, with a one-phrase reason. List only, no cleanup actions. |
| [security_notes.md](security_notes.md) | Files holding plaintext credentials (names only), credential-handling issues, recommendations. |

## Operational calendar

### Automatic (BigQuery scheduled queries, location US)

Times are as configured in BigQuery Data Transfer (UTC). JST = UTC + 9 h.

| Config | UTC | JST | Writes |
|---|---|---|---|
| `daily_dashboard_update` (legacy dashboard) | daily 00:01 | daily 09:01 | `Datastudio.combined_results` (TRUNCATE) |
| `FUJ_Amazon_freee` | daily 00:12 | daily 09:12 | `free.amazon_report_full` (APPEND) |
| `FUJ_Dashboard` | daily 00:20 | daily 09:20 | `free.dashboard_full` (TRUNCATE) |
| `FUJ_Shopify_Stripe` | daily 23:35 | daily 08:35 (next day) | `free.stripe_freee_full` (APPEND) |
| `FUJ_Shopify_Amazon_freee` | Sun, Wed, Fri 23:15 | Mon, Thu, Sat 08:15 | `free.sh_ama_freee_full` (APPEND) |
| `FUJ_Shopify` | Mon 02:05 | Mon 11:05 | `free.shopify_freee_full` (APPEND) |
| `FUJ_Rakuten_freee` | Sat 00:00 | Sat 09:00 | `free.rakuten_report_full` (APPEND) |

Details and failure checks: [scheduled_queries.md](scheduled_queries.md).

### Automatic (Cloud Functions, event driven)

- Shopify orders + fulfilments function: runs on a schedule (trigger type and frequency not in code;
  `Shopify.orders_master` and `Shopify.fulfillments` were both modified on 2026-10-16, so at least daily).
  On Mondays (server time, i.e. UTC) it deletes and re-pulls the last 2 weeks of orders.
- Shopify payouts function (`update_shopify_payout`): schedule not in code (`Shopify.shopify_payouts_api`
  last modified 2026-10-12).
- Stripe report chain (`cloud_stripe_report/`): `Shopify.stripe_api` / `Shopify.stripe_payouts` last modified
  2026-10-11, so something still loads them (weekly, per the code comment "every monday"; to confirm).
- Komoju: KOMOJU webhook -> `komoju_stage` bucket -> `Shopify.komoju_payments` (fully automatic since 2026-08).
- Amazon / Rakuten loaders: fire when the owner drops a file in their landing bucket (see below).

### Weekly / monthly, by hand

| When | Task | Notes |
|---|---|---|
| Weekly (per order-report week) | Download the Amazon Order Central (order) report from Seller Central, name it `YYYYMM_W<n>.txt` (e.g. `202605_W3.txt`), upload to the Amazon landing bucket. | `amazon/amazon_process.py` loads it into `Amazon.order_central_master` and moves it to `gs://fujiorg-sales-data/Amazon/order_central/sales_<year>/OC_<name>`. |
| Each Amazon settlement | Download the settlement (transaction view) report, name it `YYYYMMDD.txt` (e.g. `20240627.txt`), upload to the landing bucket. | Loaded into `Amazon.transaction_view_master`; archived to `.../Amazon/transaction_view/settlement_<year>/TV_<name>`. Next day 09:12 JST `FUJ_Amazon_freee` appends the settlement to `free.amazon_report_full`. |
| After each Amazon settlement | Check for `MISSING NOMENCLATURE ...` in the account column (sheet AMA_L / `free.amazon_report_full`). Add the new fee type to `Amazon.nomenclature` by hand. | The settlement is skipped once it is in `free.amazon_report_full` (`settlement_id NOT IN` filter in `FUJ_extra.AMA_SUB`), so a settlement appended with missing nomenclature has to be removed from `free.amazon_report_full` before it is rebuilt. |
| Monthly | Rakuten RMS order CSV export -> upload (orders loader) -> `Rakuten.orders`. | Which loader path is live is an open question (see cloud_functions.md). |
| Monthly / per billpay | Rakuten billpay (楽天ペイ精算書) payment CSV -> `Rakuten.payment_main` + `Rakuten.payment_coupon` (cloud function `update_payment` or `rakuten/update.ipynb` from local files). | Then Saturday 09:00 JST `FUJ_Rakuten_freee` appends to `free.rakuten_report_full`. |
| Monthly | Rakuten fees: upload the billpay CSV (`*_Shop_Billpay.csv`) to bucket `rakuten_payment_raw`; the function appends the fee rows to `free.rakuten_cost_full`; add any new 品目 (`MISSING NOMENCLATURE`) to `Rakuten.cost_nomenclature`; paste the deposit's rows into the FUJ_2026_RAKUTEN month tab; fill 決済日 from 管理番号 (20260925 -> 2026-09-25). | Since 2026-11 (before: Google Sheet RAK_PAYMENT `cost_calc`). |
| After new Komoju settlements | Run `FUJ_scheduled.komoju_rep()` by hand and INSERT the new settlements into `free.sh_komoju`. | Not scheduled (TODO below). See scheduled_queries.md for the exclusion filter to use. |
| Monthly | Reconcile in FUJ_2026_AMAZON / FUJ_2026_RAKUTEN / FUJ_2026_SHOPIFY (connected sheets on `FUJ_sheets.*_long/_short`, `Shopify_totals`), difference columns must be 0. | Stripe 管理番号 is trimmed to 20 chars (MID(id, 5, 20), drops `po_1`). |
| Monthly | Export the month tabs (17-column freee 取引 CSV) and import into freee. | Amazon and Shopify tabs as-is; Rakuten after adding the fee rows. |
| When new products / listings appear | Add rows to `free.sku_master` (store SKU -> JAN, item_name, series, set, units). Helpers: `FUJ_extra.MISSING_SKU()`, `FUJ_extra.SKU_COURSES()`. | Never reuse a JAN on a different set row: on 2026-10-13 a reused JAN fanned out `FUJ_extra.JanTable()` and duplicated rows in all `FUJ_sheets.*_long` reports. |
| When unit costs change | Update `free.cost_sku` (product_code, unit_cost, product_name). | Feeds `total_cost` in the dashboard. Last modified 2025-05-21. |

## Known TODOs

1. **Schedule `komoju_rep`.** `FUJ_scheduled.komoju_rep()` -> `free.sh_komoju` is run by hand. It has no internal
   "already loaded" filter (unlike `stripe_rep` / `SA_report`), so the scheduled version needs a
   `SETT_NUMBER NOT IN (SELECT ... FROM free.sh_komoju)` filter like the other APPEND configs.
2. **Move the Rakuten fee logic to BigQuery.** Done on 2026-11-05: the billpay CSV lands in `Rakuten.statement`
   (Cloud Function `payment-rakuten`, routed on `_Shop_Billpay.csv`), `Rakuten.cost_nomenclature` replaces the sheet
   tab, `FUJ_scheduled.rakuten_cost_report()` builds the fee rows and the function appends them to
   `free.rakuten_cost_full` (a separate table: the sales dedup by control_number would skip them). Still by hand:
   copying the deposit's rows into the monthly tab and 決済日. Amounts are whole yen, taxes allocated to the
   statement's 消費税 line (rakuten.md section 4).
3. **Idempotency for re-uploaded Amazon files.** Note: the owner's working assumption is that the
   "already on the table" branch in `amazon/amazon_process.py` is commented out. In the repository version
   (last change `b7dff95` "latest deployed", 2025-01-21; unchanged at HEAD) the branch is **active**: a file whose
   exact name is already in `FILE_NAME` is not loaded and is moved to `gs://fujiorg-sales-data/Amazon/repeated_files/`.
   What is missing is the `## TODO` inside that branch (delete the old rows and load the new file). Gaps:
   a re-export under a different file name is loaded again (duplicates); if the `FILE_NAME` lookup query fails
   the code falls back to an empty list and loads the file anyway; and the deployed source may differ from
   the repo (verify in the Cloud Functions console).
