# Fuji Organics: BigQuery to freee accounting pipeline

Documented as of 2026-10-16 from the live BigQuery definitions.

This folder documents how sales and settlement data of the Fuji Organics online stores (Amazon JP, Rakuten, Shopify) flows through BigQuery and ends up as rows of the freee 取引 (transaction) CSV import. It is written for the GCP project owner and for an AI agent that reads BigQuery through MCP, as input for building an automated BigQuery to freee pipeline. It is self-contained: nothing here assumes access to the code repository.

- GCP project: `test-bigquery-cc`. All datasets are in location `US`.
- Object names are written as `dataset.object`. Table functions are called with `()`, for example `SELECT * FROM FUJ_sheets.amazon_short()`.
- Times of scheduled queries are UTC (JST = UTC+9). All other dates are absolute calendar dates.
- Statements marked "to confirm with the owner" were not verifiable from BigQuery definitions alone.

## End-to-end picture

```
Sources (Seller Central / RMS / Shopify, Stripe, KOMOJU, Amazon Pay APIs and files)
  -> Cloud Functions (file upload or API pull)  -> landing tables  Amazon.*, Rakuten.*, Shopify.*
  -> FUJ_scheduled.<store>_report() table functions (accounting rules, one row per journal line)
  -> BigQuery scheduled queries (WRITE_APPEND, only new settlements) -> free.*_full draft tables
  -> FUJ_sheets.<store>_long()   (JAN split of bundles, occurrence_number, tax per line)
  -> FUJ_sheets.<store>_short()  (aggregated to the 17 freee columns, one row per settlement x month x account x item)
  -> Google Sheets workbooks per year (Connected Sheets tabs) -> reconciliation and monthly tabs (manual)
  -> CSV download of the monthly tab -> freee 取引 import (manual)
```

1. Files (Amazon, Rakuten) are uploaded by hand to Cloud Storage; a Cloud Function triggered by the upload loads them into the landing tables. Shopify orders and payouts are pulled from the Shopify API by scheduled Cloud Functions; KOMOJU settlements arrive through a webhook-triggered Cloud Function.
2. Report functions in dataset `FUJ_scheduled` turn landing data into accounting rows (収支区分, 勘定科目, 税区分, 品目, ...) in English-named columns.
3. Scheduled queries append only settlements that are not yet in the corresponding `free.*_full` table. Rows, once appended, are never updated (append-once).
4. `FUJ_sheets.*_long()` joins the product master (`FUJ_extra.JanTable()` over `free.sku_master`) and splits bundles into one row per physical product (JAN). `FUJ_sheets.*_short()` aggregates long rows into the freee import shape.
5. Yearly Google Sheets workbooks (`FUJ_2026_AMAZON`, `FUJ_2026_RAKUTEN`, `FUJ_2026_SHOPIFY`) pull the long and short functions through Connected Sheets. The owner reconciles settlement totals against the deposit amounts, copies the rows into a monthly tab, and downloads it as the freee CSV.
6. Rakuten needs extra manual work: fee rows (from `free.rakuten_cost_full` since 2026-11; before, calculated in the Google Sheet `RAK_PAYMENT`) are added, and 決済日 is filled from 管理番号.

## Stores at a glance

| Store / source | Landing tables | Report function | Scheduled query (UTC) | Draft table | Long / short | Typical data cadence |
|---|---|---|---|---|---|---|
| Amazon JP (Seller Central) | `Amazon.transaction_view_master`, `Amazon.order_central_master` | `FUJ_scheduled.amazon_report()` | `FUJ_Amazon_freee`, daily 00:12 | `free.amazon_report_full` | `FUJ_sheets.amazon_long()` / `amazon_short()` | Settlement files uploaded by hand when a settlement closes |
| Rakuten (RMS + 楽天ペイ) | `Rakuten.orders`, `Rakuten.payment_main` (`Rakuten.payment_coupon` is loaded as a reference table for reconciliation, not read by the report) | `FUJ_scheduled.rakuten_report()` | `FUJ_Rakuten_freee`, Saturday 00:00 | `free.rakuten_report_full` | `FUJ_sheets.rakuten_long()` / `rakuten_short()` | Deposits on the 10th and 25th of each month (observed `report_id` values) |
| Shopify Payments | `Shopify.orders_master`, `Shopify.shopify_payouts_api` | `FUJ_scheduled.shopify_report()` | `FUJ_Shopify`, Monday 02:05 | `free.shopify_freee_full` | `FUJ_sheets.shopify_long()` / `shopify_short()` (through `FUJ_extra.ShopifyCombinedReport()`) | Payouts several times per week |
| Shopify via Stripe | `Shopify.orders_master`, `Shopify.stripe_api`, `Shopify.stripe_payouts` | `FUJ_scheduled.stripe_rep()` | `FUJ_Shopify_Stripe`, daily 23:35 | `free.stripe_freee_full` | same as Shopify | Low volume in 2026 |
| Shopify via KOMOJU | `Shopify.orders_master`, `Shopify.komoju_payments` | `FUJ_scheduled.komoju_rep()` | none: run by hand (INSERT) | `free.sh_komoju` | same as Shopify | KOMOJU settlements |
| Shopify via Amazon Pay | `Shopify.orders_master`, `Shopify.amazon_pay` | `FUJ_scheduled.SA_report()` | `FUJ_Shopify_Amazon_freee`, Sun/Wed/Fri 23:15 | `free.sh_ama_freee_full` | same as Shopify | No new rows since 2025-01-07 (last 決済日) |

## Shared layer

| Object | Role | Details |
|---|---|---|
| `free.sku_master` | Product master: one row per store SKU, with JAN, bundle composition (`set`), `units`, short `item_name`, `series` | [jan_table_and_sku_master.md](jan_table_and_sku_master.md) |
| `FUJ_extra.JanTable()` | Flattens bundles into component JANs and looks up one name per JAN; joined by every `*_long()` function | [jan_table_and_sku_master.md](jan_table_and_sku_master.md) |
| `FUJ_extra.MISSING_SKU()`, `FUJ_extra.SKU_COURSES()` | Checks for SKUs sold but missing from `free.sku_master`; generator of Shopify course SKU rows | [jan_table_and_sku_master.md](jan_table_and_sku_master.md) |
| `Amazon.nomenclature` | Amazon only: maps `transaction_type` + `amount_type` + `amount_description` to 勘定科目 / 品目 / 税区分 / 備考 | [amazon.md](amazon.md) |
| Hard-coded rules | Shopify, Stripe, KOMOJU, Amazon Pay and Rakuten sales rules live inside the `FUJ_scheduled` report functions | per-store files |
| Google Sheet `RAK_PAYMENT` | Rakuten fee rows until 2026-11 (tabs `cost_calc`, `cost_nomenclature` with 36 mappings 集約科目名 -> 備考 / 品目 / 税区分 / 勘定科目); since 2026-11 in BigQuery: `Rakuten.statement`, `Rakuten.cost_nomenclature`, `free.rakuten_cost_full` | [rakuten.md](rakuten.md) |
| `FUJ_extra.Balance(amount)` | Scalar: `'支出'` if amount < 0, else `'収入'` | used by Amazon |
| `FUJ_extra.MainDateAma()`, `FUJ_extra.DateTypeAma()` | Scalars: choose the Amazon accrual date (posted date vs purchase date) | [amazon.md](amazon.md) |
| `FUJ_extra.zipCodeRak()` | Scalar: formats Rakuten postal code | not accounting relevant |

## Conventions

### The 17 freee columns

Every `*_short()` function and every monthly tab produces these columns, in this order:

| # | Column | Meaning | Values used in this pipeline |
|---|---|---|---|
| 1 | 収支区分 | Income or expense | `収入`, `支出` |
| 2 | 管理番号 | Control number grouping the rows of one deposit | per store, see below |
| 3 | 発生日 | Accrual date | first day of the month of the order (`YYYY-MM-01`) in the short output |
| 4 | 支払期日 | Due date | Amazon: deposit date; Rakuten and Shopify: empty |
| 5 | 取引先 | Business partner | `Amazon Seller`, `Rakuten`, `Shopify`, `Shopify Stripe`, `Shopify Komoju`, `Shopify Amazon` |
| 6 | 勘定科目 | Account | `売上高`, `支払手数料`, `広告宣伝費`, `荷造運賃`, `外注費`, `資本準備金` |
| 7 | 税区分 | Tax category | `課税売上8%（軽）`, `課税売上10%`, `課対仕入10%`, `対象外` |
| 8 | 金額 | Amount, tax included, always positive | integer yen |
| 9 | 税計算区分 | Tax calculation | always `内税` |
| 10 | 税額 | Tax amount | `ROUND(金額/13.5)` for 8%, `ROUND(金額/11)` for 10%, `0` for 対象外 |
| 11 | 備考 | Remark | `Product Charges`, `Product Charges Refund`, `Product Charges Discount`, `Delivery`, `Fee`, `Points issued`, Amazon nomenclature remarks, Rakuten fee remarks |
| 12 | 品目 | Item | `Sales <item_name> (<JAN>)` for product sales, otherwise `Sales`, `Delivery`, `Fee Shopify`, `Fee Amazon`, `Fee Rakuten`, `Advertisement`, `Professional`, `Others` |
| 13 | 部門 | Department | empty |
| 14 | メモタグ（複数指定可、カンマ区切り） | Memo tags | empty (BigQuery column name is `メモタグ`) |
| 15 | 決済日 | Settlement date (deposit date) | per store, see below |
| 16 | 決済口座 | Settlement account in freee | `Amazon Seller`, `Rakuten`, `Shopify` |
| 17 | 決済金額 | Settled amount | equals 金額 |

Full format rules: [freee_export.md](freee_export.md).

### 管理番号 per store

| 取引先 | 管理番号 | Example | 決済日 source |
|---|---|---|---|
| Amazon Seller | Amazon `settlement_id` | `12442705783` | `deposit_date` of the settlement (+9 h, JST) |
| Rakuten | 楽天ペイ deposit `report_id` = deposit date `YYYYMMDD` | `20260925` | Filled by hand: the date written in 管理番号 (`20260925` -> `2026-09-25`) |
| Shopify | Shopify Payments `payout_id` (integer) | `146373804344` | payout `processed_at` + 57 hours |
| Shopify Stripe | Stripe `automatic_payout_id`; BigQuery outputs the full 27-character id, which is trimmed to 20 characters in the Google Sheet at export (`MID(id, 5, 20)`, drops `po_1`) | `po_1OZNjQDl4oMd4Q8EngRQWgSd` -> `OZNjQDl4oMd4Q8EngRQW` | `automatic_payout_effective_at` |
| Shopify Komoju | KOMOJU settlement / clearing id | `DGC-27670-464471` | KOMOJU `transfer_date` |
| Shopify Amazon | Amazon Pay `settlementid` | | Amazon Pay `settlement_end_date` |

freee limits 管理番号 to 20 characters.

## Files in this folder

| File | Content |
|---|---|
| [amazon.md](amazon.md) | Amazon JP: report files, landing tables, `Amazon.nomenclature`, `amazon_report`, `amazon_long/short`, the Amazon workbook |
| [shopify.md](shopify.md) | Shopify with its four payment sources (Shopify Payments, Stripe, KOMOJU, Amazon Pay), `ShopifyCombinedReport`, `Shopify_totals`, bundle splits, the Shopify workbook |
| [rakuten.md](rakuten.md) | Rakuten orders and 楽天ペイ payments, `Rakuten_full`, `rakuten_report`, `rakuten_long/short`, fee logic (`Rakuten.statement`, `cost_nomenclature`, `rakuten_cost_report`, `free.rakuten_cost_full`; before 2026-11 sheet `RAK_PAYMENT`), 決済日 derivation |
| [jan_table_and_sku_master.md](jan_table_and_sku_master.md) | `free.sku_master`, `FUJ_extra.JanTable()`, bundle splitting, the one-JAN-one-name rule, check queries |
| [freee_export.md](freee_export.md) | freee 取引 CSV format, workbooks and Connected Sheets queries, reconciliation checks, what an automated pipeline must reproduce |
| [manual_steps.md](manual_steps.md) | Checklist of every manual step today, per store and shared |
| [pipeline_instructions.md](pipeline_instructions.md) | How to build the automated BigQuery -> freee pipeline: ground rules (scripts, dry runs, ledger, reconciliation), phases with acceptance criteria, what to ask the owner. |
| [lineage.md](lineage.md) | Generated dependency map: per-store chain from landing tables to `FUJ_sheets` functions, readers of every table and routine, scheduled queries |
| [generate_lineage.py](generate_lineage.py) | Read-only script that rebuilds `lineage.md` from the live BigQuery definitions (`python generate_lineage.py > lineage.md`, needs an authenticated `bq` CLI) |

## Out of scope and legacy

- A separate internal sales dashboard reads some of the same tables. It is not part of the freee export and is not documented here. Other scheduled queries in the project that are not listed above belong to unrelated work and can be ignored.
- Legacy / inactive objects (do not use):
  - Amazon Singapore (ASG): `Amazon.oc_sg`, `Amazon.tv_sg`, `Amazon.oc_sg_new`, `Amazon.tv_sg_new`, `Amazon.ex_sheet`, `Amazon.exchange_sg`, `free.AMA_SG`, `FUJ_scheduled.ASG_report`, `FUJ_sheets.ASG_long`, `FUJ_sheets.ASG_short`. Legacy / inactive.
  - `temp_nomenclature.*` (old Amazon nomenclature copy) and `Amazon.nomenclature_COPY`. Legacy / inactive.
  - `free.new_sku` (old product master, replaced by `free.sku_master`). Legacy / inactive.
  - `FUJ_extra.shopify_short_old`, `FUJ_extra.amazon_full`. Legacy / inactive.
  - Copies in `FUJ_draft`, `FUJ_other`, `FUJ_test` (draft or test variants of amazon/rakuten/shopify long, short and report functions). Legacy / inactive.
  - Duplicate routines in `z_functions` (Balance, DateTypeAma, MainDateAma, zipCodeRak, JanTable, SkuCoursesSH and others). Legacy / inactive.
  - `Shopify.amazon_pay_2`, `Shopify.komoju_payments_temp`, `Shopify.nomenclature_shopify`, `Shopify.refunds_master`, `Shopify.ss_api_order_number`, `Shopify.ss_pay_orders`, `Rakuten.free_draft`, `Rakuten.orders_new_cols`, `Amazon.orders_central_jp`, `Amazon.transaction_view_jp`. Legacy / inactive.
- No BigQuery caller and no scheduled query; status to confirm with the owner: `FUJ_extra.SHP_REP()` (older variant of the first step of `shopify_long`; lineage.md lists it as "active, run ad hoc") and `FUJ_scheduled.samples_shopify()` (sample and influencer orders tagged `sample` / `サンプル` / `influencer` / `インフルエン`; also "active, run ad hoc"). `FUJ_extra.MainShopifySub()`, another older variant, is treated as legacy.
