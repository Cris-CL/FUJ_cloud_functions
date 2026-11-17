# Manual steps checklist

Documented as of 2026-10-16 from the live BigQuery definitions.

Every manual step of the BigQuery to freee flow as it runs today, in order. "Owner" means the person who runs the accounting export. Scheduled times are UTC (JST = UTC+9). How often the export itself is done (per deposit or at month close) is to confirm with the owner; the workbooks have one tab per month.

Automatic steps (not manual, listed for orientation):

- Scheduled queries:

  | Scheduled query | When (UTC) | Writes to |
  |---|---|---|
  | `FUJ_Amazon_freee` | daily 00:12 | `free.amazon_report_full` |
  | `FUJ_Rakuten_freee` | Saturday 00:00 | `free.rakuten_report_full` |
  | `FUJ_Shopify` | Monday 02:05 | `free.shopify_freee_full` |
  | `FUJ_Shopify_Stripe` | daily 23:35 | `free.stripe_freee_full` |
  | `FUJ_Shopify_Amazon_freee` | Sun/Wed/Fri 23:15 | `free.sh_ama_freee_full` |

- Cloud Functions that pull Shopify orders and payouts, Stripe reports and KOMOJU settlements.

## Shared (before any store export)

| # | Step | Trigger | Who | Tool | Check |
|---|---|---|---|---|---|
| S1 | Add new SKUs and bundles to `free.sku_master` | New product or bundle launched; rows returned by `FUJ_extra.MISSING_SKU()`; new course SKUs from `FUJ_extra.SKU_COURSES()` | Owner | BigQuery console (INSERT) | Checks 4.1 to 4.4 of [jan_table_and_sku_master.md](jan_table_and_sku_master.md) give no unexpected rows; `MISSING_SKU()` empty for recent dates |
| S2 | Create the yearly workbooks (`FUJ_<YYYY>_AMAZON`, `_RAKUTEN`, `_SHOPIFY`) and update the date filters of their Connected Sheets queries | New year | Owner | Google Sheets | Connected tabs return the new year's deposits (inferred from the year-specific filters; to confirm with the owner) |

## Amazon JP

| # | Step | Trigger | Who | Tool | Check |
|---|---|---|---|---|---|
| A1 | Download the order report (`YYYYMM_W<n>.txt`) covering the settlement's orders | Before each new settlement is loaded | Owner | Seller Central | File name starts with the year |
| A2 | Upload the order report to the landing bucket | after A1 | Owner | Cloud Storage console | New `FILE_NAME` present in `Amazon.order_central_master`; the file is not moved to the repeated-files folder |
| A3 | Download the settlement report (transaction view, `YYYYMMDD.txt`) | Settlement closed and deposited | Owner | Seller Central | |
| A4 | Upload the settlement file to the landing bucket | after A2 (order data first, so purchase dates are found) | Owner | Cloud Storage console | `current_set` tab (or `Amazon.transaction_view_master`) shows the `settlement_id`, `FILE_NAME`, deposit date and total |
| A5 | Wait for `FUJ_Amazon_freee` (daily 00:12 UTC) | after A4 | automatic | | `settlement_id` present in `free.amazon_report_full` |
| A6 | Check for unmapped Amazon lines; add missing rows to `Amazon.nomenclature` | `account LIKE 'MISSING NOMENCLATURE%'` rows for the new settlement | Owner | BigQuery console | No `MISSING NOMENCLATURE` rows left. Rows already appended must be rebuilt for the settlement (procedure to confirm with the owner) |
| A7 | Refresh `AMA_L` / `AMA_S`, paste them into `REP_L` / `REP_S` | after A5/A6 | Owner | Google Sheets `FUJ_2026_AMAZON` | `REP_S`: per settlement, signed total minus the `settlements` total = 0 |
| A8 | Copy the settlement's short rows into the month tab (`2026MM`) | after A7 | Owner | Google Sheets | `settlements` "in master" = 1; `Master` totals per settlement match |
| A9 | Download the month tab as CSV and import it in freee (取引 import) | Month ready | Owner | Google Sheets, freee | Imported totals per 管理番号 match `Master` |

## Rakuten

| # | Step | Trigger | Who | Tool | Check |
|---|---|---|---|---|---|
| R1 | Export the order CSV from RMS for the period not yet loaded | Before each deposit (10th and 25th) | Owner | Rakuten RMS | Period does not overlap previous exports (the loader has no dedup) |
| R2 | Upload the order CSV to its bucket | after R1 | Owner | Cloud Storage console | New `FILE_NAME` in `Rakuten.orders`; `MAX(order_date_time)` covers the deposit's orders; no duplicate (`order_number`, `product_details_id`) |
| R3 | Download the 楽天ペイ payment CSV of the deposit and upload it | Deposit available | Owner | 楽天ペイ / RMS, Cloud Storage console | New `report_id` (`YYYYMMDD`) in `Rakuten.payment_main` (and coupon rows in `Rakuten.payment_coupon`) |
| R4 | Wait for `FUJ_Rakuten_freee` (Saturday 00:00 UTC), with R2 and R3 done before it | after R2 + R3 | automatic | | The `report_id` is present as `control_number` in `free.rakuten_report_full`; `payment_main + payment_coupon - report = 0` for that deposit |
| R5 | Refresh `RAK_L` / `RAK_S`, paste them into `REP_L` / `REP_S` | after R4 | Owner | Google Sheets `FUJ_2026_RAKUTEN` | `check` tab: per 品目, `REP_L` minus `Master` = 0 |
| R6 | Type the BillPay amount of the deposit in `PT` | after R5 | Owner | Google Sheets | `PT` Diff = 0 (which statement figure is typed is to confirm with the owner) |
| R7 | Upload the 楽天ペイ精算書 CSV (`*_Shop_Billpay.csv`) to bucket `rakuten_payment_raw`; the function derives the deposit date and appends the fee rows to `free.rakuten_cost_full` (before 2026-11: paste into `RAK_PAYMENT` `cost_calc` and type 管理番号) | Statement for the deposit available | Owner | Google Sheets `RAK_PAYMENT` | Every row with 請求合計額 <> 0 (except 消費税) has 勘定科目 / 税区分 / 品目 filled |
| R8 | Add any new 集約科目名 to `cost_nomenclature` | R7 shows an unmapped row | Owner | Google Sheets | Row now resolves (36 mappings exist on 2026-10-16, see [rakuten.md](rakuten.md)) |
| R9 | Build the month tab: `RAK_S` rows of the deposit, plus the fee rows from `cost_calc` | after R6-R8 | Owner | Google Sheets | 17 columns; fee rows have 取引先 `Rakuten` |
| R10 | Fill 決済日 from 管理番号 (`20260925` -> `2026-09-25`) for all rows, and fill 決済口座 `Rakuten` and 決済金額 for the fee rows | after R9 | Owner | Google Sheets | No empty 決済日 |
| R11 | Download the month tab as CSV and import it in freee | Month ready | Owner | Google Sheets, freee | Imported totals per 管理番号 match `Master` |

## Shopify (Shopify Payments, Stripe, KOMOJU, Amazon Pay)

| # | Step | Trigger | Who | Tool | Check |
|---|---|---|---|---|---|
| H1 | (automatic) Orders, payouts, Stripe reports and KOMOJU settlements arrive; scheduled queries append new payouts | continuous | automatic | | `FUJ_sheets.Shopify_totals()` lists the payouts of the period |
| H2 | Run `FUJ_scheduled.komoju_rep()` and INSERT the new settlements into `free.sh_komoju` | New KOMOJU settlement in `Shopify.komoju_payments` | Owner | BigQuery console | Only settlements not already in `free.sh_komoju` are inserted (exact statement to confirm with the owner); `KOMOJU` tab difference vs `rep_totals` = 0 |
| H3 | Run `FUJ_extra.SKU_COURSES()` / `FUJ_extra.MISSING_SKU()` and update `free.sku_master` (step S1) | New course or product SKUs | Owner | BigQuery console | No missing Shopify SKUs |
| H4 | Refresh `SH_L`, `SH_S`, `totals`; paste into `REP_L` / `REP_S` | Payouts of the month complete | Owner | Google Sheets `FUJ_2026_SHOPIFY` | `REP_S` `difference_rep_totals` = 0 per payout; `rep_totals` payout dates match the API; `REP_L` `all_true` and no >10 yen flags |
| H5 | Stripe check: paste Stripe payout export next to `free.stripe_freee_full` rows | Month has Stripe payouts | Owner | Google Sheets `stripe` tab | Per charge, report minus Stripe net = 0 |
| H6 | Optional per-order check against the Shopify payout transactions export | A payout does not reconcile | Owner | Google Sheets `Q_RL` | Per order difference 0 |
| H7 | Copy short rows into the month tab; trim Stripe 管理番号 with `MID(id, 5, 20)` | after H4-H6 | Owner | Google Sheets | 管理番号 at most 20 characters; `Master` totals per payout match `totals` |
| H8 | Download the month tab (and the KOMOJU rows, if uploaded separately; to confirm with the owner) as CSV and import in freee | Month ready | Owner | Google Sheets, freee | Imported totals per 管理番号 match `Master` |

## Known manual risks to watch

- **Amazon, Rakuten and Shopify drafts are append-once.** Late uploads (orders after the settlement, a nomenclature row after materialisation) are not picked up without removing and rebuilding that deposit's rows.
- **KOMOJU deposits are missing** from the export until step H2 has been run.
- **Rakuten fee rows and 決済日 exist only in the sheet.** Forgetting R7 to R10 uploads sales without fees or without a settlement date.
- **Product master errors** affect every store at once: a duplicate SKU within a store duplicates its sales rows; a JAN with two names gets an arbitrary name in 品目. Run the checks of step S1 after every change.
