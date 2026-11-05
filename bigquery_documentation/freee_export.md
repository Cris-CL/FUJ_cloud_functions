# freee export

Documented as of 2026-10-16 from the live BigQuery definitions.

This file describes the last mile: from the `FUJ_sheets.*_short()` output to the CSV that is imported into freee with the 取引 (transaction) import. Today this step runs in Google Sheets, by hand. Section 6 lists exactly what an automated pipeline has to reproduce.

## 1. The freee 取引 CSV (17 columns)

One CSV row is one detail line. All three stores use the same header, in this order:

```
収支区分,管理番号,発生日,支払期日,取引先,勘定科目,税区分,金額,税計算区分,税額,備考,品目,部門,メモタグ（複数指定可、カンマ区切り）,決済日,決済口座,決済金額
```

| # | Column | Type in `*_short()` | Rule | Values used |
|---|---|---|---|---|
| 1 | 収支区分 | STRING | Required. Income or expense | `収入`, `支出` |
| 2 | 管理番号 | STRING | Max 20 characters. Identifies the deposit (one settlement or payout) | see section 2 |
| 3 | 発生日 | STRING `YYYY-MM-01` | Accrual date. The short functions truncate it to the first day of the order month | `2026-09-01` |
| 4 | 支払期日 | DATE or NULL | Due date, optional | Amazon: deposit date; others empty |
| 5 | 取引先 | STRING | Business partner name | `Amazon Seller`, `Rakuten`, `Shopify`, `Shopify Stripe`, `Shopify Komoju`, `Shopify Amazon` |
| 6 | 勘定科目 | STRING | Account name as defined in freee | `売上高`, `支払手数料`, `広告宣伝費`, `荷造運賃`, `外注費`, `資本準備金` |
| 7 | 税区分 | STRING | Tax category name as defined in freee. Exact strings, including the full-width parentheses in `（軽）` | `課税売上8%（軽）`, `課税売上10%`, `課対仕入10%`, `対象外` |
| 8 | 金額 | FLOAT64 | Tax-included amount, always positive. The sign is carried by 収支区分 | integer yen |
| 9 | 税計算区分 | STRING | How the tax is computed | always `内税` (amount includes tax) |
| 10 | 税額 | FLOAT64 | Tax contained in 金額 | see section 3 |
| 11 | 備考 | STRING | Remark | `Product Charges`, `Product Charges Refund`, `Product Charges Discount`, `Delivery`, `Fee`, `Points issued`, Amazon nomenclature remarks, Rakuten fee remarks |
| 12 | 品目 | STRING | Item | `Sales <item_name> (<JAN>)`, `Sales`, `Delivery`, `Fee Shopify`, `Fee Amazon`, `Fee Rakuten`, `Advertisement`, `Professional`, `Others` |
| 13 | 部門 | NULL | Department | empty |
| 14 | メモタグ（複数指定可、カンマ区切り） | NULL (column named `メモタグ` in BigQuery) | Memo tags | empty |
| 15 | 決済日 | DATE | Settlement (deposit) date. When 決済日, 決済口座 and 決済金額 are filled, freee records the transaction as settled through that account | per store, see section 2 |
| 16 | 決済口座 | STRING | Settlement account (口座) name in freee | `Amazon Seller`, `Rakuten`, `Shopify` |
| 17 | 決済金額 | FLOAT64 | Settled amount | equals 金額 |

To confirm with the owner:

- whether 取引先, 品目 and 決済口座 values must already exist in freee before the import, or whether freee creates them;
- how freee groups CSV rows into one 取引 (for example by 管理番号 and 発生日);
- the character encoding and the date format of the uploaded CSV. Google Sheets downloads UTF-8; the monthly tabs store real dates and print them in the cell's display format.

## 2. 管理番号 and 決済日 per store

| 取引先 | 管理番号 in `*_short()` | Length | Change at export | 決済日 in `*_short()` |
|---|---|---|---|---|
| Amazon Seller | `settlement_id`, e.g. `12442705783` | 11 | none | deposit date (JST) |
| Rakuten | `report_id` = deposit date `YYYYMMDD`, e.g. `20260925` | 8 | none | NULL: set to `PARSE_DATE('%Y%m%d', 管理番号)` at export |
| Shopify | Shopify Payments `payout_id`, e.g. `146373804344` | 12 | none | payout date + 57 h |
| Shopify Stripe | full `automatic_payout_id`, e.g. `po_1OZNjQDl4oMd4Q8EngRQWgSd` | 27 | trim to 20 chars: `MID(id, 5, 20)` in Sheets, `SUBSTR(id, 5, 20)` in SQL -> `OZNjQDl4oMd4Q8EngRQW` | payout effective date |
| Shopify Komoju | KOMOJU settlement id, e.g. `DGC-27670-464471` | 16 | none (the draft already applies `SUBSTR(payment_id, 1, 20)` to its own control number) | KOMOJU transfer date |
| Shopify Amazon | Amazon Pay `settlementid` | to confirm | none expected (no new rows since 2025-01-07) | settlement end date |

freee limits 管理番号 to 20 characters. The Stripe id is the only one that exceeds it.

## 3. Tax amount (税額) rules

Amounts are tax-included (`内税`), so the tax inside an amount `x` is:

| 税区分 | Rate | Formula | Why |
|---|---|---|---|
| `課税売上8%（軽）` | 8% reduced rate (food, supplements) | `x / 13.5` | 8/108 = 1/13.5 |
| `課税売上10%`, `課対仕入10%` | 10% (shipping, shakers, fees and services) | `x / 11` | 10/110 = 1/11 |
| `対象外` | not taxable | `0` | |

The functions detect the rate with `税区分 LIKE '%8%'` or `LIKE '%10%'`. Rounding differs by stage:

| Stage | Amazon | Rakuten | Shopify |
|---|---|---|---|
| Draft table (`free.*_full.tax_*`) | not stored per line (computed later) | `ABS(TRUNC(x/13.5))` | `ROUND(ABS(x/13.5))` |
| `*_long()` per line | `ABS(ROUND(total_real/13.5))` | `ABS(TRUNC(real_amount/13.5))` | `ABS(TRUNC(real_amount/13.5))` |
| `*_short()` per output row (what is uploaded) | `ROUND(SUM(金額)/13.5)` | `ABS(ROUND(SUM(corrected_value)/13.5))` | `ABS(ROUND(SUM(corrected_value)/13.5))` |

The value that is uploaded is the short one: the tax is recomputed with `ROUND` on the aggregated row, not summed from the lines. The 10% case is the same with `/11`.

Rakuten fee rows are built from tax-exclusive amounts `O`: 金額 = `ROUND(|O| x 1.1, 2)`, 税額 = `ROUND(|O| x 0.1, 2)`, and no tax is added when 税区分 = `対象外` (see [rakuten.md](rakuten.md)).

## 4. Google Sheets workbooks

There is one workbook per store and year: `FUJ_2026_AMAZON`, `FUJ_2026_RAKUTEN`, `FUJ_2026_SHOPIFY`, plus the Rakuten fee workbook `RAK_PAYMENT`. Connected Sheets tabs run against project `test-bigquery-cc`.

| Workbook | Connected tab | Query |
|---|---|---|
| FUJ_2026_AMAZON | `AMA_L` | `SELECT * FROM FUJ_sheets.amazon_long() WHERE 管理番号 IN (10 latest settlement_id of Amazon.transaction_view_master) ORDER BY 管理番号, 発生日 DESC` |
| FUJ_2026_AMAZON | `AMA_S` | `SELECT *, CASE WHEN 収支区分 = '支出' THEN -決済金額 ELSE 決済金額 END AS amount_real FROM FUJ_sheets.amazon_short() WHERE 金額 <> 0 AND 決済日 >= '2024-12-01'` |
| FUJ_2026_AMAZON | `current_set` | Per settlement with deposit since 2026-01-01: `settlement_id`, `FILE_NAME`, deposit date (+9 h), `SUM(amount)` from `Amazon.transaction_view_master` |
| FUJ_2026_RAKUTEN | `RAK_L` | `SELECT * FROM FUJ_sheets.rakuten_long() WHERE CAST(管理番号 AS INT64) > 20251200` |
| FUJ_2026_RAKUTEN | `RAK_S` | `SELECT * FROM FUJ_sheets.rakuten_short() WHERE CAST(管理番号 AS INT64) >= 20251201` |
| FUJ_2026_SHOPIFY | `SH_L` | `SELECT * FROM FUJ_sheets.shopify_long() WHERE 決済日 > '2025-11-30' ORDER BY 取引先, 管理番号 DESC, 発生日, 備考, order_number, occurrence_number` |
| FUJ_2026_SHOPIFY | `SH_S` | `SELECT * FROM FUJ_sheets.shopify_short() WHERE 決済日 > '2025-11-30' AND 金額 <> 0` |
| FUJ_2026_SHOPIFY | `totals` | `SELECT * FROM FUJ_sheets.Shopify_totals() WHERE CAST(payout_date AS DATE) BETWEEN '2026-01-01' AND '2026-12-31'` |

Every workbook follows the same tab pattern:

- **Connected tabs** (hidden).
- **`REP_L` / `REP_S`:** working copies pasted from the connected tabs, with check columns.
- **Reconciliation tabs:** Amazon `settlements`; Rakuten `PT`, `check`, `LAST`; Shopify `rep_totals`, `stripe`, `KOMOJU`, `Q_RL`.
- **Monthly tabs `202601` ... `202612`:** hold the final 17 columns. These are the tabs downloaded as the freee CSV.
- **`Master`:** a QUERY union of all monthly tabs with a signed value column (`IF(収支区分 = "支出", -金額, 金額)`), used for totals per 管理番号 and per 品目, and to see which deposits were already exported.

To confirm with the owner:

- the purpose of the half-year tabs (`01-06`, `07-12`) and the `202512` carry-over tabs;
- which month tab a deposit goes into: the Rakuten `202609` tab starts with 管理番号 `20260825`;
- whether Shopify Stripe and KOMOJU rows are uploaded from the monthly tab or separately. In the 2026-09 snapshot, KOMOJU rows were in the `KOMOJU` tab.

## 5. Reconciliation checks (all must give 0 before upload)

| Store | Check | Where today | BigQuery equivalent |
|---|---|---|---|
| Amazon | Per settlement: signed sum of the short rows = settlement total (deposit) | `REP_S` difference column vs `settlements` | `SUM(IF(収支区分='支出',-金額,金額))` of `amazon_short()` per 管理番号 vs `SUM(amount)` of `Amazon.transaction_view_master` per `settlement_id` |
| Amazon | No unmapped Amazon line | visual | `SELECT COUNT(*) FROM free.amazon_report_full WHERE account LIKE 'MISSING NOMENCLATURE%'` = 0 |
| Rakuten | Per deposit: sales total = BillPay amount typed by hand | `PT` (`Diff`) | `SUM(Rakuten.payment_main.rakuten_pay_money) + SUM(Rakuten.payment_coupon.coupon_amount) - SUM(free.rakuten_report_full.real_amount)` per `report_id` = 0 (logic of `FUJ_test.check_rakuten_freee()`) |
| Rakuten | Per deposit and 品目: long = monthly rows | `check` | `SUM(corrected_value)` of `rakuten_long()` vs `SUM(signed 金額)` of `rakuten_short()` per 管理番号, 品目 |
| Rakuten | Every fee line mapped | `free.rakuten_cost_full` (`FUJ_test.check_rakuten_cost()`) | no `MISSING NOMENCLATURE` in `account`; every 品目 with 請求（税抜額） <> 0 has a row in `Rakuten.cost_nomenclature` |
| Shopify | Per payout: signed sum of short rows = payout total | `REP_S` `difference_rep_totals`, `rep_totals` | `shopify_short()` signed sum per 管理番号 vs `FUJ_sheets.Shopify_totals().total` per `control_number` (Stripe: compare with the untrimmed id) |
| Shopify | Stripe per charge: report = Stripe net | `stripe` tab | `free.stripe_freee_full.real_amount` summed per `source_id_code` vs `Shopify.stripe_payouts.net` per `charge_id` |
| Shopify | KOMOJU per settlement = bank transfer amount | `KOMOJU` tab | `free.sh_komoju` signed sum per `SETT_NUMBER` vs `Shopify_totals()` rows with `pay_method = 'Komoju'` |
| All | Bundle splits add up | `REP_L` checks | per store, `SUM(corrected_value)` of the components = `verif_amount` of the sold line |
| All | Product master sane | | the checks in [jan_table_and_sku_master.md](jan_table_and_sku_master.md) section 4 |

## 6. Done by hand today

| Task | Store | Detail |
|---|---|---|
| Fee rows | Rakuten | Upload the 楽天ペイ精算書 (`*_Shop_Billpay.csv`) to bucket `rakuten_payment_raw`; copy the rows of the deposit from `free.rakuten_cost_full` into the monthly tab (before 2026-11: `RAK_PAYMENT` `cost_calc`) |
| 決済日 | Rakuten | Fill from 管理番号 (`20260925` -> `2026-09-25`) for sales and fee rows |
| Nomenclature additions | Amazon | Add a row to `Amazon.nomenclature` when `MISSING NOMENCLATURE` appears |
| Fee mapping additions | Rakuten | Add a row to `Rakuten.cost_nomenclature` for a new 品目 (shown as `MISSING NOMENCLATURE`), then rebuild the deposit in `free.rakuten_cost_full` |
| sku_master additions | all | Insert rows into `free.sku_master` for new SKUs and bundles (helped by `FUJ_extra.MISSING_SKU()` and `FUJ_extra.SKU_COURSES()`) |
| KOMOJU report | Shopify | Run `FUJ_scheduled.komoju_rep()` and INSERT the new settlements into `free.sh_komoju` |
| Stripe 管理番号 trim | Shopify | `MID(id, 5, 20)` |
| Copy, reconcile, download, import | all | Paste connected data into the working tabs, check differences, copy to the monthly tab, download CSV, import in freee |

The full ordered checklist is in [manual_steps.md](manual_steps.md).

## 7. What an automated pipeline must reproduce

Starting point: the output of `FUJ_sheets.amazon_short()`, `FUJ_sheets.rakuten_short()` and `FUJ_sheets.shopify_short()`. These table functions are computed on the fly from the draft tables (not materialised) and always return all history, so an export step must:

1. **Select the deposits to export.** Use the deposits (管理番号) whose 決済日 falls in the target month (for Rakuten, the date in 管理番号) and that were not exported before. Today the `Master` tab is the only record of what was exported, so an automated pipeline needs its own export log keyed by (取引先, 管理番号).
2. **Gate on reconciliation.** Export a deposit only when the checks in section 5 give 0. Hold back Shopify payouts with NULL 決済日 or still pending.
3. **Drop zero rows:** `金額 <> 0` (the Amazon and Shopify connected queries filter this; Rakuten does not, so apply it too).
4. **Keep exactly the 17 columns in order.** Drop extra columns: `real_amount` from `rakuten_short()`, and `amount_real` if it was added as in `AMA_S`.
5. **Rename the header** `メモタグ` to `メモタグ（複数指定可、カンマ区切り）`.
6. **Store-specific changes:**
   - **Shopify Stripe:** `管理番号 = SUBSTR(管理番号, 5, 20)` for rows with `取引先 = 'Shopify Stripe'`. Keep the full id in the export log for reconciliation against `Shopify_totals()`.
   - **Shopify KOMOJU:** make sure `free.sh_komoju` contains the settlement first. The report is not scheduled; with automation, insert `komoju_rep()` rows whose `SETT_NUMBER` is not yet in `free.sh_komoju`.
   - **Rakuten 決済日:** `決済日 = PARSE_DATE('%Y%m%d', 管理番号)`. 決済口座 is already `Rakuten`.
   - **Rakuten fee rows:** append one row per 楽天ペイ statement line of the deposit with 請求合計額 (H) <> 0:

     | Column | Value |
     |---|---|
     | 収支区分 | `支出` if H > 0, `収入` if H < 0 |
     | 管理番号 | deposit date `YYYYMMDD` |
     | 発生日 | 精算対象期間終了日 |
     | 支払期日 | empty |
     | 取引先 | `Rakuten` |
     | 勘定科目, 税区分, 備考, 品目 | from the 36-row `cost_nomenclature` mapping in [rakuten.md](rakuten.md); 消費税 lines skipped |
     | 金額 | `ABS(O) * 1.1`, or `ABS(O)` when 税区分 = `対象外` |
     | 税計算区分 | `内税` |
     | 税額 | `ABS(O) * 0.1`, or 0 when 税区分 = `対象外` |
     | 部門, メモタグ | empty |
     | 決済日 | the deposit date |
     | 決済口座 | `Rakuten` |
     | 決済金額 | = 金額 |

     Done on 2026-11-05: the statement CSV is loaded to `Rakuten.statement` by `payment-rakuten`, the deposit date (`report_id`) is the 精算対象期間終了日 of the `楽天ﾍﾟｲ_決済金*` line (verified on all 63 hand-typed 管理番号), and the fee rows are in `free.rakuten_cost_full` (rakuten.md section 4).
   - **Amazon:** none. 金額 can be fractional when a bundle amount is not divisible by its units; rounding is to confirm with the owner.
7. **Format.** Write dates (発生日, 支払期日, 決済日) as dates, with 発生日 the first day of the month. Write amounts as integers (to confirm for the Rakuten fee rows, whose formulas keep 2 decimals).
8. **Write the CSV** with the header from section 1 and import it as freee 取引. Encoding, and any freee-side requirement that partners, items or accounts exist in advance, are to confirm with the owner.

Minimal BigQuery shape of the export for one store and month (reconciliation and export log still needed):

```sql
SELECT * EXCEPT(real_amount) REPLACE(PARSE_DATE('%Y%m%d', `管理番号`) AS `決済日`)
FROM FUJ_sheets.rakuten_short()
WHERE `金額` <> 0 AND `管理番号` BETWEEN '20260901' AND '20260930';
```

```sql
SELECT * REPLACE(IF(`取引先` = 'Shopify Stripe', SUBSTR(`管理番号`, 5, 20), `管理番号`) AS `管理番号`)
FROM FUJ_sheets.shopify_short()
WHERE `金額` <> 0 AND `決済日` BETWEEN '2026-09-01' AND '2026-09-30';
```
