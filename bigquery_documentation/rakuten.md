# Rakuten

Documented as of 2026-10-16 from the live BigQuery definitions; cost chain (sections 1, 2, 3 fee side, 4, 5, 7, 8) updated 2026-11-05.

Chain: RMS order CSV -> `Rakuten.orders` -> `FUJ_extra.Rakuten_full()` ; 楽天ペイ payment CSV -> `Rakuten.payment_main` (+ `Rakuten.payment_coupon`) ; both -> `FUJ_scheduled.rakuten_report()` -> scheduled query `FUJ_Rakuten_freee` -> `free.rakuten_report_full` -> `FUJ_sheets.rakuten_long()` -> `FUJ_sheets.rakuten_short()` -> workbook `FUJ_2026_RAKUTEN` (+ fee rows from `free.rakuten_cost_full`, + 決済日 filled by hand) -> freee CSV.

Cost chain (since 2026-11): 楽天ペイ Billpay statement CSV (`*_Shop_Billpay.csv`) -> `Rakuten.statement` -> `FUJ_scheduled.rakuten_cost_report()` (+ `Rakuten.cost_nomenclature`) -> `free.rakuten_cost_full`, appended by the same Cloud Function right after the statement lands (no scheduled query). Before 2026-11 the fee rows were built by hand in Google Sheet `RAK_PAYMENT`.

Rakuten pays out twice a month. In 2026 the deposit ids (`report_id`) are on the 10th and the 25th: `20260725`, `20260810`, `20260825`, `20260910`, `20260925`.

## 1. Sources and ingestion

| Data | File (downloaded by hand) | How it lands | Landing table | Dedup rule |
|---|---|---|---|---|
| Orders | RMS order CSV export (CP932, Japanese headers such as 注文番号, ステータス) | Uploaded to a Cloud Storage bucket (name to confirm with the owner); a Cloud Function triggered by the upload translates about 200 Japanese headers to English snake_case, drops columns that are not in the table, and appends (WRITE_APPEND) | `Rakuten.orders` | No dedup in the loader. The live table has no duplicate (`order_number`, `product_details_id`) pairs (47,332 rows from 70 files), so exports are apparently non-overlapping; how this is ensured is to confirm with the owner |
| 楽天ペイ payments (deposit detail per order) | 楽天ペイ payment CSV (header on row 3; a main section and a coupon section separated by 連番 / 合計 rows) | Uploaded to Cloud Storage; a Cloud Function triggered by the upload splits it into the main and coupon sections and appends both | `Rakuten.payment_main`, `Rakuten.payment_coupon` | No dedup in the loader (to confirm with the owner). How `report_id` (the deposit date) is assigned at load time is to confirm with the owner |
| 楽天ペイ statement (精算書, fees) | Billpay statement CSV, name ends in `_Shop_Billpay.csv` (CP932, header on row 1, 17 columns) | Uploaded to bucket `rakuten_payment_raw`; Cloud Function `payment-rakuten` routes on the file name, types the 17 columns, derives `report_id` and appends; then appends the cost rows of every deposit not yet in `free.rakuten_cost_full` (section 4) | `Rakuten.statement` | The function refuses a 精算書No already present (nothing written). Before 2026-11: pasted into Google Sheet `RAK_PAYMENT`, tab `cost_calc` |

## 2. Landing tables

| Table | Rows (2026-10) | Grain | Key columns |
|---|---|---|---|
| `Rakuten.orders` | 47,332 | One order line (one product of one order; order-level fields repeated) | `order_number`, `product_details_id`, `status`, `order_date_time` DATETIME (JST), `item_number`, `sku_management_number`, `sku_integration_number`, `sku_info`, `unit_price`, `quantity`, `shipping_total`, `store_issued_coupon_usage_amount`, `FILE_NAME` |
| `Rakuten.payment_main` | 49,511 (151 `report_id`) | One payment line of an order in a 楽天ペイ deposit | `report_id` (deposit date `YYYYMMDD`), `order_number`, `payment_number`, `rakuten_pay_money`, `payment_method`, `payment_confirmation_date` |
| `Rakuten.payment_coupon` | 1,072 | One coupon line in a deposit | `report_id`, `order_number`, `coupon_name`, `coupon_amount` |
| `Rakuten.statement` | 63 (4 statements, 2026-11) | One line of a 楽天ペイ Billpay statement (payment, deduction and cost lines) | `statement_number` (精算書No), `report_id`, `payment_billing_category` (支払 / 支払控除 / 請求), `subject_name` (品目), `billing_amount_tax_excluded`, `period_end_date`, `amount`, `FILE_NAME` |
| `Rakuten.cost_nomenclature` | 37 | One mapping row per statement 品目 | `subject_name`, `remarks`, `item`, `tax_distinction`, `account`, `translation`, `ADDED_DATETIME` (section 7) |

`Rakuten.orders.status` values (meaning as encoded in the project's routines) and row counts on 2026-10-16:

| status | meaning | rows |
|---|---|---|
| 100 | Waiting for order confirmation | 1 |
| 200 | Rakuten processing | 8 |
| 300 | Waiting for shipment | 28 |
| 400 | Waiting for change confirmation | 0 |
| 500 | Shipped | 479 |
| 600 | Payment in progress | 3,348 |
| 700 | Payment completed | 42,581 |
| 800 | Cancellation waiting for confirmation | 0 |
| 900 | Cancellation confirmed | 887 |

### All columns of `Rakuten.orders` (164)

The table holds customer personal data (names, addresses, phone, e-mail); none of it is used by the accounting chain except the postal code.

| # | column | type | # | column | type | # | column | type |
|---|---|---|---|---|---|---|---|---|
| 1 | `order_number` | STRING | 56 | `total_amount_of_goods_to_be_sent_to` | FLOAT64 | 111 | `excluded_tax_total` | FLOAT64 |
| 2 | `status` | STRING | 57 | `destination_total_amount` | FLOAT64 | 112 | `settlement_fee_tax_rate` | STRING |
| 3 | `substatus_id` | STRING | 58 | `noshi` | STRING | 113 | `wrapping_tax_rate_1` | STRING |
| 4 | `sub_status` | STRING | 59 | `destination_postal_code_1` | STRING | 114 | `wrapping_tax_amount_1` | STRING |
| 5 | `order_date_time` | DATETIME | 60 | `mailing_address_postal_code_2` | STRING | 115 | `wrapping_tax_rate_2` | STRING |
| 6 | `order_date` | DATETIME | 61 | `shipping_address_prefecture` | STRING | 116 | `wrapping_tax_amount_2` | STRING |
| 7 | `order_time` | DATETIME | 62 | `mailing_address_county_city` | STRING | 117 | `total_destination_tax` | STRING |
| 8 | `cancellation_deadline_date` | DATETIME | 63 | `shipping_address_subsequent_address` | STRING | 118 | `destination_postage_tax_rate` | STRING |
| 9 | `order_confirmation_date` | DATETIME | 64 | `mailing_to_last_name` | STRING | 119 | `shipping_destination_cod_tax_rate` | STRING |
| 10 | `order_confirmed_date` | DATETIME | 65 | `destination_name` | STRING | 120 | `commodity_tax_rate` | STRING |
| 11 | `shipment_instruction_date_and_time` | DATETIME | 66 | `mailing_address_last_name_kana` | STRING | 121 | `tax_included_price_for_each_product` | STRING |
| 12 | `shipment_completion_report_date` | DATETIME | 67 | `destination_name_kana` | STRING | 122 | `tax_rate_10_percent` | STRING |
| 13 | `payment_method_name` | STRING | 68 | `destination_phone_number_1` | STRING | 123 | `billed_amount_10_percent` | STRING |
| 14 | `credit_card_payment_method` | STRING | 69 | `mailing_address_phone_number_2` | STRING | 124 | `tax_on_billed_amount_10_percent` | STRING |
| 15 | `number_of_credit_card_payments` | FLOAT64 | 70 | `mailing_address_phone_number_3` | STRING | 125 | `total_amount_10_percent` | STRING |
| 16 | `shipping_method` | STRING | 71 | `product_details_id` | STRING | 126 | `settlement_fee_10_percent` | STRING |
| 17 | `shipping_category` | STRING | 72 | `product_id` | STRING | 127 | `coupon_discount_amount_10_percent` | STRING |
| 18 | `order_type` | STRING | 73 | `product_name` | STRING | 128 | `usage_points_10_percent` | STRING |
| 19 | `multiple_destination_flag` | STRING | 74 | `item_number` | STRING | 129 | `tax_rate_8_percent` | STRING |
| 20 | `destination_match_flag` | STRING | 75 | `merchandise_control_number` | STRING | 130 | `billed_amount_8_percent` | STRING |
| 21 | `remote_island_flag` | STRING | 76 | `unit_price` | FLOAT64 | 131 | `tax_on_billed_amount_8_percent` | STRING |
| 22 | `rakuten_confirmation_flag` | STRING | 77 | `quantity` | FLOAT64 | 132 | `total_amount_8_percent` | STRING |
| 23 | `warning_display_type` | STRING | 78 | `shipping_fee_not_included` | STRING | 133 | `settlement_fee_8_percent` | STRING |
| 24 | `rakuten_member_flag` | STRING | 79 | `tax_not_included` | STRING | 134 | `coupon_discount_amount_8_percent` | STRING |
| 25 | `purchase_history_modification_flag` | STRING | 80 | `cod_fee_not_included` | STRING | 135 | `points_used_8_percent` | STRING |
| 26 | `product_total_price` | FLOAT64 | 81 | `item_choice` | STRING | 136 | `tax_rate_0_percent` | STRING |
| 27 | `total_consumption_tax` | FLOAT64 | 82 | `point_multiplier` | STRING | 137 | `billed_amount_0_percent` | STRING |
| 28 | `shipping_total` | FLOAT64 | 83 | `delivery_date_information` | STRING | 138 | `tax_on_invoice_amount_0_percent` | STRING |
| 29 | `cash_on_delivery_total` | FLOAT64 | 84 | `inventory_type` | STRING | 139 | `total_amount_0_percent` | STRING |
| 30 | `billing_amount` | FLOAT64 | 85 | `wrapping_title_1` | STRING | 140 | `payment_fee_0_percent` | STRING |
| 31 | `total_amount` | FLOAT64 | 86 | `wrapping_name_1` | STRING | 141 | `coupon_discount_amount_0_percent` | STRING |
| 32 | `amount_of_points_used` | FLOAT64 | 87 | `wrapping_fee_1` | STRING | 142 | `usage_points_0_percent` | STRING |
| 33 | `total_coupon_usage` | FLOAT64 | 88 | `lapping_including_tax_1` | STRING | 143 | `single_item_delivery_flag` | STRING |
| 34 | `store_issued_coupon_usage_amount` | FLOAT64 | 89 | `wrapping_type_1` | STRING | 144 | `delivery_company_at_the_time_of_purchase` | STRING |
| 35 | `amount_of_coupons_issued_by_rakuten` | FLOAT64 | 90 | `wrapping_title_2` | STRING | 145 | `number_of_receipts_issued` | STRING |
| 36 | `orderer_zip_code_1` | STRING | 91 | `wrapping_name_2` | STRING | 146 | `first_issue_date_and_time_of_receipt` | DATETIME |
| 37 | `orderer_postal_code_2` | STRING | 92 | `wrapping_fee_2` | STRING | 147 | `date_and_time_of_last_issue_of_receipt` | DATETIME |
| 38 | `orderer_address_prefecture` | STRING | 93 | `wrapping_tax_included_2` | STRING | 148 | `payment_due_date` | DATETIME |
| 39 | `orderers_address` | STRING | 94 | `wrapping_type_2` | STRING | 149 | `deadline_for_changing_payment_method` | DATETIME |
| 40 | `orderer_address_subsequent_address` | STRING | 95 | `delivery_time_zone` | STRING | 150 | `deadline_for_refund_procedure` | DATETIME |
| 41 | `last_name_of_orderer` | STRING | 96 | `specify_delivery_date` | STRING | 151 | `store_issued_coupon_code` | STRING |
| 42 | `orderer_name` | STRING | 97 | `manager` | STRING | 152 | `store_issued_coupon_name` | STRING |
| 43 | `orderer_surname_kana` | STRING | 98 | `note` | STRING | 153 | `rakuten_issued_coupon_code` | STRING |
| 44 | `orderers_name_kana` | STRING | 99 | `mail_insert_message_to_customer` | STRING | 154 | `rakuten_issued_coupon_name` | STRING |
| 45 | `orderer_phone_number_1` | STRING | 100 | `request_for_gift_delivery` | STRING | 155 | `warning_display_type_details` | STRING |
| 46 | `orderer_phone_number_2` | STRING | 101 | `comment` | STRING | 156 | `sku_management_number` | STRING |
| 47 | `orderer_phone_number_3` | STRING | 102 | `terminal_used` | STRING | 157 | `sku_integration_number` | STRING |
| 48 | `orderer_email_address` | STRING | 103 | `mail_carrier_code` | STRING | 158 | `sku_info` | STRING |
| 49 | `gender_of_orderer` | STRING | 104 | `tomorrow_raku_hope_flag` | STRING | 159 | `strongest_delivery_flag` | STRING |
| 50 | `application_number` | STRING | 105 | `pharmaceutical_order_flag` | STRING | 160 | `order_same_day_shipping_flag` | STRING |
| 51 | `application_delivery_times` | STRING | 106 | `rakuten_super_deal_product_order_flag` | STRING | 161 | `delivery_flag` | STRING |
| 52 | `recipient_id` | STRING | 107 | `membership_program_order_type` | STRING | 162 | `delivery_location` | STRING |
| 53 | `destination_postage` | STRING | 108 | `settlement_fee` | FLOAT64 | 163 | `UPDATED_AT_UTC` | STRING |
| 54 | `delivery_address` | STRING | 109 | `orderers_contribution_total` | STRING | 164 | `FILE_NAME` | STRING |
| 55 | `destination_consumption_tax_total` | STRING | 110 | `total_store_charges` | FLOAT64 |  |  |  |

### Columns of the payment tables

| Table | Columns |
|---|---|
| `Rakuten.payment_main` | `order_confirmation_date` TIMESTAMP, `order_number` STRING, `payment_number` STRING, `payment_institution_link_number` STRING, `onquiry_number` STRING, `payment_confirmation_date` TIMESTAMP, `rakuten_pay_money` FLOAT64, `payment_method` STRING, `summary` STRING, `report_id` STRING |
| `Rakuten.payment_coupon` | `order_number` STRING, `order_confirmation_date` TIMESTAMP, `coupon_name` STRING, `coupon_amount` FLOAT64, `coupon_confirmation_date` TIMESTAMP, `report_id` STRING |

`payment_method` values seen: クレジットカード, 楽天ポイント, 楽天スーパーポイント, 楽天口座決済, 後払い決済, セブンイレブン（前払）, ファミリーマート、ローソン等（前払）, ローソン、郵便局ATM等（前払）, 決済変更.

### Columns of `Rakuten.statement`

CSV header -> column, in file order; `-` in the file is NULL. Created by `queries_bq/rakuten_cost/01_rakuten_statement.sql`.

| Column | Type | Statement field |
|---|---|---|
| `issue_date` | DATE | 発行日 |
| `statement_number` | STRING | 精算書No |
| `store_statement_number` | STRING | 店舗別内訳書No |
| `store_id` | STRING | 店舗別ID |
| `url` | STRING | ＵＲＬ |
| `store_name` | STRING | 店舗名 |
| `payment_amount_tax_included` | FLOAT64 | 支払（税込額） |
| `billing_amount_tax_excluded` | FLOAT64 | 請求（税抜額）; cost lines are the rows where this is not 0 |
| `billing_tax_amount` | FLOAT64 | 請求（税額）; only the 消費税 line has a value |
| `payment_billing_category` | STRING | 支払/請求分類: 支払, 支払控除, 請求 |
| `aggregate_subject` | STRING | 集約科目 |
| `subject_name` | STRING | 品目 (集約科目名 in the older 16-column layout); exact-match key of `Rakuten.cost_nomenclature` |
| `period_start_date` | DATE | 精算対象期間開始日 |
| `period_end_date` | DATE | 精算対象期間終了日 (becomes 発生日) |
| `amount` | FLOAT64 | 金額 (tax exclusive) |
| `consumption_tax` | FLOAT64 | うち消費税 |
| `tax_rate` | FLOAT64 | 税率 (10, 0, NULL) |
| `report_id` | STRING | deposit date `YYYYMMDD` = 精算対象期間終了日 of the single line whose 品目 starts with `楽天ﾍﾟｲ_決済金` (決済金 in old files, 決済金等 since 2026); same key as `payment_main.report_id` |
| `FILE_NAME` | STRING | object name in the bucket |

## 3. Account mapping

### Sales side: hard-coded in `FUJ_scheduled.rakuten_report()`

| Line | Built from | 勘定科目 | 税区分 | 税額 in draft | 備考 (draft) | 品目 |
|---|---|---|---|---|---|---|
| Product | each order line: `unit_price * quantity` | 売上高 | `課税売上8%（軽）`; `課税売上10%` for product names starting `GoCLN（ゴークリーン） シェイカー` | `ABS(TRUNC(x/13.5))` or `/11` | `Product Charges (#<order_number>)` | `Sales` |
| Delivery | one line per order with `shipping_total > 0` | 売上高 | `課税売上10%` | `ABS(TRUNC(x/11))` | `Delivery (#<order_number>)` | `Delivery` |
| Store coupon | one line per order with `store_issued_coupon_usage_amount > 0`, amount negative | 売上高 | `課税売上8%（軽）` | `/13.5` | `Points issued (#<order_number>)` | `Sales` |

For cancelled orders (`status = '900'`) the delivery and coupon lines are set to 0 (and then dropped); product lines are not zeroed.

### Fee side: `Rakuten.cost_nomenclature` + `FUJ_scheduled.rakuten_cost_report()`

Fees, advertising and logistics charges are invoiced in the 楽天ペイ statement (精算書). Since 2026-11 the statement is loaded to `Rakuten.statement` and the freee rows are built in BigQuery by `rakuten_cost_report()` (section 4) with the mapping table `Rakuten.cost_nomenclature` (listed in section 7). Google Sheet `RAK_PAYMENT` is the previous manual procedure and the reference the routine was verified against.

## 4. Report function and scheduled query

### `FUJ_extra.Rakuten_full()`

Flattens `Rakuten.orders` to: `order_number`, `order_date` (= `order_date_time`), `status`, `product_name` (with ` Info: <sku_info>` appended when present), `unit_price`, `quantity`, `shipping_total`, `store_issued_coupon_usage_amount`, `zip_code` (`FUJ_extra.zipCodeRak()`), `item_number` (remapped, see below), `ORIGINAL_ITEM_NUMBER`, `merchandise_control_number`, `sku_management_number`, `sku_integration_number`, `sku_info`, `FILE_NAME`.

`item_number` remapping (multi-variant listings to the JAN of the chosen variant):

| Listing `item_number` contains | Condition | Resulting item_number |
|---|---|---|
| `4799387107399` | `sku_info` ends with プレーン味 / ココア味 / 抹茶味 / ダージリン味 | `4799460868167` / `4799387107399` / `4799422005319` / `4580581540479` |
| `4580581540257` | `sku_info` ends with 抹茶味 | `4580581540417` |
| `4580581540363` | `sku_integration_number` present and not in scientific notation (`E+`) | `sku_integration_number` |
| `4580581541155`, `4580581540233`, `4580581541285`, `4580581541216`, `4580581540455` | `sku_management_number` present and not `E+` | `sku_management_number` |
| `6546481414215` | `sku_info` contains 3本セット | `sku_management_number` |
| `6546470731847` | `sku_info` contains 2本セット | `sku_management_number` |
| anything else | | `item_number` |

### `FUJ_scheduled.rakuten_report()`

- Takes `Rakuten_full()`; normalises `item_number` (`v_` prefix: characters 3-15; otherwise the first 13 characters); builds product, delivery and coupon lines (section 3).
- `control_number` = `report_id` from a LEFT JOIN with `SELECT DISTINCT order_number, report_id FROM Rakuten.payment_main` on `order_number`. Orders not yet paid out have no `report_id` and are filtered out.
- `balance` = `収入` if the line amount >= 0 else `支出`; `supplier = 'Rakuten'`; `payment_date` = NULL; `amount` = absolute value; `real_amount` = signed value.
- Filters inside the function: `control_number IS NOT NULL`, `amount <> 0`, `control_number NOT IN (SELECT control_number FROM free.rakuten_report_full)`.

### Scheduled query `FUJ_Rakuten_freee`

| Property | Value |
|---|---|
| Schedule | every Saturday 00:00 UTC (09:00 JST Saturday) |
| Query | `SELECT * FROM FUJ_scheduled.rakuten_report() WHERE control_number IS NOT NULL AND amount <> 0 AND control_number NOT IN (SELECT DISTINCT control_number FROM free.rakuten_report_full WHERE control_number IS NOT NULL)` |
| Destination | `free.rakuten_report_full`, WRITE_APPEND |
| Dedup | by `control_number` (= deposit `report_id`): a deposit is written once |

### `FUJ_scheduled.rakuten_cost_report()` (cost chain, 2026-11)

Definition: `queries_bq/rakuten_cost/03_rakuten_cost_report.sql`.

- Source `Rakuten.statement` LEFT JOIN `Rakuten.cost_nomenclature` ON `subject_name` (exact match, no normalisation). Only lines with `billing_amount_tax_excluded <> 0`: payment (支払), deduction (支払控除) and 消費税 lines are skipped, as in the sheet.
- `balance` = `支出` when 請求（税抜額） > 0, `収入` when < 0; `control_number` = `report_id`; `order_date` = 精算対象期間終了日; `supplier = 'Rakuten'`; `payment_date` NULL; `tax_calculation_distinction = '内税'`.
- Tax in whole yen, reconciled to the statement (this is where it departs from the sheet, which rounded each line and was 1-2 yen off): taxable means the statement's own 税率 column is 10 (0 for the ﾎﾟｲﾝﾄ付与料 lines). Rakuten invoices one 消費税 line computed on the statement total and truncated; each line gets `FLOOR(税抜 x 税率)` and the remaining yen go, one each, to the lines with the largest fractional parts (largest remainder) until the per-line taxes add up to that 消費税 line. `amount` = 税抜 + tax, `tax_amount` = tax, `real_amount` signed (negative for 支出); per deposit `SUM(amount)` equals the statement's 請求 total exactly. Without a 請求 消費税 line the target is `TRUNC(sum of exact taxes)`.
- Unknown 品目: `account = 'MISSING NOMENCLATURE {subject_name:...}'`, NULL 税区分 / item / remarks; the tax still follows the statement's 税率; the row is kept.
- `order_id` = 精算書No, `product_name` = statement 品目, `full_date` = 発生日 as DATETIME; `item_number`, `quantity`, `zip_code` NULL.
- No dedup inside the function (a deposit can be re-checked after it was written); the append step does it.
- Verified on 2026-11-05 against sheet `RAK_PAYMENT` for 管理番号 20260910 and 20261010: 46 rows, same rows, same 収支区分 / 発生日 / 勘定科目 / 税区分 / 備考 / 品目; per-line taxes equal the sheet's rounded values except on the lines that absorb the remainder (1 yen each), and the totals equal the statement's 消費税 and 請求 totals exactly, which the sheet's did not.

### Append step (cost chain, no scheduled query)

Right after a statement lands, Cloud Function `payment-rakuten` runs the query recorded in `queries_bq/rakuten_cost/05_materialise_cost_rows.sql`:

| Property | Value |
|---|---|
| When | at the end of every Billpay statement load (`materialise_cost_rows()` in `rakuten/payment_rakuten/main.py`); by hand with the same file to re-trigger it |
| Query | `INSERT INTO free.rakuten_cost_full SELECT * FROM FUJ_scheduled.rakuten_cost_report() WHERE control_number IS NOT NULL AND amount <> 0 AND control_number NOT IN (SELECT DISTINCT control_number FROM free.rakuten_cost_full WHERE control_number IS NOT NULL)` |
| Dedup | by `control_number`: a deposit is written once, a re-run appends nothing, a deposit whose append failed is picked up by the next statement load |

`FUJ_Rakuten_freee` (sales) is unchanged.

## 5. Draft table `free.rakuten_report_full`

52,396 rows. One row per deposit x order x line (product, delivery, coupon).

| Column | Type | freee meaning |
|---|---|---|
| `balance` | STRING | 収支区分 |
| `control_number` | STRING | 管理番号 (`YYYYMMDD` of the deposit) |
| `order_date` | DATE | 発生日 |
| `payment_date` | INT64 | 支払期日 (always NULL) |
| `supplier` | STRING | 取引先 (`Rakuten`) |
| `account` | STRING | 勘定科目 (`売上高`) |
| `tax_distinction` | STRING | 税区分 |
| `amount` | FLOAT64 | 金額 (absolute) |
| `tax_calculation_distinction` | STRING | `内税` |
| `tax_amount` | FLOAT64 | 税額 |
| `remarks` | STRING | 備考 with `(#order_number)` |
| `item` | STRING | 品目 before JAN naming |
| `order_id` | STRING | Rakuten order number |
| `item_number` | STRING | normalised Rakuten item number (joined to `free.sku_master.sku_item`) |
| `quantity` | FLOAT64 | units |
| `full_date` | DATETIME | order datetime |
| `zip_code` | STRING | delivery postal code |
| `real_amount` | FLOAT64 | signed amount |
| `product_name` | STRING | product name (or `delivery` / `points`) |

### `free.rakuten_cost_full` (cost rows, 2026-11)

Same 19 columns and types as `free.rakuten_report_full` (`queries_bq/rakuten_cost/04_rakuten_cost_full.sql`), kept in a separate table because the sales dedup by `control_number` would skip the cost rows of deposits already written. Amounts are whole yen. 46 rows on 2026-11-05 (deposits 20260910 and 20261010, both booked in freee from the sheet before this table existed). For cost rows: `account` is 支払手数料 / 広告宣伝費 / 荷造運賃 (or `MISSING NOMENCLATURE {...}`), `remarks` and `item` come from the nomenclature, `order_id` = 精算書No, `product_name` = statement 品目, `item_number` / `quantity` / `zip_code` are NULL.

## 6. `FUJ_sheets.rakuten_long()` and `FUJ_sheets.rakuten_short()`

### `rakuten_long()`

- Source `free.rakuten_report_full` LEFT JOIN `FUJ_extra.JanTable()` on `rak.item_number = janc.original_sku AND rak.supplier = janc.store_name` (store `Rakuten`).
- 税区分 override: when the bundle name matches `%set%shaker%` (case-insensitive) the component lines get `課税売上8%（軽）`.
- Component amount (`amount`, `tax_amount`, `real_amount`):
  - `Set Complete Cocoa.1375`: shaker component 0, other component full amount.
  - Happy Box (`original_name LIKE '%HB%'`): the component whose name contains `HB` gets the full amount, the contents 0.
  - `original_units > 1`: `TRUNC(x / original_units)`.
  - otherwise unchanged.
- `occurrence_number = ROW_NUMBER() OVER (PARTITION BY control_number, order_id, remarks, original_name, CAST(real_amount AS INT64) ORDER BY item)` (the signed amount is in the partition to separate positive and negative lines).
- Remainder correction: on the last component (`occurrence_number = original_units`) of a bundle whose amount is not divisible by its units, the amount becomes `|verif_amount| - |real_amount x original_units| + real_amount` (positive lines) or the mirrored formula (negative lines), so components add up to the original line.
- `金額` = absolute corrected amount; `corrected_value` = signed corrected amount; `税額 = ABS(TRUNC(real_amount/13.5))` (8%) or `/11` (10%).
- `備考` = remarks cut at the first parenthesis (`Product Charges (#123-...)` -> `Product Charges`).
- `品目` = `CONCAT(item, ' ', item_name, ' (', JAN, ')')` when the item resolved to a JAN, else `item`.
- Output columns: 収支区分, 管理番号, 発生日, 支払期日, 取引先, 勘定科目, 税区分, 金額, 税計算区分, 税額, 備考, 品目, 部門, メモタグ, `units_total`, `occurrence_number`, `corrected_value`, `real_amount`, `amount_notrunc`, `original_units`, `verif_amount`, `order_id`. There is no 決済日 / 決済口座 / 決済金額 in the long output.

### `rakuten_short()`

- Source `FUJ_sheets.rakuten_long()` with `CAST(管理番号 AS INT64) >= 20230000`.
- `発生日` becomes `YYYY-MM-01` (STRING).
- Groups by 管理番号, month, 支払期日, 取引先, 勘定科目, 税区分, 税計算区分, 備考, 品目, 部門, メモタグ, 決済日, 決済口座.
- `収支区分 = '収入'` if `SUM(corrected_value) > 0` else `'支出'`; `金額 = 決済金額 = ABS(SUM(corrected_value))`; `税額 = ABS(ROUND(SUM(corrected_value)/13.5))` (8%) or `/11` (10%).
- `決済日` = NULL (filled by hand at export), `決済口座 = 'Rakuten'`.
- Output: the 17 freee columns plus `real_amount` (signed sum).

## 7. Export step

### Workbook `FUJ_2026_RAKUTEN`

| Tab | Type | Content |
|---|---|---|
| `RAK_L` | Connected (hidden) | `SELECT * FROM FUJ_sheets.rakuten_long() WHERE CAST(管理番号 AS INT64) > 20251200` |
| `RAK_S` | Connected (hidden) | `SELECT * FROM FUJ_sheets.rakuten_short() WHERE CAST(管理番号 AS INT64) >= 20251201` |
| `REP_L`, `REP_S` | Working | Pasted copies of RAK_L / RAK_S with per-settlement and per-品目 sums and a "diff corrected real" column |
| `PT` | Working | Reconciliation per 管理番号: totals from `REP_S`, from `Master` excluding `Fee Rakuten` / `Advertisement` rows, from `REP_L`, and the BillPay amount typed by hand; `Diff` must be 0. Which figure of the 楽天ペイ statement is typed as "BillPay" is to confirm with the owner |
| `check` | Working | For one 管理番号: per 品目, `REP_L` sum vs `Master` sum, difference 0 |
| `LAST` | Working (hidden) | Filter of `REP_L` for the latest 管理番号 |
| `202601` ... `202612` | Monthly | Final 17-column rows (sales from `RAK_S` plus fee rows, from `free.rakuten_cost_full` since 2026-11, before from `RAK_PAYMENT`) and a signed value column; downloaded as the freee CSV |
| `Master` | Working | QUERY union of monthly tabs, signed `real_value`, totals per 管理番号 |
| `01-06`, `07-12`, `202512_F` | Working | Half-year and carry-over tabs; purpose to confirm with the owner |

### 決済日 derivation

`rakuten_short()` returns 決済日 = NULL. At export 決済日 is set to the date written in 管理番号:

```sql
PARSE_DATE('%Y%m%d', `管理番号`) AS `決済日`   -- '20260925' -> 2026-09-25
```

Example from the `202609` tab: 管理番号 `20260825`, 決済日 `2026-08-25`, 発生日 `2026-07-01` / `2026-08-01`. Fee rows (`free.rakuten_cost_full`) get the same treatment.

### Fee rows: `free.rakuten_cost_full` (before 2026-11: Google Sheet `RAK_PAYMENT`)

Fee rows are read from `free.rakuten_cost_full` (section 5) and completed at export with 決済日 (from 管理番号), 決済口座 `Rakuten` and 決済金額 (= 金額), like the sales rows. The sheet procedure below is what `rakuten_cost_report()` reproduces. The 2026 statement files have 17 columns (発行日, 精算書No, 店舗別内訳書No, 店舗別ID, ＵＲＬ, 店舗名, 支払（税込額）, 請求（税抜額）, 請求（税額）, 支払/請求分類, 集約科目, 品目, 精算対象期間開始日, 精算対象期間終了日, 金額, うち消費税, 税率); the older layout pasted into the sheet had 16:

Tab `cost_calc`: the 楽天ペイ statement CSV is pasted in columns A..P:

| Col | Statement field |
|---|---|
| A | 精算書発行年月日 |
| B | 精算書番号 |
| C | 店舗別精算書番号 |
| D | 店舗別ID |
| E | ＵＲＬ |
| F | 店舗名 |
| G | 支払合計額 |
| H | 請求合計額 |
| I | 請求税抜額 |
| J | 請求税額 |
| K | 債権債務区分 |
| L | 集約科目名 |
| M | 精算対象期間開始日 |
| N | 精算対象期間終了日 |
| O | 金額 (tax exclusive) |
| P | 税率 |

Formulas build the freee columns in R..AE:

| Col | freee column | Formula logic |
|---|---|---|
| R | 収支区分 | blank when 請求合計額 (H) = 0 (these are the 楽天ペイ_決済金 and 楽天ﾍﾟｲ_ｸｰﾎﾟﾝ利用注文分支払額 payment lines, skipped); `支出` when H > 0; `収入` when H < 0 |
| S | 管理番号 | typed by hand: the deposit date `YYYYMMDD` the fees belong to |
| T | 発生日 | 精算対象期間終了日 (N) |
| U | 支払期日 | blank |
| V | 取引先 | `Rakuten` |
| W | 勘定科目 | VLOOKUP of 集約科目名 (L) in `cost_nomenclature`, column 5 |
| X | 税区分 | VLOOKUP column 4 |
| Y | 金額 | `ROUND(ABS(O) + IF(X <> "対象外", 0.1, 0) * ABS(O), 2)` (statement amounts are tax exclusive; 10% is added) |
| Z | 税計算区分 | `内税` |
| AA | 税額 | `ROUND(IF(X <> "対象外", 0.1, 0) * ABS(O), 2)` |
| AB | 備考 | VLOOKUP column 2 |
| AC | 品目 | VLOOKUP column 3 |
| AD, AE | Real value, Real value tax | signed Y and AA (negative for 支出) |

Example: 集約科目名 `ｼｽﾃﾑ利用料_PC`, 金額 29,801 (tax exclusive) -> 支出, 支払手数料, 課対仕入10%, 金額 32,781, 税額 2,980, 備考 `System Usage`, 品目 `Fee Rakuten`.

The rows with a 収支区分 for the deposit are copied into the monthly tab, completed with 決済日 (from 管理番号), 決済口座 `Rakuten` and 決済金額 (= 金額). Amounts can have 2 decimals from the formula, and the per-line rounding left the total 1-2 yen off the statement; `free.rakuten_cost_full` uses whole yen allocated to the statement's 消費税 line instead (section 4).

`Rakuten.cost_nomenclature` (seeded by `queries_bq/rakuten_cost/02_rakuten_cost_nomenclature.sql` from tab `cost_nomenclature` of the sheet) maps the statement 品目 (`subject_name`; called 集約科目名 in the older 16-column layout) to the freee columns: `subject_name`, `remarks` (備考), `item` (品目), `tax_distinction` (税区分), `account` (勘定科目), `translation`, `ADDED_DATETIME`. Full mapping (36 rows as provided by the owner on 2026-10-16, plus row 37 added on 2026-11-05):

| # | 集約科目名 | 備考 | 品目 | 税区分 | 勘定科目 |
|---|---|---|---|---|---|
| 1 | ｼｽﾃﾑ利用料_PC | System Usage | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 2 | ｽｰﾊﾟｰｱﾌｨﾘｴｲﾄ_成果報酬原資 | Achievement Reward Fund | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 3 | ｽｰﾊﾟｰｱﾌｨﾘｴｲﾄ_ｱﾌｨﾘｴｲﾄｼｽﾃﾑ利用料 | Affiliate System | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 4 | ﾌﾟﾗﾝ共通_ﾎﾟｲﾝﾄ付与料_PC | Point Grant | Fee Rakuten | 対象外 | 支払手数料 |
| 5 | ﾌﾟﾗﾝ共通_ﾎﾟｲﾝﾄ付与料_ﾓﾊﾞｲﾙ | Point Grant Mobile | Fee Rakuten | 対象外 | 支払手数料 |
| 6 | ﾌﾟﾗﾝ共通_ﾓｰﾙにおける取引の安全性･利便性向上のためのｼｽﾃﾑ利用料 | Affiliate System Usage | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 7 | ﾌﾟﾗﾝ共通_楽天ﾍﾟｲ利用料 | Rakuten Pay | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 8 | ｼｽﾃﾑ利用料_ﾓﾊﾞｲﾙ | System Usage Mobile | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 9 | ﾌﾟﾗﾝ共通_商品一括登録ｻｰﾋﾞｽ | Product Batch Registration | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 10 | 広告掲載料_楽天CPA広告 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 11 | 広告掲載料_楽天ﾌﾟﾛﾓｰｼｮﾝﾌﾟﾗｯﾄﾌｫｰﾑ | Advertisement | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 12 | 検索連動型広告(RPP)_広告掲載料 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 13 | 効果保証型広告(楽天CPA広告)_広告掲載料 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 14 | 消費税 | (no mapping: tax line, skipped) | | | |
| 15 | ｽﾀﾝﾀﾞｰﾄﾞﾌﾟﾗﾝ_出店料 | Initial Fee Shop Setting | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 16 | R-Mail_配信料 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 17 | 運用型ｸｰﾎﾟﾝ広告(ｸｰﾎﾟﾝｱﾄﾞﾊﾞﾝｽ広告)_広告掲載料 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 18 | 検索連動型広告（RPP）_控除分 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 19 | R-Mail_配信料_無料配信適用額 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 20 | ｽｰﾊﾟｰDEAL_広告掲載料_ﾓﾊﾞｲﾙ | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 21 | ｽｰﾊﾟｰDEAL_広告掲載料 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 22 | ｽｰﾊﾟｰDEAL_広告掲載料_ｸｰﾎﾟﾝ控除額 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 23 | 楽天市場／楽天ｸﾞﾙｰﾌﾟ広告_広告掲載料 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 24 | 楽天ｽｰﾊﾟｰﾛｼﾞｽﾃｨｸｽ_ｻｰﾋﾞｽ料金_課税対象額 | Fulfillment Fee | Delivery | 課対仕入10% | 荷造運賃 |
| 25 | 楽天ｽｰﾊﾟｰﾛｼﾞｽﾃｨｸｽ_ｻｰﾋﾞｽ料金_課税対象額【割引】 | Fulfillment Fee Discount | Delivery | 課対仕入10% | 荷造運賃 |
| 26 | 楽天ｽｰﾊﾟｰﾛｼﾞｽﾃｨｸｽ_ｻｰﾋﾞｽ料金_課税対象額（返還） | Fulfillment Fee Refund | Delivery | 課対仕入10% | 荷造運賃 |
| 27 | ｻｰﾋﾞｽｽｸｴｱ_BOSS_ｻｰﾋﾞｽ利用料 | Fulfillment Fee | Delivery | 課対仕入10% | 荷造運賃 |
| 28 | 出店料_差額請求 | Initial Fee Shop Setting | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 29 | R-SNS_月額固定費 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 30 | ｼｽﾃﾑ利用料_ｼｮｯﾌﾟｸｰﾎﾟﾝ | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 31 | ｼｽﾃﾑ利用料_ｼｮｯﾌﾟｸｰﾎﾟﾝ_値引き | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 32 | ｽｰﾊﾟｰｱﾌｨﾘｴｲﾄ_ｱﾄﾞﾊﾞﾝｽｻｰﾋﾞｽ料 | Affiliate System | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 33 | ｽｰﾊﾟｰｱﾌｨﾘｴｲﾄ_ｱﾄﾞﾊﾞﾝｽｻｰﾋﾞｽ料【割引】 | Affiliate System | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 34 | 定期購入･頒布会購入_ｼｽﾃﾑ利用料 | System Usage | Fee Rakuten | 課対仕入10% | 支払手数料 |
| 35 | R-SNS_LINE ﾗｲﾄﾌﾟﾗﾝ_月額費用 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 36 | 検索連動型広告-ｴｸｽﾊﾟﾝｼｮﾝ(RPP-EXP)_広告掲載料 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |
| 37 | 検索連動型広告(RPP)_控除分 | Advertisement | Advertisement | 課対仕入10% | 広告宣伝費 |

Notes for an automated pipeline:

- **Exact match:** the lookup is an exact match on 品目 (集約科目名). The statement uses half-width katakana and mixes half-width and full-width brackets (row 12 `(RPP)`, row 18 `（RPP）`), so do not normalise the strings. The 2026 statements write row 18 with half-width brackets; the sheet history maps that spelling exactly like row 18 (3 rows), so it is row 37 of the table.
- **Exception in the advertising group:** 広告掲載料_楽天ﾌﾟﾛﾓｰｼｮﾝﾌﾟﾗｯﾄﾌｫｰﾑ (row 11) is booked as 支払手数料 / `Fee Rakuten`, not as advertising.
- **Discount and refund rows** (【割引】, （返還）, 控除分, 無料配信適用額, 値引き) map to the same account as the charge they reduce. The sign comes from 請求合計額 (H): a negative H gives 収支区分 `収入`.
- **Unknown 品目:** a new value must be added to `Rakuten.cost_nomenclature` by hand. Until then its row gets `account = 'MISSING NOMENCLATURE {subject_name:...}'` and empty 税区分 / 備考 / 品目, and because the empty 税区分 is not `対象外`, 10% tax is added to it (the sheet did the same). After adding the row, delete the deposit from `free.rakuten_cost_full` and re-run the append query (README of the function).

### Reconciliation in BigQuery

Per deposit, payments must equal the report:

```sql
SUM(Rakuten.payment_main.rakuten_pay_money) + SUM(Rakuten.payment_coupon.coupon_amount)
  - SUM(free.rakuten_report_full.real_amount)   -- per report_id = control_number, expected 0
```

This check exists as `FUJ_test.check_rakuten_freee()` (test dataset). Whether the owner uses it or only the `PT` tab is to confirm with the owner.

Cost side: `FUJ_test.check_rakuten_cost()` (`queries_bq/rakuten_cost/06_check_rakuten_cost.sql`) compares, per deposit, the 請求 total of the statement with the rows in `free.rakuten_cost_full`. `diff_tax_excluded` and `diff_tax` (against the statement's 消費税 line) must both be 0 (`status` = `OK`); `rate_mismatch` counts cost lines whose statement 税率 disagrees with the nomenclature 税区分 (expected 0); `NOT MATERIALISED` means the statement has cost lines but no rows were appended; `NO COST LINES` is a statement with no 請求 line.

## 8. Known pitfalls and manual steps

- **Load order matters (append-once per deposit).** Load all orders of the period and the 楽天ペイ payment file before the Saturday 00:00 UTC run. Orders missing from `Rakuten.orders` when a deposit is first materialised are never added later; the deposit must then be removed from `free.rakuten_report_full` and rebuilt (procedure to confirm with the owner).
- **An order in two deposits** (for example a payment-method change, `決済変更`) gets its lines under both `report_id`, because the join takes every distinct (`order_number`, `report_id`) pair. In `payment_main`, 201 (`order_number`, `payment_number`, `report_id`) combinations occur more than once (208 surplus rows); whether these are expected is to confirm with the owner.
- **Fees are in BigQuery since 2026-11** (`free.rakuten_cost_full`); they still have to be copied into the monthly tab and completed with 決済日 / 決済口座 / 決済金額 like the sales rows. `PT` compares sales without fees against the BillPay amount.
- **Do not upload a Billpay statement before the function that routes on `_Shop_Billpay.csv` is deployed**: the previous code would read it as a payment file.
- **A statement is loaded once.** Uploading the same 精算書No again is refused; to reload, delete its lines from `Rakuten.statement` and the deposit from `free.rakuten_cost_full` first. Two statements with the same deposit date would share a `report_id` and only the first one's cost rows would be written.
- **New 品目 in a statement** gives a `MISSING NOMENCLATURE` row (kept, not dropped): add it to `Rakuten.cost_nomenclature` and rebuild the deposit.
- **Cost rows are whole yen** and their taxes add up to the statement's 消費税 line (section 4), unlike the sheet's per-line rounding; a line can therefore differ from the sheet by 1 yen. The sheet's 発生日 cell is blank for the 17-column statements (its formula seems tied to the old column layout); the routine uses 精算対象期間終了日 as documented.
- **決済日 is empty in BigQuery**; it must be derived from 管理番号 for every row, including fee rows.
- **New Rakuten item numbers or multi-variant listings** need a row in `free.sku_master` (store `Rakuten`) and, for variant listings, possibly a new remapping rule in `FUJ_extra.Rakuten_full()`.
- Cancelled orders (`status = '900'`) keep product lines; delivery and coupon lines are zeroed.
