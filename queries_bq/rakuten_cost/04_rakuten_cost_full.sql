-- Materialised freee cost rows (one deposit written once), same column set as free.rakuten_report_full.
-- Written by Cloud Function payment-rakuten after each statement load (05_materialise_cost_rows.sql). Run once.
CREATE TABLE IF NOT EXISTS `test-bigquery-cc.free.rakuten_cost_full`
(
  balance                     STRING   OPTIONS (description = '収支区分'),
  control_number              STRING   OPTIONS (description = '管理番号 = deposit report_id YYYYMMDD'),
  order_date                  DATE     OPTIONS (description = '発生日 = 精算対象期間終了日'),
  payment_date                INT64    OPTIONS (description = '支払期日 (always NULL)'),
  supplier                    STRING   OPTIONS (description = '取引先 (Rakuten)'),
  account                     STRING   OPTIONS (description = '勘定科目, or MISSING NOMENCLATURE {...} for an unknown 品目'),
  tax_distinction             STRING   OPTIONS (description = '税区分'),
  amount                      FLOAT64  OPTIONS (description = '金額 tax included, absolute'),
  tax_calculation_distinction STRING   OPTIONS (description = '内税'),
  tax_amount                  FLOAT64  OPTIONS (description = '税額'),
  remarks                     STRING   OPTIONS (description = '備考'),
  item                        STRING   OPTIONS (description = '品目 (freee)'),
  order_id                    STRING   OPTIONS (description = '精算書No of the statement'),
  item_number                 STRING   OPTIONS (description = 'NULL for cost rows'),
  quantity                    FLOAT64  OPTIONS (description = 'NULL for cost rows'),
  full_date                   DATETIME OPTIONS (description = '発生日 as DATETIME'),
  zip_code                    STRING   OPTIONS (description = 'NULL for cost rows'),
  real_amount                 FLOAT64  OPTIONS (description = 'Signed amount: negative for 支出'),
  product_name                STRING   OPTIONS (description = '品目 of the Billpay statement (subject_name)')
)
OPTIONS (
  description = 'freee cost rows from the 楽天ペイ Billpay statements, appended once per control_number by Cloud Function payment-rakuten (after each statement load) from FUJ_scheduled.rakuten_cost_report(). Kept apart from free.rakuten_report_full, whose dedup by control_number would skip cost rows of deposits already written.'
);
