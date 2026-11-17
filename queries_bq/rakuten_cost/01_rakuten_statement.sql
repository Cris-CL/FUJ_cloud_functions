-- Landing table for the 楽天ペイ Billpay statement (精算書) CSV, one row per CSV line.
-- Loaded by Cloud Function payment-rakuten (rakuten/payment_rakuten/main.py) from
-- *_Shop_Billpay.csv uploads to bucket rakuten_payment_raw. Run once.
CREATE TABLE IF NOT EXISTS `test-bigquery-cc.Rakuten.statement`
(
  issue_date                  DATE    OPTIONS (description = '発行日'),
  statement_number            STRING  OPTIONS (description = '精算書No (one per file; the loader refuses a number already present)'),
  store_statement_number      STRING  OPTIONS (description = '店舗別内訳書No'),
  store_id                    STRING  OPTIONS (description = '店舗別ID'),
  url                         STRING  OPTIONS (description = 'ＵＲＬ'),
  store_name                  STRING  OPTIONS (description = '店舗名'),
  payment_amount_tax_included FLOAT64 OPTIONS (description = '支払（税込額）'),
  billing_amount_tax_excluded FLOAT64 OPTIONS (description = '請求（税抜額）; cost lines are the rows where this is not 0'),
  billing_tax_amount          FLOAT64 OPTIONS (description = '請求（税額）; only the 消費税 line has a value'),
  payment_billing_category    STRING  OPTIONS (description = '支払/請求分類: 支払, 支払控除 or 請求'),
  aggregate_subject           STRING  OPTIONS (description = '集約科目'),
  subject_name                STRING  OPTIONS (description = '品目 (集約科目名 in the older 16-column layout); exact-match key of Rakuten.cost_nomenclature'),
  period_start_date           DATE    OPTIONS (description = '精算対象期間開始日'),
  period_end_date             DATE    OPTIONS (description = '精算対象期間終了日; becomes 発生日 of the cost row'),
  amount                      FLOAT64 OPTIONS (description = '金額 (tax exclusive)'),
  consumption_tax             FLOAT64 OPTIONS (description = 'うち消費税'),
  tax_rate                    FLOAT64 OPTIONS (description = '税率 (10 or 0; NULL where the file has -)'),
  report_id                   STRING  OPTIONS (description = 'Deposit date YYYYMMDD = 精算対象期間終了日 of the 楽天ﾍﾟｲ_決済金* line; same key as Rakuten.payment_main.report_id'),
  FILE_NAME                   STRING  OPTIONS (description = 'Object name in bucket rakuten_payment_raw')
)
OPTIONS (
  description = '楽天ペイ Billpay statement (精算書) lines, one row per CSV line. Loaded by Cloud Function payment-rakuten from *_Shop_Billpay.csv uploads. Dates and amounts are NULL where the CSV has "-".'
);
