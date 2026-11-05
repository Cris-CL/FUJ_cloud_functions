-- freee cost rows built from the 楽天ペイ Billpay statement lines in Rakuten.statement.
-- Reproduces Google Sheet RAK_PAYMENT, tab cost_calc:
--   * only lines with 請求（税抜額） <> 0 (payment, 支払控除 and 消費税 lines are skipped)
--   * 収支区分: 支出 when 請求（税抜額） > 0, 収入 when < 0 (discounts, refunds, deductions)
--   * 発生日 = 精算対象期間終了日, 取引先 = Rakuten, 税計算区分 = 内税, 支払期日 = NULL
--   * 勘定科目 / 税区分 / 備考 / 品目 from Rakuten.cost_nomenclature, exact match on 品目 (subject_name)
--   * 金額 = ROUND(|金額| x 1.1, 2) and 税額 = ROUND(|金額| x 0.1, 2); no tax when 税区分 = 対象外.
--     An unknown 品目 gets 勘定科目 = MISSING NOMENCLATURE {...} and 10% tax, like the sheet; the row is kept.
-- Same column set as free.rakuten_report_full so both tables read the same way. The dedup by
-- control_number is done by scheduled query FUJ_Rakuten_cost_freee, not here, so the function can be
-- used to re-check a deposit after it was materialised.
-- 決済日 is not stored (as for the sales rows): at export it is PARSE_DATE('%Y%m%d', control_number).
CREATE OR REPLACE TABLE FUNCTION `test-bigquery-cc.FUJ_scheduled.rakuten_cost_report`()
AS
WITH cost_lines AS (
  SELECT
    st.report_id,
    st.statement_number,
    st.subject_name,
    st.period_end_date,
    st.billing_amount_tax_excluded,
    ABS(st.amount) AS base_amount,
    nom.remarks,
    nom.item,
    nom.tax_distinction,
    nom.account,
    -- NULL tax_distinction (unknown 品目) -> 0.1, as the sheet does
    IF(nom.tax_distinction = '対象外', 0, 0.1) AS tax_rate
  FROM
    `Rakuten.statement` AS st
  LEFT JOIN
    `Rakuten.cost_nomenclature` AS nom
  ON
    st.subject_name = nom.subject_name
  WHERE
    st.billing_amount_tax_excluded IS NOT NULL
    AND st.billing_amount_tax_excluded <> 0
)
SELECT
  IF(billing_amount_tax_excluded < 0, '収入', '支出') AS balance,
  report_id AS control_number,
  period_end_date AS order_date,
  CAST(NULL AS INT64) AS payment_date,
  'Rakuten' AS supplier,
  IFNULL(account, CONCAT('MISSING NOMENCLATURE {subject_name:', subject_name, '}')) AS account,
  tax_distinction,
  ROUND(base_amount * (1 + tax_rate), 2) AS amount,
  '内税' AS tax_calculation_distinction,
  ROUND(base_amount * tax_rate, 2) AS tax_amount,
  remarks,
  item,
  statement_number AS order_id,
  CAST(NULL AS STRING) AS item_number,
  CAST(NULL AS FLOAT64) AS quantity,
  CAST(period_end_date AS DATETIME) AS full_date,
  CAST(NULL AS STRING) AS zip_code,
  -- signed like the sheet's "Real value": negative for 支出, positive for 収入
  IF(billing_amount_tax_excluded < 0, 1, -1) * ROUND(base_amount * (1 + tax_rate), 2) AS real_amount,
  subject_name AS product_name
FROM
  cost_lines
ORDER BY
  CAST(report_id AS INT64) DESC,
  period_end_date;
