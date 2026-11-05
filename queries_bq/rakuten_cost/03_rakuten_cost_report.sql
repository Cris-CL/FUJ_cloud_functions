-- freee cost rows built from the 楽天ペイ Billpay statement lines in Rakuten.statement.
-- Follows Google Sheet RAK_PAYMENT, tab cost_calc, for the line selection and the mapping:
--   * only lines with 請求（税抜額） <> 0 (payment, 支払控除 and 消費税 lines are skipped)
--   * 収支区分: 支出 when 請求（税抜額） > 0, 収入 when < 0 (discounts, refunds, deductions)
--   * 発生日 = 精算対象期間終了日, 取引先 = Rakuten, 税計算区分 = 内税, 支払期日 = NULL
--   * 勘定科目 / 税区分 / 備考 / 品目 from Rakuten.cost_nomenclature, exact match on 品目 (subject_name).
--     An unknown 品目 gets 勘定科目 = MISSING NOMENCLATURE {...}; the row is kept.
-- Tax, whole yen, reconciled to the statement (differs from the sheet, which rounded each line and was
-- 1-2 yen off the statement):
--   * taxable = the statement's own 税率 column (10 -> 10%, 0 -> none); when the file gives no 税率 (-),
--     the nomenclature decides (税区分 対象外 -> none, anything else -> 10%), as the sheet did
--   * Rakuten invoices one 消費税 line computed on the statement total (truncated). Each line gets
--     FLOOR(税抜 x 税率) and the remaining yen go, one each, to the lines with the largest fractional
--     parts (largest remainder) until the per-line taxes add up to that 消費税 line. When a statement
--     has no 請求 消費税 line, the target is TRUNC(sum of exact taxes), which is what Rakuten computes.
--   * 金額 = |税抜 + tax|, 税額 = |tax|; SUM(real_amount) per deposit = -(請求 税抜 total + 消費税).
-- Same column set as free.rakuten_report_full so both tables read the same way. The dedup by
-- control_number is done by the append step (05_materialise_cost_rows.sql, run by the Cloud Function
-- after each statement load), not here, so the function can be used to re-check a deposit after it
-- was materialised.
-- 決済日 is not stored (as for the sales rows): at export it is PARSE_DATE('%Y%m%d', control_number).
CREATE OR REPLACE TABLE FUNCTION `test-bigquery-cc.FUJ_scheduled.rakuten_cost_report`()
AS
WITH cost_lines AS (
  SELECT
    st.statement_number,
    st.report_id,
    st.subject_name,
    st.period_end_date,
    st.billing_amount_tax_excluded,
    CAST(st.amount AS NUMERIC) AS base_amount,                 -- signed, tax exclusive
    CASE
      WHEN st.tax_rate IS NOT NULL THEN CAST(st.tax_rate AS NUMERIC) / 100
      WHEN nom.tax_distinction = '対象外' THEN 0
      ELSE 0.1
    END AS rate,
    nom.remarks,
    nom.item,
    nom.tax_distinction,
    nom.account
  FROM
    `Rakuten.statement` AS st
  LEFT JOIN
    `Rakuten.cost_nomenclature` AS nom
  ON
    st.subject_name = nom.subject_name
  WHERE
    st.billing_amount_tax_excluded IS NOT NULL
    AND st.billing_amount_tax_excluded <> 0
),
invoiced_tax AS (
  -- the 消費税 line of the 請求 side: the tax Rakuten actually invoices for the statement
  SELECT
    statement_number,
    CAST(SUM(billing_tax_amount) AS NUMERIC) AS invoiced_tax
  FROM
    `Rakuten.statement`
  WHERE
    payment_billing_category = '請求'
    AND subject_name = '消費税'
  GROUP BY
    statement_number
),
exact AS (
  SELECT
    c.*,
    c.base_amount * c.rate AS exact_tax,
    FLOOR(c.base_amount * c.rate) AS floor_tax,
    c.base_amount * c.rate - FLOOR(c.base_amount * c.rate) AS remainder
  FROM
    cost_lines AS c
),
targets AS (
  SELECT
    e.statement_number,
    SUM(e.floor_tax) AS floor_sum,
    IFNULL(MAX(i.invoiced_tax), TRUNC(SUM(e.exact_tax))) AS target_tax
  FROM
    exact AS e
  LEFT JOIN
    invoiced_tax AS i
  USING (statement_number)
  GROUP BY
    e.statement_number
),
allocated AS (
  -- largest remainder: the (target - floor_sum) lines with the biggest fractional parts get +1 yen
  SELECT
    e.*,
    e.floor_tax + IF(
      ROW_NUMBER() OVER (
        PARTITION BY e.statement_number
        ORDER BY e.remainder DESC, ABS(e.base_amount) DESC, e.subject_name
      ) <= t.target_tax - t.floor_sum, 1, 0) AS line_tax
  FROM
    exact AS e
  JOIN
    targets AS t
  USING (statement_number)
)
SELECT
  IF(billing_amount_tax_excluded < 0, '収入', '支出') AS balance,
  report_id AS control_number,
  period_end_date AS order_date,
  CAST(NULL AS INT64) AS payment_date,
  'Rakuten' AS supplier,
  IFNULL(account, CONCAT('MISSING NOMENCLATURE {subject_name:', subject_name, '}')) AS account,
  tax_distinction,
  CAST(ABS(base_amount + line_tax) AS FLOAT64) AS amount,
  '内税' AS tax_calculation_distinction,
  CAST(ABS(line_tax) AS FLOAT64) AS tax_amount,
  remarks,
  item,
  statement_number AS order_id,
  CAST(NULL AS STRING) AS item_number,
  CAST(NULL AS FLOAT64) AS quantity,
  CAST(period_end_date AS DATETIME) AS full_date,
  CAST(NULL AS STRING) AS zip_code,
  -- signed like the sheet's "Real value": negative for 支出, positive for 収入
  CAST(-(base_amount + line_tax) AS FLOAT64) AS real_amount,
  subject_name AS product_name
FROM
  allocated
ORDER BY
  CAST(report_id AS INT64) DESC,
  period_end_date;
