-- Reconciliation per deposit: the 請求 total of the Billpay statement against the cost rows
-- written to free.rakuten_cost_full. Both differences must be 0 (whole yen, tax allocated to the
-- statement's 消費税 line by rakuten_cost_report()).
--   diff_tax_excluded : statement 請求（税抜額） total - (cost rows tax included - cost rows tax)
--   diff_tax          : statement 消費税 line - sum of the cost rows' tax
--   rate_mismatch     : cost lines whose statement 税率 (0 / 10) disagrees with the nomenclature 税区分
--                       (対象外 / taxable); the tax follows the statement, the freee 税区分 the nomenclature
--   status            : OK / MISMATCH / NOT MATERIALISED (cost lines exist but no rows yet) / NO COST LINES
CREATE OR REPLACE TABLE FUNCTION `test-bigquery-cc.FUJ_test.check_rakuten_cost`()
AS
WITH statement_totals AS (
  SELECT
    st.report_id,
    st.statement_number,
    SUM(IF(st.payment_billing_category = '請求', st.billing_amount_tax_excluded, 0)) AS statement_tax_excluded,
    SUM(IF(st.payment_billing_category = '請求', st.billing_tax_amount, 0)) AS statement_tax,
    COUNTIF(st.billing_amount_tax_excluded <> 0) AS statement_cost_lines,
    COUNTIF(st.billing_amount_tax_excluded <> 0
            AND (IFNULL(st.tax_rate, 0) = 0) <> (nom.tax_distinction = '対象外')) AS rate_mismatch
  FROM
    `Rakuten.statement` AS st
  LEFT JOIN
    `Rakuten.cost_nomenclature` AS nom
  ON
    st.subject_name = nom.subject_name
  GROUP BY
    st.report_id,
    st.statement_number
),
cost_totals AS (
  SELECT
    control_number,
    -SUM(real_amount) AS cost_total,  -- net charge tax included, positive
    SUM(IF(balance = '支出', tax_amount, -tax_amount)) AS cost_tax,
    COUNT(*) AS cost_rows,
    COUNTIF(account LIKE 'MISSING NOMENCLATURE%') AS missing_nomenclature
  FROM
    `free.rakuten_cost_full`
  GROUP BY
    control_number
)
SELECT
  st.report_id,
  st.statement_number,
  st.statement_tax_excluded + st.statement_tax AS statement_total,
  ct.cost_total,
  st.statement_tax_excluded - (ct.cost_total - ct.cost_tax) AS diff_tax_excluded,
  st.statement_tax - ct.cost_tax AS diff_tax,
  st.statement_cost_lines,
  ct.cost_rows,
  ct.missing_nomenclature,
  st.rate_mismatch,
  CASE
    WHEN st.statement_cost_lines = 0 THEN 'NO COST LINES'
    WHEN ct.control_number IS NULL THEN 'NOT MATERIALISED'
    WHEN st.statement_tax_excluded - (ct.cost_total - ct.cost_tax) = 0
     AND st.statement_tax - ct.cost_tax = 0
     AND st.statement_cost_lines = ct.cost_rows THEN 'OK'
    ELSE 'MISMATCH'
  END AS status
FROM
  statement_totals AS st
LEFT JOIN
  cost_totals AS ct
ON
  st.report_id = ct.control_number
ORDER BY
  CAST(st.report_id AS INT64) DESC;
