-- Reconciliation per deposit: the 請求 total of the Billpay statement against the cost rows
-- written to free.rakuten_cost_full.
--   diff_tax_excluded : expected 0 (every cost line of the statement is materialised once)
--   diff_tax          : the statement computes the 10% tax on the total while the cost rows carry
--                       ROUND(x 0.1, 2) per line; a difference below 1 yen is rounding, not a miss
--   status            : OK / MISMATCH / NOT MATERIALISED (cost lines exist but no rows yet) / NO COST LINES
CREATE OR REPLACE TABLE FUNCTION `test-bigquery-cc.FUJ_test.check_rakuten_cost`()
AS
WITH statement_totals AS (
  SELECT
    report_id,
    statement_number,
    SUM(IF(payment_billing_category = '請求', billing_amount_tax_excluded, 0)) AS statement_tax_excluded,
    SUM(IF(payment_billing_category = '請求', billing_tax_amount, 0)) AS statement_tax,
    COUNTIF(billing_amount_tax_excluded <> 0) AS statement_cost_lines
  FROM
    `Rakuten.statement`
  GROUP BY
    report_id,
    statement_number
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
  ROUND(st.statement_tax_excluded - (ct.cost_total - ct.cost_tax), 2) AS diff_tax_excluded,
  ROUND(st.statement_tax - ct.cost_tax, 2) AS diff_tax,
  st.statement_cost_lines,
  ct.cost_rows,
  ct.missing_nomenclature,
  CASE
    WHEN st.statement_cost_lines = 0 THEN 'NO COST LINES'
    WHEN ct.control_number IS NULL THEN 'NOT MATERIALISED'
    WHEN ROUND(st.statement_tax_excluded - (ct.cost_total - ct.cost_tax), 2) = 0
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
