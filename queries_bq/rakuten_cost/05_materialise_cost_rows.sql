-- Appends the freee cost rows of every deposit not yet in free.rakuten_cost_full (one deposit written
-- once; re-running appends nothing). Cloud Function payment-rakuten runs this same query right after
-- it lands a Billpay statement (main.py, materialise_cost_rows). Run it by hand only to re-trigger the
-- step, for example after a nomenclature fix and a DELETE of that deposit's rows.
INSERT INTO `test-bigquery-cc.free.rakuten_cost_full`
SELECT
  *
FROM
  FUJ_scheduled.rakuten_cost_report()
WHERE
  control_number IS NOT NULL AND amount <> 0
AND
  control_number NOT IN (
    SELECT DISTINCT
      control_number
    FROM
      `free.rakuten_cost_full`
    WHERE
      control_number IS NOT NULL
  );
