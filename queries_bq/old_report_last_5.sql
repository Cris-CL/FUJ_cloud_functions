--- NEW AMAZON Q
--- Append
with sub as (
with tor as(
with tv as (
  select
CAST(settlement_id AS INT64) as settlement_id,
DATETIME_ADD(posted_date_time, interval 9 HOUR) as posted_date_time,
transaction_type,
amount_type,
amount_description,
sum(amount) as total_amount,
order_id,
sku,
Case
  When transaction_type in ('CouponRedemptionFee','Imaging Services') THEN concat(transaction_type,amount_type)
  ELSE concat(transaction_type,amount_type,amount_description)
End as concat_fields,
sum(quantity_purchased) as total_purchased,
from `Amazon.transaction_view_master`
group by

settlement_id,
posted_date_time,
transaction_type,
order_id,
amount_type,
amount_description,
sku
)
SELECT
tv.* except(posted_date_time),
tv.posted_date_time,
oc.purchase_date,

CASE ------REFUNDS APPEAR AT THE SETTLEMENT DATE NOW
  WHEN transaction_type = 'Refund' THEN tv.posted_date_time
  WHEN amount_type = 'FBA Inventory Reimbursement' THEN tv.posted_date_time
  WHEN oc.purchase_date is null THEN tv.posted_date_time
  ELSE oc.purchase_date
END AS main_date,
CASE
  WHEN transaction_type = 'Refund' THEN 'POSTED_DATE_REFUND'
  WHEN amount_type = 'FBA Inventory Reimbursement' THEN 'POSTED_DATE_REIMBURSEMENT'
  WHEN oc.purchase_date is null THEN 'POSTED_DATE'
  ELSE 'ORDER_DATE'
END AS DATE_TYPE

from tv
LEFT JOIN (SELECT DISTINCT
amazon_order_id,
DATETIME_ADD(purchase_date,INTERVAL 9 HOUR) as purchase_date
FROM `Amazon.order_central_master`) as oc
ON oc.amazon_order_id = tv.order_id

)

SELECT
  CASE
    WHEN sum(total_amount) < 0 THEN '支出'
    WHEN sum(total_amount) >= 0 THEN '収入'
  END AS balance,
  CAST(settlement_id AS STRING) as settlement_id,
  CAST(main_date AS DATE) as main_date,
  -- purchase_or_posted_date as payment_date,
  'Amazon Seller' as supplier,
  Case
    when account is null then 'MISSING NOMENCLATURE'
    ELSE account
  end as  account,
  tax_distiction,
  abs(sum(total_amount)) as total_sum,
  '内税' as tax_calc_distinction,
  remarks,
  item,
  null as department,
  null as memo,
  DATE_TYPE,
  sku,
  total_purchased,
  sum(total_amount) total_real,
  order_id,
  CAST(posted_date_time AS DATE) AS posted,
  main_date as full_date,
  CURRENT_DATE() AS DATE_OF_QUERY

FROM tor
LEFT JOIN temp_nomenclature.nomenclature_temp
    on tor.concat_fields = concatenated_fields
WHERE main_date is not null
GROUP BY
settlement_id,
main_date,
full_date,
account,
tax_distiction,
tax_calc_distinction,
remarks,
item,
department,
memo,
DATE_TYPE,
sku,
order_id,
posted_date_time,
total_purchased
)

SELECT
  CASE
    WHEN total_real < 0 THEN '支出'
    WHEN total_real >= 0 THEN '収入'
  END AS balance,
settlement_id,
cast(main_date as DATE) as main_date,
CAST(DATETIME_ADD(dat.deposit_date, INTERVAL 9 HOUR) AS DATE) as deposit_date,
sub.* except(balance,settlement_id,main_date,full_date),
dat.deposit_date as deposit_full_date,
full_date as order_full_date
from sub left join
(SELECT DISTINCT settlement_id as sett,deposit_date FROM Amazon.transaction_view_master where
      deposit_date is not null) as dat on sub.settlement_id = CAST(CAST(dat.sett AS INT64) AS STRING)

where settlement_id in (
  select distinct CAST(CAST(settlement_id AS INT64) AS STRING) as settlement_id
  from `Amazon.transaction_view_master` order by settlement_id desc limit 5
  )
order by settlement_id desc ,main_date asc
