--- SKU query AMAZON
with am as (
  select
  *,
  CASE
    WHEN remarks <> 'Product Charges' THEN CAST(null AS INT64)
    ELSE CAST(total_purchased AS INT64)
  END as adj_quantity
  from `free.amazon_report_full`)

select

Case
when SUM(total_real) < 0 THEN '支出'
ELSE '収入'
END as balance,
settlement_id,
sku,
DATE_TRUNC(main_date, MONTH) AS month, -- this keeps everything from the date exept the day so to group the monthly sales
account,
tax_distiction,
remarks,
item,
SUM(total_real) as total_amount,
sum(adj_quantity) as total_quantity

from am
group by
settlement_id,
sku,
month,
account,
tax_distiction,
remarks,
item

order by settlement_id desc,sku, month desc
