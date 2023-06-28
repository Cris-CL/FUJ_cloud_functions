--- item_number query RAKUTEN
with rk as (
  select
  *,
  CASE
    WHEN remarks not like 'Product Charges%' THEN CAST(null AS INT64)
    ELSE CAST(quantity AS INT64)
  END as adj_quantity
  from `free.rakuten_report_full`)

select

Case
when SUM(real_amount) < 0 THEN '支出'
ELSE '収入'
END as balance,
control_number,
item_number,
DATE_TRUNC(order_date, MONTH) AS month, -- this keeps everything from the date exept the day so to group the monthly sales
account,
tax_distinction,
remarks,
item,
SUM(real_amount) as total_amount,
sum(adj_quantity) as total_quantity

from rk
group by
control_number,
item_number,
month,
account,
tax_distinction,
remarks,
item

order by control_number desc,item_number, month desc

-- select * from `free.rakuten_report_full`
