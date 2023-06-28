--- SKU query SHOPIFY
with sh as (
  select
  *,
  CASE
    WHEN remarks not like 'Product Charges%' or product in (
                        'Shipping',
                        'Discount',
                        'Chargeback',
                        'Handling Fee',
                        'partial refund') THEN CAST(null AS INT64)
    ELSE CAST(product_count AS INT64)
  END as adj_quantity,
  CASE
    WHEN remarks like 'Product Charges%' THEN 'Product Charges'
    WHEN remarks like 'Delivery%' THEN 'Delivery'
    WHEN remarks like 'Fee%' THEN 'Fee'
  END as adj_remarks,
  CASE WHEN product in ('Shipping',
                        'Discount',
                        'Chargeback',
                        'Handling Fee',
                        'partial refund') THEN product
      ELSE null
  END AS adj_product



  from `free.shopify_freee_full`)

select

Case
when SUM(real_amount) < 0 THEN '支出'
ELSE '収入'
END as balance,
control_number,
sku,
DATE_TRUNC(transaction_date, MONTH) AS month, -- this keeps everything from the date exept the day so to group the monthly sales
account,
tax_distinction,
adj_remarks as remarks,
item,
SUM(real_amount) as total_amount,
sum(adj_quantity) as total_quantity,
adj_product as product

from sh
group by
control_number,
sku,
month,
account,
tax_distinction,
remarks,
item,
product

order by control_number desc,sku, month desc
