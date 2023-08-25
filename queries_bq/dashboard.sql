----- Scheduled dashboard
----- Updated 08-22 added taxes columns
with full_data as (---- AMAZON PART ----
SELECT
order_full_date as transaction_date,
total_real as amount,
CASE
  WHEN remarks in ('Product Charges','Product Charges Refund','Product Charges Inventory Reimbursement') THEN total_purchased
  ELSE 0
END as quantity,
sku,
'Amazon' as SOURCE,
ship_postal_code as zip_code,
item,
settlement_id,
account,
case
  when tax_distiction like '%8%' then 0.08
  WHEN tax_distiction like '%10%' then 0.1
  ELSE 0
END AS tax_multip

from `free.amazon_report_full` as rf
left join
(
  SELECT
    DISTINCT -- DISTINCT clause for picking only one order_id/purchase_date pair otherwise there are duplicates that change the totals
    ship_postal_code,
    CASE
      WHEN merchant_order_id IS NOT NULL THEN merchant_order_id
    ELSE
    amazon_order_id
  END
    AS amazon_order_id,
    DATETIME_ADD(purchase_date,INTERVAL 9 HOUR) AS purchase_date
  FROM
    `Amazon.order_central_master`
  WHERE
    amazon_order_id IS NOT NULL
) AS oc on rf.order_id = oc.amazon_order_id

---- RAKUTEN PART ----
UNION ALL
select
full_date as transaction_date,
real_amount as amount,
quantity,
item_number as sku,
'Rakuten' as SOURCE,
zip_code,
item,
control_number as settlement_id,
account,
case
  when tax_distinction like '%8%' then 0.08
  WHEN tax_distinction like '%10%' then 0.1
  ELSE 0
END AS tax_multip

from `free.rakuten_report_full`
---- SHOPIFY PART ----
UNION ALL
(with sho as (SELECT
-- SA
CAST(real_date as DATETIME) as transaction_date,
real_amount as amount,
product_count as quantity,
sku,
'Shopify' as SOURCE,
item,
control_number as settlement_id,
order_number,
account,
case
  when tax_distinction like '%8%' then 0.08
  WHEN tax_distinction like '%10%' then 0.1
  ELSE 0
END AS tax_multip

from `free.sh_ama_freee_full`

-- SS
UNION ALL
SELECT
CAST(real_date as DATETIME) as transaction_date,
real_amount as amount,
product_count as quantity,
sku,
'Shopify' as SOURCE,
item,
SETT_NUMBER as settlement_id,
order_number,
account,
case
  when tax_distinction like '%8%' then 0.08
  WHEN tax_distinction like '%10%' then 0.1
  ELSE 0
END AS tax_multip
from `free.stripe_freee_full`

-- SH
UNION ALL
SELECT
CAST(real_date as DATETIME) as transaction_date,
real_amount as amount,
product_count as quantity,
sku,
'Shopify' as SOURCE,
item,
CAST(control_number as STRING) as settlement_id,
order_number,
account,
case
  when tax_distinction like '%8%' then 0.08
  WHEN tax_distinction like '%10%' then 0.1
  ELSE 0
END AS tax_multip
from `free.shopify_freee_full`
)
select

transaction_date,
amount,
quantity,
sku,
SOURCE,
ship_zip as zip_code,
item,
settlement_id,
account,
tax_multip

from sho LEFT join (SELECT DISTINCT name,ship_zip FROM `Shopify.orders_master`) as om
on sho.order_number = om.name
)
ORDER BY transaction_date desc, settlement_id,item
)

select
full_data.* except(account,quantity,tax_multip),
sk.item_name,
account,
CASE
  WHEN item <> 'Sales' THEN 0
  WHEN account = '売上高' and sku is not null and item_name is not null and amount > 0 THEN quantity
  WHEN account = '売上高' and sku is not null and item_name is not null and amount < 0 THEN -quantity
  ELSE 0
END AS quantity,
tax_multip,
ROUND(amount/(1+tax_multip)) as amount_no_tax,
amount - ROUND(amount/(1+tax_multip)) as amount_tax
from full_data LEFT JOIN
`Datastudio.combined_sku` as sk on full_data.sku = sk.sku_item and sk.store_name = full_data.SOURCE
where amount <> 0
