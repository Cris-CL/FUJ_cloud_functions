--- rakuten_report_full
--- new rakuten scheduled 2020-23
--- Query now only appends new values

With sub_2 as (
WITH sub_query_1 as
(
  ------- join with rakuten 2020 -------
  with full_rak as (
    SELECT
  order_number,
  order_date_time as order_date,
  status,
  product_name,
  item_number,
  unit_price,
  quantity,
  shipping_total,
  store_issued_coupon_usage_amount,
  CASE
  WHEN CHAR_LENGTH(mailing_address_postal_code_2) < 4
    THEN  CONCAT(destination_postal_code_1,'-',repeat("0", 4 - CHAR_LENGTH(mailing_address_postal_code_2)),mailing_address_postal_code_2)
  ELSE CONCAT(destination_postal_code_1,'-',mailing_address_postal_code_2)
  END AS zip_code,
FROM
  `Rakuten.test_raku`

union all
SELECT
  order_number,
  CAST(order_date_time as DATETIME) as order_date,
  status,
  product_name,
  item_number,
  unit_price,
  quantity,
  shipping_total,
  store_issued_coupon_usage_amount,
    CASE
  WHEN CHAR_LENGTH(mailing_address_postal_code_2) < 4
    THEN  CONCAT(destination_postal_code_1,'-',repeat("0", 4 - CHAR_LENGTH(mailing_address_postal_code_2)),mailing_address_postal_code_2)
  ELSE CONCAT(destination_postal_code_1,'-',mailing_address_postal_code_2)
  END AS zip_code,
FROM `Rakuten.rak_2020`
  )
  ------- join with rakuten 2020 -------
SELECT

  order_number, -- usado
  order_date,
  status,
  shipping_total as delivery,

  product_name,

  CASE
    WHEN item_number like 'v_%' then CAST(substr(item_number, 3 ,13) AS STRING)
    ELSE CAST(LEFT(item_number,13) AS STRING)
  END AS item_number,

  unit_price,
  quantity,
  unit_price*quantity as unit_per_price,
  zip_code

FROM full_rak

UNION ALL
SELECT distinct
  order_number, -- usado
  order_date,
  status,
  shipping_total as delivery,
  'delivery' as product_name,
  cast(null AS STRING) as item_number,
  shipping_total as unit_price,
  1 AS quantity,
  shipping_total as unit_per_price,
  zip_code

FROM full_rak
Where shipping_total >0
UNION ALL
SELECT distinct
  order_number, -- usado
  order_date,
  status,
  null as delivery,
  'points' as product_name,
  cast(null AS STRING) as item_number,
  -store_issued_coupon_usage_amount as unit_price,
  1 AS quantity,
  -store_issued_coupon_usage_amount as unit_per_price,
  zip_code

FROM full_rak
Where store_issued_coupon_usage_amount >0

)
--- sub with delivery cost

SELECT

CASE
  WHEN unit_per_price >= 0 THEN '収入'
  WHEN unit_per_price < 0 THEN '支出'
  ELSE 'Unknown value'
END AS balance, ---


report_id as control_number,
CAST(order_date AS DATE) as order_date,
null as payment_date,
-- CAST(set2.payment_due_date AS DATE) as payment_date,

'Rakuten' as supplier,

'売上高' as account,

CASE

  WHEN product_name = 'delivery' THEN '課税売上10%'
  WHEN product_name like 'GoCLN（ゴークリーン） シェイカー%' THEN '課税売上10%'
  WHEN product_name <> 'delivery' THEN '課税売上8%（軽）'

END AS tax_distinction,

CASE
WHEN status = '900' and product_name in ('delivery','points') THEN 0
-- WHEN status = '900' and product_name = 'delivery' THEN 0 ---- old version
ELSE abs(unit_per_price)
END AS amount,

'内税' as tax_calculation_distinction,

CASE
-------------------- down round the value from amount
  WHEN product_name = 'delivery' THEN abs(TRUNC(unit_per_price*0.1))
  WHEN product_name like 'GoCLN（ゴークリーン） シェイカー%' THEN ABS(TRUNC(unit_per_price*0.1))
  WHEN product_name <> 'delivery' THEN ABS(TRUNC(unit_per_price*0.08))

END AS tax_amount,

CASE
    WHEN product_name = 'delivery' THEN CONCAT('Delivery (#',sub_query_1.order_number,')')
    WHEN product_name = 'points' THEN CONCAT('Points issued (#',sub_query_1.order_number,')')
    WHEN product_name not in ('delivery' , 'points') THEN CONCAT('Product Charges (#',sub_query_1.order_number,')')
END AS remarks,

CASE

  WHEN product_name = 'delivery' THEN 'Delivery'
  WHEN product_name <> 'delivery' THEN 'Sales'

END AS item,
sub_query_1.order_number as order_id,
item_number,
quantity,
order_date as full_date,
zip_code,
CASE
WHEN status = '900' and product_name in ('delivery','points') THEN 0
-- WHEN status = '900' and product_name = 'delivery' THEN 0 ---- old version
ELSE unit_per_price
END AS real_amount,
product_name
from sub_query_1 LEFT JOIN (SELECT distinct order_number, report_id
FROM `test-bigquery-cc.Rakuten.payment_main`) as pay on sub_query_1.order_number = pay.order_number

order by order_date desc
)
-- 10482

select * from sub_2
where control_number is not null and amount <> 0
and control_number not in (
  SELECT distinct control_number FROM `free.rakuten_report_full`
  where control_number is not null
  )
