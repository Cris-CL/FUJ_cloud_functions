---- scheduled SS report
---- Append
WITH dtim as (
WITH shopify_filtered AS (
------ START FILTERING ------
SELECT
  name AS order_number,
  email AS mail,
  Datetime_add(processed_at,interval 9 HOUR) AS date_transaction,
  ROUND((lineitem_quantity*lineitem_price)/1.08) AS subtotal,
  lineitem_quantity*lineitem_price - ROUND((lineitem_quantity*lineitem_price)/1.08) AS tax,
  lineitem_quantity*lineitem_price AS total,
  lineitem_quantity AS product_count,
  lineitem_name AS product,
  processing_method,
  lineitem_sku as sku

FROM `test-bigquery-cc.Shopify.orders_master` WHERE payment_gateway_names = 'stripe'

UNION ALL
SELECT distinct
  name AS order_number,
  email AS mail,
  Datetime_add(processed_at,interval 9 HOUR) AS date_transaction,
  ROUND(shipping_shop_amount - shipping_shop_amount/11) AS subtotal,
  ROUND(shipping_shop_amount/11) AS tax,
  shipping_shop_amount AS total,
  1 AS product_count,
  'Shipping' AS product,
  processing_method,
  CAST(NULL as STRING) as sku
FROM `test-bigquery-cc.Shopify.orders_master` WHERE payment_gateway_names = 'stripe' and shipping_shop_amount >0

UNION ALL
SELECT distinct
  name AS order_number,
  email AS mail,
  Datetime_add(processed_at,interval 9 HOUR) AS date_transaction,
  total_tax AS subtotal,
  ROUND(shipping_shop_amount/11) AS tax,
  total_tax AS total,
  1 AS product_count,
  'Tax not included' AS product,
  processing_method,
  CAST(NULL as STRING) as sku
FROM `test-bigquery-cc.Shopify.orders_master` WHERE payment_gateway_names = 'stripe' and total_tax >0

UNION ALL
SELECT distinct
  name AS order_number,
  email AS mail,
  Datetime_add(processed_at,interval 9 HOUR) AS date_transaction,
  -ROUND(discount_amount - discount_amount/11) AS subtotal,
  -ROUND(discount_amount/11) AS tax,
  -discount_amount AS total,
  1 AS product_count,
  'Discount' AS product,
  processing_method,
  CAST(NULL as STRING) as sku

FROM `test-bigquery-cc.Shopify.orders_master` WHERE payment_gateway_names = 'stripe' and discount_amount <> 0

UNION ALL
SELECT distinct
  name AS order_number,
  email AS mail,
  Datetime_add(processed_at,interval 9 HOUR) AS date_transaction,
  -(ROUND(subtotal_price*0.036)+ROUND(subtotal_price*0.01)) AS subtotal,
  0 AS tax,
  -(ROUND(total_price*0.036)+ROUND(total_price*0.01)) AS total,
  1 AS product_count,
  'Handling Fee' AS product,
  processing_method,
  CAST(NULL as STRING) as sku

FROM `test-bigquery-cc.Shopify.orders_master`
WHERE name in (SELECT name FROM `test-bigquery-cc.Shopify.orders_master` WHERE payment_gateway_names = 'stripe')


order by order_number desc)
------　FINISH FILTERING ------
SELECT

SUBSTR(pay.automatic_payout_id,5,20) AS control_number, -- 管理番号 column
CASE
  WHEN pay.reporting_category <> "charge" THEN CAST(pay.created_utc AS DATE)
  ELSE CAST(date_transaction as DATE)
END AS accrual_date, -- 発生日 column ---- now takes the refunds on the date that were refunded

null AS settlement_date, -- 決済期日 column

'Shopify Stripe' AS suppliers, -- 取引先 column

CASE
  WHEN product = 'Handling Fee' THEN '支払手数料'
  Else '売上高'
End as account, -- 勘定科目 column

CASE
  when reporting_category = 'refund' then '課税売上8%（軽）'
  when reporting_category = 'fee' then '対象外'
  WHEN product in ('Shipping',
                  'GoCLN シェイカー'
                  ) THEN '課税売上10%'
  WHEN product like '%シェイカー%' THEN '課税売上10%'
  WHEN product = 'Handling Fee' THEN '対象外'
  ELSE '課税売上8%（軽）' --- previous '課税売上8%'
END AS tax_distinction, ----- 税区分 column
CASE
  WHEN pay.reporting_category = 'refund' and product = 'Handling Fee' then 0 ---CONTINUAR ACA ACON LOS CASOS PARA QUE SEA NEGATIVO EL REFUND
  WHEN pay.reporting_category = 'fee' and product = 'Handling Fee' then ABS(pay.fee)
  WHEN pay.reporting_category = 'refund' and product <> 'Handling Fee' then ABS(pay.net)
  ELSE ABS(total)
end as amount, -- 金額  column

'内税' as tax_calculation_distinction, -- 税計算区分 column

CASE
  WHEN product in ('Shipping','GoCLN シェイカー') THEN ROUND(ABS(total*0.1))
  WHEN product like '%シェイカー%' THEN ROUND(ABS(total*0.1))
  WHEN product = 'Handling Fee' THEN 0
  ELSE ROUND(ABS(total*0.08))
END as tax_total, ---- 税額 column

CASE
  WHEN product = 'Shipping' THEN CONCAT('Delivery (',order_number,')')
  WHEN product = 'Handling Fee' THEN CONCAT('Fee (',order_number,')')
  WHEN product = 'Discount' Then CONCAT('Product Charges Discount (',order_number,')')
  WHEN product not in ('Shipping','Handling Fee','Discount') Then CONCAT('Product Charges (',order_number,')')
END AS remarks, ---- 備考 column

CASE
  WHEN product = 'Shipping' THEN 'Delivery'
  WHEN product = 'Handling Fee' THEN 'Fee Shopify'
  WHEN product not in ('Shipping','Handling Fee') Then 'Sales'
END AS item, ---- 品目 column

null as department, --- 部門 column
null as memo_tag, ----- メモタグ（複数指定可、カンマ区切り） column

FORMAT_DATE("%Y-%m-%d",CAST(pay.automatic_payout_effective_at AS DATE)) as date_3, ---- 決済日 column

'Shopify' as settlement_account,  ----- 決済口座 column

CASE
  WHEN pay.reporting_category = 'refund' and product = 'Handling Fee' then 0 ---CONTINUAR ACA ACON LOS CASOS PARA QUE SEA NEGATIVO EL REFUND
  WHEN pay.reporting_category = 'fee' and product = 'Handling Fee' then ABS(pay.fee)
  WHEN pay.reporting_category = 'refund' and product <> 'Handling Fee' then ABS(pay.net)
  ELSE ABS(total)
end as settlement_amount,

CASE
  WHEN pay.reporting_category = 'refund' and product in ('Handling Fee','Shipping') then 0 ---CONTINUAR ACA ACON LOS CASOS PARA QUE SEA NEGATIVO EL REFUND
  WHEN pay.reporting_category = 'fee' and product = 'Handling Fee' then pay.fee
  WHEN pay.reporting_category = 'refund' and product not in ('Handling Fee','Shipping') then pay.net
  ELSE total
end as real_amount, ---- 決済金額 column

order_number as ORDER_NUMBER,
mail as MAIL,
ABS(DATETIME_DIFF(CAST(sp.date_transaction AS DATETIME) , datetime_add(pay.created_utc,interval 9 HOUR),SECOND)) as diff_time,
reporting_category,
product,
product_count,
sku,
pay.automatic_payout_id as SETT_NUMBER,
CASE -- 2991
  WHEN pay.reporting_category <> "charge" THEN CAST(pay.created_utc as DATETIME)
  ELSE CAST(date_transaction as DATETIME)
END AS real_date,


from shopify_filtered AS sp
LEFT JOIN
-------- MATCHING THE ORDER NUMBER WITH THE CHARGE_ID
(with or_na as (SELECT
  om.name,
  tr.charge_id,
  CASE
    WHEN tr.customer_email <> om.email THEN 'WRONG_UNION'
    ELSE NULL
  END AS VERIFY
FROM
  (select *,row_number() over(order by created_utc asc) as rn from `Shopify.stripe_api`   where reporting_category = 'charge'
  order by created_utc asc) tr
  LEFT JOIN (
    SELECT
      name,
      email,
      ROW_NUMBER() OVER(ORDER BY created_at asc) AS rn
    FROM
      (select distinct name,email, created_at from
       `Shopify.orders_master`
    WHERE
      payment_gateway_names = 'stripe')
  ) om ON tr.rn = om.rn
WHERE
  tr.reporting_category = 'charge'
)
SELECT
  CASE WHEN VERIFY is null THEN name
  ELSE 'BAD_REPORT'
  END AS name,
pa.*
FROM or_na RIGHT JOIN (SELECT DISTINCT * FROM `Shopify.stripe_payouts`) as pa on or_na.charge_id = pa.charge_id)

as pay on pay.name = sp.order_number

where CAST(date_transaction AS DATE) < (SELECT Datetime_add(max(created_utc),interval 1 day)  from`Shopify.stripe_payouts`) -------- filter only values that are in the stripe data
order by date_transaction desc,automatic_payout_id desc
)
select
CASE
  WHEN real_amount > 0 THEN '収入'
  WHEN real_amount <= 0 THEN '支出'
  ELSE 'ERROR'
END AS balance, -- 収支区分 column
dtim.* except(diff_time)
from dtim
where SETT_NUMBER not in (SELECT DISTINCT SETT_NUMBER FROM `free.stripe_freee_full`)
and amount <> 0
order by date_3 desc
