---- Updated 12-05 with the payout number for the transactions
----
WITH shopify_filtered AS (
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

FROM `test-bigquery-cc.Shopify.orders_master`
WHERE name in (SELECT name FROM `test-bigquery-cc.Shopify.orders_master` WHERE payment_gateway_names = 'stripe')


order by order_number desc )

------ ROWS: order_number ; mail ; date_transaction ; subtotal ; tax ; total ; product_count ; product ; processing_method

SELECT

CASE
  WHEN total > 0 THEN '??????'
  WHEN total <= 0 THEN '??????'
  ELSE 'SOMETHING_WRONG'
END AS balance, -- ???????????? column

pay.automatic_payout_id AS control_number, -- ???????????? column

FORMAT_DATE("%Y-%m-%d", date_transaction) AS accrual_date, -- ????????? column  (FORMAT_DATE("%Y-%m-01"

FORMAT_DATE("%Y-%m-%d",CAST(pay.automatic_payout_effective_at AS DATE)) AS deposit_date, -- ???????????? column

'Shopify Stripe' AS suppliers, -- ????????? column

CASE
  WHEN product = 'Handling Fee' THEN '???????????????'
  Else '?????????'
End as account, -- ???????????? column

CASE
  WHEN product in ('Shipping',
                  'GoCLN ???????????????'
                  ) THEN '????????????10%'
  WHEN product = 'Handling Fee' THEN '?????????'
  ELSE '????????????8%'
END AS tax_distinction, ----- ????????? column

ABS(total) as amount, -- ??????  column

'??????' as tax_calculation_distinction, -- ??????????????? column

CASE
  WHEN product in ('Shipping','GoCLN ???????????????') THEN ROUND(ABS(total-total/1.1))
  WHEN product = 'Handling Fee' THEN 0
  ELSE ROUND(ABS(total-total/1.08))
END as tax_total, ---- ?????? column

CASE
  WHEN product = 'Shipping' THEN CONCAT('Delivery (',order_number,')')
  WHEN product = 'Handling Fee' THEN CONCAT('Fee (',order_number,')')
  WHEN product = 'Discount' Then CONCAT('Product Charges Discount (',order_number,')')
  WHEN product not in ('Shipping','Handling Fee','Discount') Then CONCAT('Product Charges (',order_number,')')
END AS remarks, ---- ?????? column

CASE
  WHEN product = 'Shipping' THEN 'Delivery'
  WHEN product = 'Handling Fee' THEN 'Fee Shopify'
  WHEN product not in ('Shipping','Handling Fee') Then 'Sales'
END AS item, ---- ?????? column

null as department, --- ?????? column
null as memo_tag, ----- ?????????????????????????????????????????????????????? column

FORMAT_DATE("%Y-%m-%d", date_transaction) as date_3, ---- ????????? column

'Shopify' as settlement_account,  ----- ???????????? column

total as settlement_amount, ---- ???????????? column
(select max(processed_at) from `test-bigquery-cc.Shopify.orders_master` where payment_gateway_names = 'stripe') as LAST_UPDATED,
order_number as ORDER_NUMBER,
mail as MAIL,

from shopify_filtered AS sp
--------- Join with payout data
---- shopify doesnt have a reference number for stripe payments so the join is made on
---- customer email and closest date of transaction
LEFT JOIN `Shopify.stripe_payouts` as pay on CAST(sp.date_transaction AS DATETIME) < datetime_add(pay.created_utc,interval 9 HOUR)
and ABS(DATETIME_DIFF(CAST(sp.date_transaction AS DATETIME) , datetime_add(pay.created_utc,interval 9 HOUR),SECOND)) <= 7
---- 7 seconds seems to be the best threshold to match the orders between shopify and stripe
and sp.mail = pay.customer_email
where sp.total is not null
---------

and CAST(date_transaction AS DATE) < (SELECT Datetime_add(max(created_utc),interval 1 day)  from`Shopify.stripe_payouts` ) -------- filter only values that are in the stripe data
order by date_transaction desc
