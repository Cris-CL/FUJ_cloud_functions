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
  WHEN total > 0 THEN '収入'
  WHEN total <= 0 THEN '支出'
  ELSE 'SOMETHING_WRONG'
END AS balance, -- 収支区分 column

null AS control_number, -- 管理番号 column

FORMAT_DATE("%Y-%m-%d", date_transaction) AS accrual_date, -- 発生日 column  (FORMAT_DATE("%Y-%m-01"

null AS settlement_date, -- 決済期日 column

'Shopify Stripe' AS suppliers, -- 取引先 column

CASE
  WHEN product = 'Handling Fee' THEN '支払手数料'
  Else '売上高'
End as account, -- 勘定科目 column

CASE
  WHEN product in ('Shipping',
                  'GoCLN シェイカー'
                  ) THEN '課税売上10%'
  WHEN product = 'Handling Fee' THEN '対象外'
  ELSE '課税売上8%'
END AS tax_distinction, ----- 税区分 column

ABS(total) as amount, -- 金額  column

'内税' as tax_calculation_distinction, -- 税計算区分 column

CASE
  WHEN product in ('Shipping','GoCLN シェイカー') THEN ROUND(ABS(total-total/1.1))
  WHEN product = 'Handling Fee' THEN 0
  ELSE ROUND(ABS(total-total/1.08))
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

FORMAT_DATE("%Y-%m-%d", date_transaction) as date_3, ---- 決済日 column

'Shopify' as settlement_account,  ----- 決済口座 column

total as settlement_amount, ---- 決済金額 column
(select max(processed_at) from `test-bigquery-cc.Shopify.orders_master` where payment_gateway_names = 'stripe') as LAST_UPDATED


from shopify_filtered
where shopify_filtered.total is not null
and CAST(date_transaction AS DATE) < (SELECT Datetime_add(max(created_utc),interval 1 day)  from`Shopify.stripe_api` )
order by date_transaction desc
