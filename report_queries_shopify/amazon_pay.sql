---- UPDATED 03-03 with new '課税売上8%（軽）' in tax and correct tax calc
---- FOR REPORTS updated 02-08 with correct deposit date in date_3
WITH amazon_pay as (
SELECT
st.name,
ap.settlementid as settlement_id,
st.payment_gateway_names as payment_references,
ap.sellerorderid as sellerorderid,
ap.transactiontype as transactiontype,
st.financial_status as financial_status,
ap.transactionposteddate as date_ap,
ap.transactionamount as ap_amount,
ap.transactionpercentagefee as ap_fee,
ap.nettransactionamount as ap_net_amount,
st.subtotal_price as subtotal,
st.shipping_presentment_amount as shipping,
st.total_tax as taxes,
st.discount_amount as discount_amount,
st.total_price as total,
st.lineitem_name,

CASE
  WHEN ap.transactiontype = 'Refund' THEN -st.lineitem_price
  ELSE st.lineitem_price
END AS lineitem_price,

st.lineitem_quantity,
st.created_at as paid_at,
st.email,
st.processing_method as payment_method,
(select max(transactionposteddate) from `test-bigquery-cc.Shopify.amazon_pay_2` as dat where dat.settlementid = ap.settlementid) as LAST_UPDATED


FROM `test-bigquery-cc.Shopify.amazon_pay_2` as ap
LEFT JOIN `test-bigquery-cc.Shopify.orders_master`  as st on  ap.sellerorderid like concat('%',st.checkout_id,'%')
where name is not null and transactiontype in ('Capture','Refund') ---- Only taking the Captured or refunded transactions

order by name desc
),

shopify_filtered AS (

SELECT
  name AS order_number,
  settlement_id,
  email AS mail,
  Datetime_add(paid_at,interval 9 HOUR) AS date_transaction,
  ROUND((lineitem_quantity*lineitem_price)/1.08) AS subtotal,
  lineitem_quantity*lineitem_price - ROUND((lineitem_quantity*lineitem_price)/1.08) AS tax,
  lineitem_quantity*lineitem_price AS total,
  lineitem_quantity AS product_count,
  lineitem_name AS product,
  payment_method,
  LAST_UPDATED

FROM amazon_pay

UNION ALL
SELECT distinct
  name AS order_number,
  settlement_id,
  email AS mail,
  Datetime_add(paid_at,interval 9 HOUR) AS date_transaction,
  ROUND(shipping - shipping/11) AS subtotal,
  ROUND(shipping/11) AS tax,
  shipping AS total,
  1 AS product_count,
  'Shipping' AS product,
  payment_method,
  LAST_UPDATED
FROM amazon_pay
WHERE shipping >0

UNION ALL
SELECT distinct
  name AS order_number,
  settlement_id,
  email AS mail,
  Datetime_add(paid_at,interval 9 HOUR) AS date_transaction,
  -ROUND(discount_amount - discount_amount/11) AS subtotal,
  -ROUND(discount_amount/11) AS tax,
  -discount_amount AS total,
  1 AS product_count,
  'Discount' AS product,
  payment_method,
  LAST_UPDATED
FROM amazon_pay
WHERE discount_amount <> 0 and discount_amount is not null

UNION ALL
SELECT distinct
  name AS order_number,
  settlement_id,
  email AS mail,
  Datetime_add(paid_at,interval 9 HOUR) AS date_transaction,
  ap_fee AS subtotal,
  0 AS tax,
  ap_fee AS total,
  1 AS product_count,
  'Handling Fee' AS product,
  payment_method,
  LAST_UPDATED
FROM amazon_pay

order by order_number desc

)

------ ROWS: order_number ; mail ; date_transaction ; subtotal ; tax ; total ; product_count ; product ; payment_method

SELECT

CASE
  WHEN total > 0 THEN '収入'
  WHEN total <= 0 THEN '支出'
  ELSE 'SOMETHING_WRONG'
END AS balance, -- 収支区分 column

settlement_id AS control_number, -- 管理番号 column

FORMAT_DATE("%Y-%m-%d", date_transaction) AS accrual_date, -- 発生日 column  (FORMAT_DATE("%Y-%m-01"

null AS settlement_date, -- 決済期日 column

'Shopify Amazon' AS suppliers, -- 取引先 column

CASE
  WHEN product = 'Handling Fee' THEN '支払手数料'
  Else '売上高'
End as account, -- 勘定科目 column

CASE
  WHEN product in ('Shipping',
                  'GoCLN シェイカー') THEN '課税売上10%'
  WHEN product = 'Handling Fee' THEN '対象外'
  ELSE '課税売上8%（軽）' --- updated
END AS tax_distinction, ----- 税区分 column

ABS(total) as amount, -- 金額  column

'内税' as tax_calculation_distinction, -- 税計算区分 column

CASE
  WHEN product in ('Shipping','GoCLN シェイカー') THEN ROUND(ABS(total*0.1))
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

CAST(LAST_UPDATED as DATE) as date_3, ---- 決済日 column

'Shopify' as settlement_account,  ----- 決済口座 column

total as settlement_amount, ---- 決済金額 column


-- (select string_field_1 from `Shopify.nomenclature_shopify` Where string_field_1 = product.shopify_filtered) as account,


from shopify_filtered
order by order_number desc, date_transaction
