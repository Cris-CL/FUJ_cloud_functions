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
(select max(ap.transactionposteddate) from `test-bigquery-cc.Shopify.amazon_pay_2`) as LAST_UPDATED


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
  WHEN total > 0 THEN '??????'
  WHEN total <= 0 THEN '??????'
  ELSE 'SOMETHING_WRONG'
END AS balance, -- ???????????? column

settlement_id AS control_number, -- ???????????? column

FORMAT_DATE("%Y-%m-%d", date_transaction) AS accrual_date, -- ????????? column  (FORMAT_DATE("%Y-%m-01"

null AS settlement_date, -- ???????????? column

'Shopify Amazon' AS suppliers, -- ????????? column

CASE
  WHEN product = 'Handling Fee' THEN '???????????????'
  Else '?????????'
End as account, -- ???????????? column

CASE
  WHEN product in ('Shipping',
                  'GoCLN ???????????????') THEN '????????????10%'
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
LAST_UPDATED

-- (select string_field_1 from `Shopify.nomenclature_shopify` Where string_field_1 = product.shopify_filtered) as account,


from shopify_filtered
order by order_number desc, date_transaction
