--- Improved 12-02
--- All 2022 data is correct
WITH shopify_filtered as (
    WITH shop_pay as
    (
      SELECT
      om.name as order_number,
      om.email as mail,
      CASE
      WHEN pay.source_type like '%Dispute' THEN 'Chargeback'
      ELSE om.lineitem_name
      END AS lineitem_name,

      CASE WHEN (pay.source_type like '%Refund' and financial_status = 'partially_refunded') THEN 1
      WHEN pay.source_type like '%Dispute' THEN 1
      WHEN (pay.source_type like '%Refund' and financial_status <> 'partially_refunded') THEN om.lineitem_quantity
      ELSE om.lineitem_quantity
      end as lineitem_quantity,

      CASE WHEN (pay.source_type like '%Refund' and financial_status = 'partially_refunded') THEN CAST(pay.amount AS FLOAT64)
      WHEN (pay.source_type like '%Refund' and financial_status <> 'partially_refunded') THEN -om.lineitem_price
      WHEN pay.source_type like '%Dispute' THEN CAST(pay.amount AS FLOAT64)
      ELSE om.lineitem_price
      end as lineitem_price,

      CASE WHEN pay.source_type like '%Refund' and financial_status = 'refunded' THEN 0
      WHEN financial_status = 'refunded' THEN 0
      ELSE om.discount_amount
      end as discount_amount,


      om.lineitem_sku as sku,

      CASE WHEN pay.source_type like '%Refund' or financial_status = 'refunded' THEN 0
      WHEN pay.source_type like '%Refund' and financial_status <> 'refunded' THEN 0
      WHEN pay.source_type like '%Dispute' THEN -om.shipping_shop_amount
      ELSE om.shipping_shop_amount
      end as shipping,

      om.current_total_discounts,
      pay.payout_id as control_number,

      CASE WHEN pay.source_type like '%Refund' THEN 0
      ELSE CAST(pay.fee AS FLOAT64)
      end as sp_fee,

      pay.processed_at as payment_processing_date,
      om.processed_at as order_processing_date,
      om.payment_gateway_names as payment_method
      FROM `Shopify.shopify_payouts_api` as pay JOIN `Shopify.orders_master` as om on pay.source_order_id = om.id
      WHERE payout_id is not null
      order by name desc
    )


    SELECT
      order_number,
      control_number,
      mail,
      Datetime_add(order_processing_date,interval 9 HOUR) AS date_transaction,
      ROUND((lineitem_quantity*lineitem_price)/1.08) AS subtotal,
      lineitem_quantity*lineitem_price - ROUND((lineitem_quantity*lineitem_price)/1.08) AS tax,
      lineitem_quantity*lineitem_price AS total,
      lineitem_quantity AS product_count,
      lineitem_name AS product,
      payment_method

    FROM shop_pay where lineitem_name <> 'Chargeback'

    UNION ALL
    SELECT distinct
    order_number,
    control_number,
    mail,
    Datetime_add(order_processing_date,interval 9 HOUR) AS date_transaction,
    ROUND((lineitem_quantity*lineitem_price)/1.08) AS subtotal,
    lineitem_quantity*lineitem_price - ROUND((lineitem_quantity*lineitem_price)/1.08) AS tax,
    lineitem_quantity*lineitem_price AS total,
    lineitem_quantity AS product_count,
    lineitem_name AS product,
    payment_method

    FROM shop_pay where lineitem_name = 'Chargeback'

    UNION ALL
    SELECT distinct
      order_number,
      control_number,
      mail,
      Datetime_add(order_processing_date,interval 9 HOUR) AS date_transaction,
      ROUND(shipping - shipping/11) AS subtotal,
      ROUND(shipping/11) AS tax,
      shipping AS total,
      1 AS product_count,
      'Shipping' AS product,
      payment_method
    FROM shop_pay
    where shipping <>0

    UNION ALL
    SELECT distinct
      order_number,
      control_number,
      mail,
      Datetime_add(order_processing_date,interval 9 HOUR) AS date_transaction,

      -ROUND(current_total_discounts - current_total_discounts/11) AS subtotal,

      -ROUND(current_total_discounts/11) AS tax,

      -current_total_discounts astotal,

      1 AS product_count,
      'Discount' AS product,
      payment_method
    FROM shop_pay
    WHERE (discount_amount <> 0 or current_total_discounts <> 0) and (discount_amount is not null or current_total_discounts is not null)

    UNION ALL
    SELECT distinct
      order_number,
      control_number,
      mail,
      Datetime_add(order_processing_date,interval 9 HOUR) AS date_transaction,
      -sp_fee AS subtotal,
      0 AS tax,
      -sp_fee AS total,
      1 AS product_count,
      'Handling Fee' AS product,
      payment_method,
    FROM shop_pay

)

SELECT

CASE
  WHEN total > 0 THEN '??????'
  WHEN total <= 0 THEN '??????'
  ELSE 'SOMETHING_WRONG'
END AS balance, -- ???????????? column

control_number, -- ???????????? column

## accrual date is the date where the payment is processed accordint to the api
(select processed_at
FROM `test-bigquery-cc.Shopify.shopify_payouts_api`
where
source_type = 'payout' and payout_id = control_number) AS accrual_date, -- ????????? column

null AS settlement_date, -- ???????????? column

payment_method AS suppliers, -- ????????? column

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

ROUND(ABS(total-total/1.08)) as tax_total, ---- ?????? column

CASE
  WHEN product = 'Shipping' THEN CONCAT('Delivery (',order_number,')')
  WHEN product = 'Handling Fee' THEN CONCAT('Fee (',order_number,')')
  WHEN product not in ('Shipping','Handling Fee') Then CONCAT('Product Charges (',order_number,')')
END AS remarks, ---- ?????? column

CASE
  WHEN product = 'Shipping' THEN 'Delivery'
  WHEN product = 'Handling Fee' THEN 'Fee Shopify'
  WHEN product not in ('Shipping','Handling Fee') Then 'Sales'
END AS item, ---- ?????? column

null as department, --- ?????? column
null as memo_tag, ----- ?????????????????????????????????????????????????????? column

date_transaction as transaction_date, ---- ????????? column

'Shopify' as settlement_account,  ----- ???????????? column

 total as settlement_amount, ---- ???????????? column

from shopify_filtered
-- where control_number = '76602114119'
order by date_transaction desc,
control_number
