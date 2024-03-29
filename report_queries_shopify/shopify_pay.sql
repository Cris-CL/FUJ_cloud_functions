---　tax change to verify that is 8%
--- tax on fee and sorting change 02-27
--- Change order dates 02-08
--- All 2022 data is correct
WITH shopify_filtered as (
    WITH shop_pay as
    (
      SELECT
      om.name as order_number,
      om.email as mail,
      CASE
      WHEN pay.source_type like '%Dispute' THEN 'Chargeback'
      WHEN pay.source_type like '%Refund' THEN CONCAT('Refund ',om.lineitem_name)
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
------------------------------------------------- Shipping management, usually there are problems when
------------------------------------------------- refunds happen
      CASE
--      WHEN pay.payout_status like '%paid' THEN om.shipping_shop_amount
      WHEN pay.source_type like '%Refund' or financial_status = 'refunded' THEN 0
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


      CASE
      WHEN lineitem_name like 'Refund%' THEN ABS(ROUND(current_total_discounts - current_total_discounts/11))
      ELSE -ROUND(current_total_discounts - current_total_discounts/11)
      END AS subtotal,


      CASE
      WHEN lineitem_name like 'Refund%' THEN ABS(ROUND(current_total_discounts/11))
      ELSE -ROUND(current_total_discounts/11)
      END AS tax,

      CASE
      WHEN lineitem_name like 'Refund%' THEN ABS(current_total_discounts)
      ELSE -current_total_discounts
      END AS total,
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
  WHEN total > 0 THEN '収入'
  WHEN total <= 0 THEN '支出'
  ELSE 'SOMETHING_WRONG'
END AS balance, -- 収支区分 column

CAST(control_number AS INTEGER) as control_number, -- 管理番号 column



CAST(date_transaction AS DATE) as transaction_date, ---- 発生日 column

## accrual date is the date where the payment is processed accordint to the api
null AS accrual_date, -- 決済期日 column

payment_method AS suppliers, -- 取引先 column

CASE
  WHEN product = 'Handling Fee' THEN '支払手数料'
  Else '売上高'
End as account, -- 勘定科目 column

CASE
  WHEN product in ('Shipping',
                  'GoCLN シェイカー'
                  ) THEN '課税売上10%'
  WHEN product = 'Handling Fee' THEN '対象外'
  ELSE '課税売上8%（軽）' -------- CAMBIO EN EL KANJI DE 8%
END AS tax_distinction, ----- 税区分 column

ABS(total) as amount, -- 金額  column

'内税' as tax_calculation_distinction, -- 税計算区分 column
-------- Fixed Tax calculation
CASE
  WHEN product in ('Shipping',
                  'GoCLN シェイカー'
                  ) THEN ROUND(ABS(total*0.1))
  WHEN product = 'Handling Fee' THEN 0
  ELSE ROUND(ABS(total*0.08))
END AS tax_total, ---- 税額 column

CASE
  WHEN product = 'Shipping' THEN CONCAT('Delivery (',order_number,')')
  WHEN product = 'Handling Fee' THEN CONCAT('Fee (',order_number,')')
  WHEN product not in ('Shipping','Handling Fee') Then CONCAT('Product Charges (',order_number,')')
END AS remarks, ---- 備考 column

CASE
  WHEN product = 'Shipping' THEN 'Delivery'
  WHEN product = 'Handling Fee' THEN 'Fee Shopify'
  WHEN product not in ('Shipping','Handling Fee') Then 'Sales'
END AS item, ---- 品目 column

null as department, --- 部門 column
null as memo_tag, ----- メモタグ（複数指定可、カンマ区切り） column



CAST( (select processed_at
FROM `test-bigquery-cc.Shopify.shopify_payouts_api`
where
source_type = 'payout' and payout_id = control_number) AS DATE)AS settlement_date, -- 決済日 column


'Shopify' as settlement_account,  ----- 決済口座 column

 total as settlement_amount, ---- 決済金額 column

from shopify_filtered

order by control_number desc,date_transaction desc
