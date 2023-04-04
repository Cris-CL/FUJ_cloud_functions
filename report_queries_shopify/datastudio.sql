-------- For Datastudio dashboard updated 04-03
WITH
  sub_q AS ( (
    SELECT
      CASE
        WHEN OC.purchase_date IS NULL THEN vm.posted_date_time
      ELSE
      OC.purchase_date
    END
      AS transaction_date,
      SUM(amount) AS amount,
      SUM(quantity_purchased) AS quantity,
      sku,
      ship_postal_code AS zip_code,
      'Amazon' AS SOURCE
    FROM
      `Amazon.transaction_view_master` AS vm
    LEFT JOIN (
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
        amazon_order_id IS NOT NULL ) AS oc
    ON
      vm.order_id = oc.amazon_order_id
    WHERE
      (transaction_type IS NOT NULL
        OR amazon_order_id IS NOT NULL
        OR amount_type IS NOT NULL
        OR amount_description IS NOT NULL )
      AND (transaction_type = 'Order'
        AND amount_type in ('ItemPrice','ItemFees','Promotion','Points')) ----- added all sales parameters
    GROUP BY
      settlement_id,
      transaction_date,
      sku,
      ship_postal_code
    ORDER BY
      transaction_date DESC )
  UNION ALL (
    SELECT
      CAST(DATETIME_ADD(order_date_time,INTERVAL 9 HOUR) AS DATETIME) AS transaction_date,
      quantity*unit_price AS amount,
      quantity,

      CASE ----- UPATED sku
       WHEN item_number like 'v_%' then CAST(substr(item_number, 3 ,13) AS STRING)
       ELSE CAST(LEFT(item_number,13) AS STRING)
      END AS sku,

      ---- FIX FOR THE ZIP CODE THAT IS NOT IN THE FORMAT XXX-XXXX
      CASE
        WHEN CHAR_LENGTH(mailing_address_postal_code_2) < 4
          THEN  CONCAT(destination_postal_code_1,'-',repeat("0", 4 - CHAR_LENGTH(mailing_address_postal_code_2)),mailing_address_postal_code_2)
        ELSE CONCAT(destination_postal_code_1,'-',mailing_address_postal_code_2)
      END AS zip_code,
      'Rakuten' AS SOURCE
    FROM
      `Rakuten.test_raku`
    WHERE
      status in ('700','600','500') ---------- updated to get all orders that are confirmed, paid or in process
    ORDER BY
      order_date_time DESC )
  UNION ALL (
    SELECT
      CAST(DATETIME_ADD(created_at,INTERVAL 9 HOUR) AS DATETIME) AS transaction_date,
      -- CAST(DATETIME_ADD(created_at,INTERVAL -9 HOUR) AS DATETIME) AS transaction_date,
      lineitem_price*lineitem_quantity AS amount,
      lineitem_quantity AS quantity,
      CAST(lineitem_sku AS STRING) AS sku,
      ship_zip AS zip_code,
      'Shopify' AS SOURCE
    FROM
      `test-bigquery-cc.Shopify.orders_master`
    WHERE financial_status = 'paid'
    ORDER BY
      created_at DESC )
  ORDER BY
    transaction_date DESC)
SELECT
  transaction_date,
  amount,
  quantity,
  sub_q.sku,
  zip_code,
  SOURCE,
  ref_sku.item_name,
  -- count(ref_sku.product_name) as count_sku
FROM
  sub_q
LEFT JOIN
  `Datastudio.combined_sku` AS ref_sku
ON
  sku_item = sub_q.sku and SOURCE = ref_sku.store_name
order by transaction_date desc
