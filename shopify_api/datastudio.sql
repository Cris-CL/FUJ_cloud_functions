---- All results combined
With sub_q as (
(SELECT
CAST(purchase_or_posted_date AS DATETIME) as transaction_date,
-- transaction_type,
sum_amount as amount,
quantity,
CAST(sku as STRING) as sku,
-- ref.product_name,
'Amazon' as source
FROM `test-bigquery-cc.queries_amazon.merged_ama_datastudio` --LEFT JOIN `Datastudio.sku_reference` as ref on sku = amazon_sku
WHERE transaction_type = 'Order' and amount_description ='Principal'
-- order by purchase_or_posted_date desc
)

UNION ALL
(SELECT
CAST(DATETIME_ADD(order_date_time,INTERVAL 9 HOUR) AS DATETIME) as transaction_date,
quantity*unit_price as amount,
quantity,
CAST(LEFT(item_number,13) AS STRING) as sku,
-- ref.product_name,
'Rakuten' as source
FROM `test-bigquery-cc.Datastudio.results_rakuten` --LEFT JOIN `Datastudio.sku_reference` AS ref on item_number = CAST(rakuten_item_number AS STRING)
where status = '700'
-- order by order_date_time desc
)

UNION ALL
(SELECT
CAST(DATETIME_ADD(created_at,INTERVAL 9 HOUR) AS DATETIME) as transaction_date,
lineitem_price*lineitem_quantity as amount,
lineitem_quantity,
CAST(lineitem_sku AS STRING) as sku,
-- ref.product_name,
'Shopify' as source
FROM `test-bigquery-cc.Datastudio.results_shopify` --LEFT JOIN `Datastudio.sku_reference` AS ref on lineitem_sku = CAST(shopify_sku AS STRING)
)

ORDER BY transaction_date desc)

SELECT
transaction_date,
amount,
quantity,
sub_q.sku,
source,
ref_sku.item_name,
-- count(ref_sku.product_name) as count_sku
From sub_q left join `Datastudio.combined_sku` as ref_sku on sku_item = sub_q.sku
