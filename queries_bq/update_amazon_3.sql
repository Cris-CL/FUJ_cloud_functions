SELECT sum(total_real) as total_to_update from
`free_adj.amazon_2022`

-- UPDATE `free_adj.amazon_2022`
-- SET
-- main_date = CAST(datetime_sub(main_date, INTERVAL 1 MONTH) as DATE),
-- order_full_date = datetime_sub(order_full_date, INTERVAL 1 MONTH),
-- tax_calc_distinction = 'ONE MONTH SUBSTRACTED'
-- -- - tax_calc_distinction = '内税'

where
account = '売上高'
AND item = 'Delivery'
and settlement_id = '11087204913'
and CAST(DATETIME_TRUNC(order_full_date, MONTH) as DATE) = '2022-02-01'
and remarks = 'Shipping'
AND total_real in (37.0, 110.0, 273.0, 410.0)

AND order_full_date IN (
'2022-02-23T20:13:54',
'2022-02-16T00:40:23',
'2022-02-19T19:39:20',
'2022-02-23T11:30:37'
)
