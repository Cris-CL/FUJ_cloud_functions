SELECT
-- settlement_id,
-- account,
-- item,
-- remarks,
-- CAST(DATETIME_TRUNC(order_full_date, MONTH) as DATE) as month_data,
-- account,
-- order_full_date,
-- posted,
MAX(order_full_date) as first_appearance_date,
total_real,
-- sum(total_real) OVER (ORDER BY order_full_date ASC) AS running_sum

from
`free_adj.amazon_2022`

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
-- AND order_full_date <= '2022-11-07T02:54:08'

-- (1950, 1950, 1950, 4980, 8480, 1950, 4480)
-- or (order_full_date = '2022-10-01T04:45:20' and remarks = 'Product Charges')

-- (-2980, -2980, -9960, -1950, -2980, -2980, -2980, -1950, -1980, -5960)
-- Product Charges Promotion Refund
-- Product Charges Refund
-- and total_real < 0
-- AND order_full_date >= '2021-12-01T23:46:58'
-- AND order_full_date <= '2022-03-17T10:38:09'
-- AND total_real = -12
-- order by total_real ASC
group by total_real
-- order by remarks ASC, order_full_date asc
