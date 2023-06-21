
select
item_name,
sku,
sum(quantity) as q_sales,
sum(amount) as sales
from `free.dashboard_full`
where SOURCE = 'Amazon'
and EXTRACT(YEAR FROM transaction_date) = 2023 and EXTRACT(MONTH FROM transaction_date) = 1
group by
item_name,
sku

order by sales desc
