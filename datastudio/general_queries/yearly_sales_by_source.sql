select
SOURCE,
extract(YEAR FROM transaction_date) as year,
sum(amount) as total

from free.dashboard_full
where item = 'Sales'
group by
SOURCE,
year
order by SOURCE, year asc
