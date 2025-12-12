select
    toDate(order_date) as date,
    count(*) as orders,
    avg(total_amount) as avg_order_value,
    median(total_amount) as median_order_value
from ecommerce.orders
group by date
order by date desc
limit 90;
