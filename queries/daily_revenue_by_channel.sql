select
    toDate(order_date) as date,
    source_channel,
    count(*) as orders,
    sum(total_amount) as revenue
from ecommerce.orders
group by date, source_channel
order by date desc, revenue desc
limit 100;
