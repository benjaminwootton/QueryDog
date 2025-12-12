select
    toStartOfHour(order_date) as hour,
    count(*) as orders,
    sum(total_amount) as revenue
from ecommerce.orders
where order_date >= now() - interval 48 hour
group by hour
order by hour desc;
