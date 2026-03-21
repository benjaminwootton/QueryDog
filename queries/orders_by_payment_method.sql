select
    payment_method,
    count(*) as order_count,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_order_value
from ecommerce.orders
group by payment_method
order by total_revenue desc;
