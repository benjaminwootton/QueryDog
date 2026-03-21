select
    customer_segment,
    count(*) as customer_count,
    avg(total_spent) as avg_lifetime_value,
    avg(total_orders) as avg_orders,
    sum(loyalty_points) as total_loyalty_points
from ecommerce.customers
group by customer_segment
order by avg_lifetime_value desc;
