select
    loyalty_tier,
    count(*) as customers,
    avg(total_spent) as avg_spent,
    avg(total_orders) as avg_orders,
    avg(loyalty_points) as avg_points
from ecommerce.customers
group by loyalty_tier
order by avg_spent desc;
