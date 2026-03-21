select
    customer_id,
    email,
    first_name,
    last_name,
    total_orders,
    total_spent,
    loyalty_tier
from ecommerce.customers
order by total_spent desc
limit 50;
