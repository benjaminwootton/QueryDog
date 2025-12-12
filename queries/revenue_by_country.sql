select
    shipping_address_country as country,
    count(*) as orders,
    sum(total_amount) as revenue,
    avg(total_amount) as avg_order_value
from ecommerce.orders
group by shipping_address_country
order by revenue desc
limit 20;
