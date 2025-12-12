select
    cart_id,
    customer_id,
    items_count,
    estimated_total,
    cart_abandoned_at,
    source_channel,
    device_type
from ecommerce.shopping_cart
where cart_status = 'abandoned'
    and cart_abandoned_at >= now() - interval 24 hour
order by estimated_total desc
limit 100;
