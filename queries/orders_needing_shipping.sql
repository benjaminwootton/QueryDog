select
    order_id,
    order_number,
    customer_first_name,
    customer_last_name,
    shipping_address_city,
    shipping_address_country,
    shipping_method,
    total_amount,
    order_date
from ecommerce.orders
where order_status = 'confirmed'
    and shipped_date is null
order by order_date asc
limit 100;
