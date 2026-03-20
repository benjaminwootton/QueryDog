select
    order_id,
    order_number,
    customer_email,
    total_amount,
    order_status,
    payment_status,
    order_date
from ecommerce.orders
where order_status in ('pending', 'processing')
order by order_date asc
limit 100;
