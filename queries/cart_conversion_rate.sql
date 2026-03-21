select
    toDate(cart_created_at) as date,
    count(*) as total_carts,
    countIf(cart_status = 'converted') as converted,
    countIf(cart_status = 'abandoned') as abandoned,
    round(countIf(cart_status = 'converted') * 100.0 / count(*), 2) as conversion_rate
from ecommerce.shopping_cart
group by date
order by date desc
limit 30;
