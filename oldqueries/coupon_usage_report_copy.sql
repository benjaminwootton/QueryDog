select
    coupon_code,
    count(*) as times_used,
    sum(discount_amount) as total_discount,
    sum(total_amount) as revenue_with_coupon,
    avg(total_amount) as avg_order_value
from ecommerce.orders
where coupon_code is not null
    and coupon_code != ''
group by coupon_code
order by times_used desc
limit 50;
