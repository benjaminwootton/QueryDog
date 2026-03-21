select
    arrayJoin(item_categories) as category,
    count(*) as orders_containing,
    sum(total_amount) as revenue
from ecommerce.orders
group by category
order by revenue desc
limit 20;
