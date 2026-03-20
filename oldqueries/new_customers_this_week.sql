select
    customer_id,
    email,
    first_name,
    last_name,
    registration_date,
    customer_segment,
    shipping_city,
    shipping_country
from ecommerce.customers
where registration_date >= now() - interval 7 day
order by registration_date desc
limit 100;
