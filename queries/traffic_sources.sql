select
    utm_source,
    utm_medium,
    count(*) as visits,
    uniqExact(session_id) as unique_sessions,
    countIf(add_to_cart_clicked = 1) as add_to_cart_actions
from ecommerce.page_views
where utm_source is not null
group by utm_source, utm_medium
order by visits desc
limit 50;
