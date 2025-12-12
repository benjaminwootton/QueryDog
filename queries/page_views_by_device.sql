select
    device_type,
    browser,
    count(*) as views,
    avg(page_load_time_ms) as avg_load_time,
    avg(time_on_page_seconds) as avg_time_on_page
from ecommerce.page_views
where view_timestamp >= now() - interval 7 day
group by device_type, browser
order by views desc;
