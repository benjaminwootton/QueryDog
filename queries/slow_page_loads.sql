select
    page_path,
    page_type,
    count(*) as views,
    avg(page_load_time_ms) as avg_load_time,
    max(page_load_time_ms) as max_load_time
from ecommerce.page_views
where page_load_time_ms > 3000
group by page_path, page_type
order by avg_load_time desc
limit 50;
