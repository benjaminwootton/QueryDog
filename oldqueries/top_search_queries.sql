select
    search_query,
    count(*) as search_count,
    avg(search_results_count) as avg_results
from ecommerce.page_views
where search_query is not null
    and search_query != ''
group by search_query
order by search_count desc
limit 50;
