select
    shipping_carrier,
    shipping_method,
    count(*) as shipments,
    avg(dateDiff('day', shipped_date, delivered_date)) as avg_delivery_days,
    countIf(delivered_date is not null) as delivered,
    round(countIf(delivered_date is not null) * 100.0 / count(*), 2) as delivery_rate
from ecommerce.orders
where shipped_date is not null
group by shipping_carrier, shipping_method
order by shipments desc;
