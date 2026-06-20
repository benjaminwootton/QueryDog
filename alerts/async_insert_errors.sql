SELECT
    event_time,
    database,
    table,
    status,
    exception,
    rows,
    bytes
FROM system.asynchronous_insert_log
WHERE event_time >= now() - INTERVAL 1 HOUR
  AND status != 'Ok'
ORDER BY event_time DESC
LIMIT 100
