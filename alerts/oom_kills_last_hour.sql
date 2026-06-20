SELECT
    event_time,
    user,
    query_id,
    exception,
    formatReadableSize(memory_usage) AS memory_h,
    substring(query, 1, 200) AS query_preview
FROM system.query_log
WHERE event_time >= now() - INTERVAL 1 HOUR
  AND exception_code = 241
ORDER BY event_time DESC
LIMIT 100
