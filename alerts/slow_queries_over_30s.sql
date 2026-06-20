SELECT
    event_time,
    user,
    query_id,
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes_h,
    substring(query, 1, 200) AS query_preview
FROM system.query_log
WHERE event_time >= now() - INTERVAL 1 HOUR
  AND type = 'QueryFinish'
  AND query_duration_ms > 30000
ORDER BY query_duration_ms DESC
LIMIT 100
