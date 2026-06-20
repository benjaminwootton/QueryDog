SELECT
    event_time,
    level,
    logger_name,
    substring(message, 1, 300) AS message_preview
FROM system.text_log
WHERE event_time >= now() - INTERVAL 1 HOUR
  AND level IN ('Error', 'Fatal')
  AND (
        message ILIKE '%zookeeper%'
     OR message ILIKE '%keeper%'
     OR logger_name ILIKE '%ZooKeeper%'
  )
ORDER BY event_time DESC
LIMIT 100
