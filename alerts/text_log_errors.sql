SELECT
    hostname()                    AS host,
    toStartOfMinute(event_time)   AS minute,
    level,
    logger_name,
    count()                       AS message_count,
    any(message)                  AS sample_message,
    multiIf(level IN ('Fatal', 'Critical', 'Error'), 'CRIT',
            level = 'Warning' AND count() > 50, 'WARN',
            'OK')                 AS severity
FROM system.text_log
WHERE event_time >= now() - INTERVAL 15 MINUTE
  AND level IN ('Fatal', 'Critical', 'Error', 'Warning')
GROUP BY host, minute, level, logger_name
HAVING (level IN ('Fatal', 'Critical', 'Error'))
    OR (level = 'Warning' AND message_count > 50)
ORDER BY minute DESC, level, message_count DESC
LIMIT 100
