SELECT
    hostname()     AS host,
    metric,
    toInt64(value) AS value,
    description,
    multiIf(
        metric = 'BrokenDistributedFilesToInsert' AND value > 0, 'CRIT',
        metric = 'DistributedFilesToInsert' AND value > 10000, 'CRIT',
        metric = 'DistributedFilesToInsert' AND value > 1000, 'WARN',
        'OK'
    ) AS severity
FROM system.metrics
WHERE metric IN ('DistributedFilesToInsert',
                 'DistributedBytesToInsert',
                 'BrokenDistributedFilesToInsert',
                 'BrokenDistributedBytesToInsert',
                 'DistributedSend')
  AND (
       (metric = 'DistributedFilesToInsert'         AND value > 1000)
    OR (metric = 'BrokenDistributedFilesToInsert'   AND value > 0)
    OR (metric = 'BrokenDistributedBytesToInsert'   AND value > 0)
  )
ORDER BY severity, metric
