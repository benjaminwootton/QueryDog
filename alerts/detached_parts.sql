SELECT
    hostname()                AS host,
    database,
    table,
    disk,
    splitByChar('_', name)[1] AS reason_prefix,
    count()                   AS detached_count,
    sum(bytes_on_disk)        AS bytes_on_disk,
    min(modification_time)    AS oldest,
    max(modification_time)    AS newest,
    multiIf(
        splitByChar('_', name)[1] IN ('broken', 'broken-on-start'), 'CRIT',
        splitByChar('_', name)[1] IN ('unexpected', 'covered-by-broken'), 'WARN',
        'INFO'
    ) AS severity
FROM system.detached_parts
WHERE splitByChar('_', name)[1] != 'ignored'
GROUP BY host, database, table, disk, reason_prefix
ORDER BY severity, detached_count DESC
LIMIT 100
