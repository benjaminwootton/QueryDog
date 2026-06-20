SELECT
    hostname() AS host,
    database,
    table,
    partition,
    count() AS active_parts,
    multiIf(count() > 300, 'CRIT', count() > 150, 'WARN', 'OK') AS severity
FROM system.parts
WHERE active = 1
  AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
GROUP BY host, database, table, partition
HAVING active_parts > 150
ORDER BY active_parts DESC
LIMIT 50
