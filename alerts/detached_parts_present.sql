SELECT
    database,
    table,
    partition_id,
    name,
    reason,
    disk
FROM system.detached_parts
ORDER BY database, table, name
LIMIT 200
