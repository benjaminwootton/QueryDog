SELECT
    database,
    table,
    replica_name,
    absolute_delay,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    is_readonly,
    is_session_expired
FROM system.replicas
WHERE absolute_delay > 60
ORDER BY absolute_delay DESC
