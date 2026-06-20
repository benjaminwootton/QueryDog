SELECT
    database,
    table,
    replica_name,
    is_readonly,
    is_session_expired,
    queue_size,
    absolute_delay
FROM system.replicas
WHERE is_readonly = 1 OR is_session_expired = 1
