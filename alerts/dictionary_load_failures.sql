SELECT
    database,
    name,
    status,
    last_exception,
    loading_start_time,
    loading_duration
FROM system.dictionaries
WHERE status IN ('FAILED', 'FAILED_AND_RELOADING')
   OR last_exception != ''
