SELECT
    database,
    table,
    data_path,
    is_blocked,
    error_count,
    data_files,
    formatReadableSize(data_compressed_bytes) AS pending_h
FROM system.distribution_queue
WHERE data_files > 100 OR is_blocked = 1
ORDER BY data_files DESC
