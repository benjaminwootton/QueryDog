SELECT
    query_id,
    user,
    elapsed,
    formatReadableSize(memory_usage) AS memory_h,
    read_rows,
    substring(query, 1, 200) AS query_preview
FROM system.processes
WHERE elapsed > 300
ORDER BY elapsed DESC
