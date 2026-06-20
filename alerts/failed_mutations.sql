SELECT
    database,
    table,
    mutation_id,
    create_time,
    parts_to_do,
    is_done,
    latest_fail_reason,
    latest_fail_time
FROM system.mutations
WHERE is_done = 0
  AND latest_fail_reason != ''
ORDER BY latest_fail_time DESC
