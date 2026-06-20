SELECT
    hostname()     AS host,
    name           AS disk_name,
    type,
    path,
    is_broken,
    is_read_only,
    formatReadableSize(total_space)                                     AS total,
    formatReadableSize(free_space)                                      AS free,
    formatReadableSize(unreserved_space)                                AS unreserved,
    round(free_space / nullIf(total_space, 0) * 100, 2)                 AS free_pct,
    'CRIT' AS severity
FROM system.disks
WHERE total_space > 0  -- exclude object-storage disks that don't report capacity
  AND (is_broken = 1 OR free_space / total_space < 0.10)
ORDER BY free_pct ASC
