SELECT
    name,
    path,
    formatReadableSize(free_space) AS free_h,
    formatReadableSize(total_space) AS total_h,
    round(free_space / total_space * 100, 2) AS free_pct
FROM system.disks
WHERE total_space > 0
  AND free_space / total_space < 0.99  -- TEMP: relaxed for portal demo
ORDER BY free_pct ASC
