SELECT
    database,
    table,
    elapsed,
    progress,
    num_parts,
    source_part_names,
    result_part_name,
    formatReadableSize(total_size_bytes_compressed) AS size_h
FROM system.merges
WHERE num_parts > 50
ORDER BY num_parts DESC
LIMIT 50
