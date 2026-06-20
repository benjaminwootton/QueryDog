SELECT
    hostname() AS host,
    message,
    message_format_string
FROM system.warnings
ORDER BY host, message
