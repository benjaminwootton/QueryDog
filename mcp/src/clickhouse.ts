import { createClient, ClickHouseClient } from '@clickhouse/client';
import * as https from 'https';
import { Environment } from './config';

let client: ClickHouseClient | null = null;
let currentEnv: Environment | null = null;

export function connect(env: Environment): void {
  if (client && currentEnv?.name === env.name) {
    return; // Already connected
  }

  const protocol = env.secure ? 'https' : 'http';
  const url = `${protocol}://${env.host}:${env.port}`;

  const options: Parameters<typeof createClient>[0] = {
    url,
    username: env.user,
    password: env.password,
    database: env.database,
    request_timeout: 30000,
  };

  const skipVerify = env.skip_tls_verify || env.tls_reject_unauthorized === false;
  if (skipVerify && env.secure) {
    options.http_agent = new https.Agent({
      rejectUnauthorized: false,
    });
  }

  if (env.http_headers && Object.keys(env.http_headers).length > 0) {
    options.http_headers = env.http_headers;
  }

  client = createClient(options);
  currentEnv = env;
}

export async function query<T = Record<string, unknown>>(sql: string): Promise<T[]> {
  if (!client) {
    throw new Error('Not connected. Call connect() first.');
  }
  const result = await client.query({
    query: sql,
    format: 'JSONEachRow',
  });
  return result.json<T>();
}

export async function close(): Promise<void> {
  if (client) {
    await client.close();
    client = null;
    currentEnv = null;
  }
}

// Query templates
export const Queries = {
  tables: (database?: string) => `
    SELECT name, engine, total_rows, formatReadableSize(total_bytes) as total_bytes,
           partition_key, sorting_key
    FROM system.tables
    WHERE database = ${database ? `'${database}'` : 'currentDatabase()'}
      AND engine NOT IN ('View', 'MaterializedView')
    ORDER BY total_bytes DESC
  `,

  queriesSlow: (hours = 24, limit = 50) => `
    SELECT event_time, query_duration_ms, formatReadableQuantity(read_rows) as read_rows,
           formatReadableSize(memory_usage) as memory_usage, user,
           substring(query, 1, 300) as query
    FROM system.query_log
    WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
      AND event_time >= now() - INTERVAL ${hours} HOUR
    ORDER BY query_duration_ms DESC
    LIMIT ${limit}
  `,

  queriesFastest: (hours = 24, limit = 50) => `
    SELECT event_time, query_duration_ms, formatReadableQuantity(read_rows) as read_rows,
           formatReadableSize(memory_usage) as memory_usage, user,
           substring(query, 1, 300) as query
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND event_time >= now() - INTERVAL ${hours} HOUR
      AND query_duration_ms > 0
    ORDER BY query_duration_ms ASC
    LIMIT ${limit}
  `,

  queriesRowsRead: (hours = 24, limit = 50) => `
    SELECT event_time, read_rows, formatReadableQuantity(read_rows) as read_rows_fmt,
           query_duration_ms, formatReadableSize(memory_usage) as memory_usage, user,
           substring(query, 1, 300) as query
    FROM system.query_log
    WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
      AND event_time >= now() - INTERVAL ${hours} HOUR
    ORDER BY read_rows DESC
    LIMIT ${limit}
  `,

  queriesMemory: (hours = 24, limit = 50) => `
    SELECT event_time, formatReadableSize(memory_usage) as memory_usage,
           query_duration_ms, formatReadableQuantity(read_rows) as read_rows, user,
           substring(query, 1, 300) as query
    FROM system.query_log
    WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
      AND event_time >= now() - INTERVAL ${hours} HOUR
    ORDER BY memory_usage DESC
    LIMIT ${limit}
  `,

  queriesFrequent: (hours = 24, limit = 50) => `
    SELECT substring(query, 1, 300) as example_query, user, count() as count,
           round(avg(query_duration_ms), 2) as avg_duration_ms,
           max(query_duration_ms) as max_duration_ms,
           formatReadableSize(avg(memory_usage)) as avg_memory
    FROM system.query_log
    WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
      AND event_time >= now() - INTERVAL ${hours} HOUR
    GROUP BY normalizedQueryHash(query), user
    ORDER BY count DESC
    LIMIT ${limit}
  `,

  queriesByTable: (hours = 24, limit = 50) => `
    SELECT arrayJoin(tables) as table_name, count() as query_count,
           round(sum(query_duration_ms), 2) as total_duration_ms,
           round(avg(query_duration_ms), 2) as avg_duration_ms,
           formatReadableSize(sum(memory_usage)) as total_memory
    FROM system.query_log
    WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
      AND event_time >= now() - INTERVAL ${hours} HOUR
      AND length(tables) > 0
    GROUP BY table_name
    ORDER BY query_count DESC
    LIMIT ${limit}
  `,

  queriesErrors: (hours = 24, limit = 50) => `
    SELECT event_time, user, exception_code, exception,
           substring(query, 1, 300) as query
    FROM system.query_log
    WHERE type = 'ExceptionWhileProcessing'
      AND event_time >= now() - INTERVAL ${hours} HOUR
    ORDER BY event_time DESC
    LIMIT ${limit}
  `,

  processes: () => `
    SELECT query_id, user, round(elapsed, 2) as elapsed_sec,
           formatReadableQuantity(read_rows) as read_rows,
           formatReadableSize(memory_usage) as memory_usage,
           substring(query, 1, 200) as query
    FROM system.processes
    WHERE is_cancelled = 0
    ORDER BY elapsed DESC
  `,

  merges: () => `
    SELECT database, table, round(elapsed, 2) as elapsed_sec,
           round(progress * 100, 2) as progress_pct, num_parts,
           formatReadableSize(memory_usage) as memory_usage
    FROM system.merges
    ORDER BY elapsed DESC
  `,

  mutations: (database?: string) => `
    SELECT database, table, mutation_id, command, create_time,
           is_done, parts_to_do, latest_fail_reason
    FROM system.mutations
    WHERE ${database ? `database = '${database}'` : '1=1'}
    ORDER BY create_time DESC
    LIMIT 100
  `,

  disks: () => `
    SELECT name, path, formatReadableSize(free_space) as free_space,
           formatReadableSize(total_space) as total_space,
           round(100 * (1 - toFloat64(free_space) / toFloat64(total_space)), 2) as used_pct
    FROM system.disks
    ORDER BY name
  `,

  partitions: (database?: string, table?: string) => `
    SELECT database, table, partition, count() as parts,
           sum(rows) as total_rows, formatReadableSize(sum(bytes_on_disk)) as total_bytes
    FROM system.parts
    WHERE active = 1
      ${database ? `AND database = '${database}'` : ''}
      ${table ? `AND table = '${table}'` : ''}
    GROUP BY database, table, partition
    ORDER BY total_bytes DESC
    LIMIT 100
  `,

  schemaNullables: (database?: string) => `
    SELECT database, table, name as column, type
    FROM system.columns
    WHERE type LIKE 'Nullable(%)'
      ${database ? `AND database = '${database}'` : "AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"}
    ORDER BY database, table, name
  `,

  schemaOversized: (database?: string) => `
    SELECT database, table, name as column, type
    FROM system.columns
    WHERE type IN ('Int64', 'UInt64', 'Int128', 'UInt128', 'Int256', 'UInt256')
       OR type LIKE 'Nullable(Int64)%' OR type LIKE 'Nullable(UInt64)%'
      ${database ? `AND database = '${database}'` : "AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"}
    ORDER BY database, table, name
  `,

  systemErrors: () => `
    SELECT name, code, value, last_error_time, last_error_message
    FROM system.errors
    WHERE value > 0
    ORDER BY value DESC
  `,

  warnings: () => `SELECT * FROM system.warnings`,

  replicas: () => `
    SELECT database, table, is_leader, is_readonly, future_parts,
           queue_size, total_replicas, active_replicas
    FROM system.replicas
    ORDER BY database, table
  `,
};
