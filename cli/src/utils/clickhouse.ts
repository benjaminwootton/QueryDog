import { createClient, ClickHouseClient } from '@clickhouse/client';
import * as https from 'https';
import { Environment } from './config';

let client: ClickHouseClient | null = null;

export function createClickHouseClient(env: Environment): ClickHouseClient {
  const protocol = env.secure ? 'https' : 'http';
  const url = `${protocol}://${env.host}:${env.port}`;

  const options: Parameters<typeof createClient>[0] = {
    url,
    username: env.user,
    password: env.password,
    database: env.database,
    request_timeout: 30000,
  };

  // Allow self-signed certificates if configured
  const skipVerify = env.skip_tls_verify || env.tls_reject_unauthorized === false;
  if (skipVerify && env.secure) {
    options.http_agent = new https.Agent({
      rejectUnauthorized: false,
    });
  }

  client = createClient(options);

  return client;
}

export function getClient(): ClickHouseClient {
  if (!client) {
    throw new Error('ClickHouse client not initialized. Call createClickHouseClient first.');
  }
  return client;
}

export async function executeQuery<T>(query: string): Promise<T[]> {
  const clickhouse = getClient();
  const result = await clickhouse.query({
    query,
    format: 'JSONEachRow',
  });
  return result.json<T>();
}

export async function closeClient(): Promise<void> {
  if (client) {
    await client.close();
    client = null;
  }
}

// Helper for time range filtering
export function getTimeFilter(hours: number = 24): string {
  return `event_time >= now() - INTERVAL ${hours} HOUR`;
}

// Query builders for system tables
export const SystemQueries = {
  // Tables & Schema
  tables: (database?: string) => `
    SELECT
      name,
      engine,
      total_rows,
      formatReadableSize(total_bytes) as total_bytes,
      partition_key,
      sorting_key,
      primary_key,
      create_table_query
    FROM system.tables
    WHERE database = ${database ? `'${database}'` : 'currentDatabase()'}
      AND engine NOT IN ('View', 'MaterializedView')
    ORDER BY name
  `,

  views: (database?: string) => `
    SELECT
      database,
      name,
      engine,
      as_select,
      metadata_modification_time,
      create_table_query
    FROM system.tables
    WHERE ${database ? `database = '${database}'` : '1=1'}
      AND engine IN ('View', 'MaterializedView')
    ORDER BY database, name
  `,

  materializedViews: (database?: string) => `
    SELECT
      database,
      name,
      engine,
      as_select,
      total_rows,
      formatReadableSize(total_bytes) as total_bytes
    FROM system.tables
    WHERE ${database ? `database = '${database}'` : '1=1'}
      AND engine = 'MaterializedView'
    ORDER BY database, name
  `,

  // Partitions & Parts
  partitions: (database?: string, table?: string) => `
    SELECT
      database,
      table,
      partition,
      name,
      rows,
      formatReadableSize(bytes_on_disk) as bytes_on_disk,
      modification_time,
      active
    FROM system.parts
    WHERE ${database ? `database = '${database}'` : '1=1'}
      ${table ? `AND table = '${table}'` : ''}
      AND active = 1
    ORDER BY modification_time DESC
    LIMIT 100
  `,

  partitionsSummary: (database?: string) => `
    SELECT
      database,
      table,
      partition_id,
      count() as parts_count,
      sum(rows) as total_rows,
      formatReadableSize(sum(bytes_on_disk)) as total_bytes,
      max(modification_time) as latest_modification
    FROM system.parts
    WHERE active = 1
      ${database ? `AND database = '${database}'` : ''}
    GROUP BY database, table, partition_id
    ORDER BY latest_modification DESC
    LIMIT 100
  `,

  parts: (database?: string, table?: string) => `
    SELECT
      database,
      table,
      name,
      partition,
      rows,
      formatReadableSize(bytes_on_disk) as bytes_on_disk,
      formatReadableSize(data_compressed_bytes) as compressed,
      formatReadableSize(data_uncompressed_bytes) as uncompressed,
      marks,
      modification_time,
      level
    FROM system.parts
    WHERE active = 1
      ${database ? `AND database = '${database}'` : ''}
      ${table ? `AND table = '${table}'` : ''}
    ORDER BY modification_time DESC
    LIMIT 100
  `,

  // Mutations
  mutations: (database?: string) => `
    SELECT
      database,
      table,
      mutation_id,
      command,
      create_time,
      is_done,
      parts_to_do,
      latest_fail_reason
    FROM system.mutations
    WHERE ${database ? `database = '${database}'` : '1=1'}
    ORDER BY create_time DESC
    LIMIT 100
  `,

  // Processes & Activity
  processes: () => `
    SELECT
      query_id,
      user,
      round(elapsed, 2) as elapsed,
      formatReadableQuantity(read_rows) as read_rows,
      formatReadableSize(memory_usage) as memory_usage,
      substring(query, 1, 200) as query
    FROM system.processes
    WHERE is_cancelled = 0
    ORDER BY elapsed DESC
  `,

  merges: () => `
    SELECT
      database,
      table,
      elapsed,
      progress,
      num_parts,
      formatReadableQuantity(rows_read) as rows_read,
      formatReadableQuantity(rows_written) as rows_written,
      formatReadableSize(memory_usage) as memory_usage,
      is_mutation
    FROM system.merges
    ORDER BY elapsed DESC
  `,

  // Query Log - All
  queriesAll: (hours: number = 24, limit: number = 100) => `
    SELECT
      event_time,
      query_duration_ms,
      formatReadableQuantity(read_rows) as read_rows,
      formatReadableSize(memory_usage) as memory_usage,
      type,
      user,
      substring(query, 1, 200) as query
    FROM system.query_log
    WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
      AND event_time >= now() - INTERVAL ${hours} HOUR
    ORDER BY event_time DESC
    LIMIT ${limit}
  `,

  // Query Log - Slowest
  queriesSlowest: (hours: number = 24, limit: number = 50) => `
    SELECT
      event_time,
      query_duration_ms,
      formatReadableQuantity(read_rows) as read_rows,
      formatReadableQuantity(written_rows) as written_rows,
      formatReadableSize(memory_usage) as memory_usage,
      type,
      user,
      substring(query, 1, 200) as query
    FROM system.query_log
    WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
      AND event_time >= now() - INTERVAL ${hours} HOUR
    ORDER BY query_duration_ms DESC
    LIMIT ${limit}
  `,

  // Query Log - Fastest
  queriesFastest: (hours: number = 24, limit: number = 50) => `
    SELECT
      event_time,
      query_duration_ms,
      formatReadableQuantity(read_rows) as read_rows,
      formatReadableSize(memory_usage) as memory_usage,
      user,
      substring(query, 1, 200) as query
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND event_time >= now() - INTERVAL ${hours} HOUR
      AND query_duration_ms > 0
    ORDER BY query_duration_ms ASC
    LIMIT ${limit}
  `,

  // Query Log - Most Rows Read
  queriesRowsRead: (hours: number = 24, limit: number = 50) => `
    SELECT
      event_time,
      read_rows,
      formatReadableQuantity(read_rows) as read_rows_fmt,
      query_duration_ms,
      formatReadableSize(memory_usage) as memory_usage,
      user,
      substring(query, 1, 200) as query
    FROM system.query_log
    WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
      AND event_time >= now() - INTERVAL ${hours} HOUR
    ORDER BY read_rows DESC
    LIMIT ${limit}
  `,

  // Query Log - Highest Memory
  queriesHighestMemory: (hours: number = 24, limit: number = 50) => `
    SELECT
      event_time,
      formatReadableSize(memory_usage) as memory_usage,
      query_duration_ms,
      formatReadableQuantity(read_rows) as read_rows,
      type,
      user,
      substring(query, 1, 200) as query
    FROM system.query_log
    WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
      AND event_time >= now() - INTERVAL ${hours} HOUR
    ORDER BY memory_usage DESC
    LIMIT ${limit}
  `,

  // Query Log - Most Frequent (Grouped)
  queriesGrouped: (hours: number = 24, limit: number = 50) => `
    SELECT
      substring(query, 1, 200) as example_query,
      user,
      count() as count,
      round(sum(query_duration_ms), 2) as total_duration,
      round(avg(query_duration_ms), 2) as avg_duration,
      max(query_duration_ms) as max_duration,
      formatReadableSize(sum(memory_usage)) as total_memory,
      formatReadableSize(avg(memory_usage)) as avg_memory,
      min(event_time) as first_seen,
      max(event_time) as last_seen
    FROM system.query_log
    WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
      AND event_time >= now() - INTERVAL ${hours} HOUR
    GROUP BY normalizedQueryHash(query), user
    ORDER BY count DESC
    LIMIT ${limit}
  `,

  // Query Log - By Table
  queriesByTable: (hours: number = 24, limit: number = 50) => `
    SELECT
      arrayJoin(tables) as table_name,
      count() as count,
      round(sum(query_duration_ms), 2) as total_duration,
      round(avg(query_duration_ms), 2) as avg_duration,
      formatReadableSize(sum(memory_usage)) as total_memory,
      formatReadableQuantity(sum(read_rows)) as total_read_rows
    FROM system.query_log
    WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
      AND event_time >= now() - INTERVAL ${hours} HOUR
      AND length(tables) > 0
    GROUP BY table_name
    ORDER BY count DESC
    LIMIT ${limit}
  `,

  // Errors
  queriesErrors: (hours: number = 24, limit: number = 50) => `
    SELECT
      event_time,
      query_duration_ms,
      user,
      exception_code,
      exception,
      substring(query, 1, 200) as query
    FROM system.query_log
    WHERE type = 'ExceptionWhileProcessing'
      AND event_time >= now() - INTERVAL ${hours} HOUR
    ORDER BY event_time DESC
    LIMIT ${limit}
  `,

  // Disks & Storage
  disks: () => `
    SELECT
      name,
      path,
      formatReadableSize(free_space) as free_space_readable,
      formatReadableSize(total_space) as total_space_readable,
      if(total_space > 0, round(100 * (1 - toFloat64(free_space) / toFloat64(total_space)), 2), 0) as used_pct,
      type
    FROM system.disks
    ORDER BY name
  `,

  storagePolicies: () => `
    SELECT
      policy_name,
      volume_name,
      volume_priority,
      disks,
      max_data_part_size,
      move_factor
    FROM system.storage_policies
    ORDER BY policy_name, volume_priority
  `,

  // Users & Security
  users: () => `
    SELECT
      name,
      storage,
      auth_type,
      host_ip,
      host_names,
      default_roles_all,
      grantees_any
    FROM system.users
    ORDER BY name
  `,

  roles: () => `
    SELECT
      name,
      storage
    FROM system.roles
    ORDER BY name
  `,

  grants: () => `
    SELECT
      user_name,
      role_name,
      access_type,
      database,
      table,
      column,
      grant_option
    FROM system.grants
    ORDER BY user_name, role_name
  `,

  quotas: () => `
    SELECT
      name,
      id,
      storage,
      keys,
      durations,
      apply_to_all,
      apply_to_list,
      apply_to_except
    FROM system.quotas
    ORDER BY name
  `,

  // Metrics
  metrics: () => `
    SELECT
      metric,
      value,
      description
    FROM system.metrics
    ORDER BY metric
  `,

  asyncMetrics: () => `
    SELECT
      metric,
      value,
      description
    FROM system.asynchronous_metrics
    ORDER BY metric
  `,

  events: () => `
    SELECT
      event,
      value,
      description
    FROM system.events
    WHERE value > 0
    ORDER BY value DESC
    LIMIT 100
  `,

  errors: () => `
    SELECT
      name,
      code,
      value,
      last_error_time,
      last_error_message,
      last_error_trace,
      remote
    FROM system.errors
    WHERE value > 0
    ORDER BY value DESC
  `,

  warnings: () => `
    SELECT * FROM system.warnings
  `,

  // Cluster
  clusters: () => `
    SELECT
      cluster,
      shard_num,
      shard_weight,
      replica_num,
      host_name,
      host_address,
      port,
      is_local
    FROM system.clusters
    ORDER BY cluster, shard_num, replica_num
  `,

  replicas: () => `
    SELECT
      database,
      table,
      is_leader,
      is_readonly,
      future_parts,
      parts_to_check,
      queue_size,
      inserts_in_queue,
      merges_in_queue,
      log_pointer,
      total_replicas,
      active_replicas
    FROM system.replicas
    ORDER BY database, table
  `,

  replicationQueue: () => `
    SELECT
      database,
      table,
      replica_name,
      type,
      create_time,
      source_replica,
      new_part_name,
      is_currently_executing,
      num_tries,
      last_exception
    FROM system.replication_queue
    ORDER BY create_time DESC
    LIMIT 100
  `,

  zookeeper: (path: string = '/') => `
    SELECT
      name,
      path,
      value,
      czxid,
      mzxid,
      ctime,
      mtime,
      numChildren
    FROM system.zookeeper
    WHERE path = '${path}'
  `,

  // Background Operations
  backgroundJobs: () => `
    SELECT
      id,
      name,
      status,
      error,
      start_time,
      end_time,
      num_files,
      formatReadableSize(total_size) as total_size,
      formatReadableSize(uncompressed_size) as uncompressed_size,
      formatReadableSize(compressed_size) as compressed_size
    FROM system.backups
    ORDER BY start_time DESC
    LIMIT 50
  `,

  asyncInserts: () => `
    SELECT
      query,
      database,
      table,
      format,
      first_update,
      formatReadableSize(total_bytes) as total_bytes
    FROM system.asynchronous_inserts
    ORDER BY first_update DESC
    LIMIT 50
  `,

  queryCache: () => `
    SELECT
      query,
      result_size,
      stale,
      shared,
      compressed,
      expires_at,
      key_hash
    FROM system.query_cache
    ORDER BY expires_at DESC
    LIMIT 50
  `,

  viewRefreshes: () => `
    SELECT
      database,
      view,
      status,
      last_refresh_time,
      next_refresh_time,
      last_refresh_replica,
      exception
    FROM system.view_refreshes
    ORDER BY last_refresh_time DESC
  `,

  // Indexes
  dataSkippingIndexes: (database?: string, table?: string) => `
    SELECT
      database,
      table,
      name,
      type_full,
      expr,
      granularity,
      formatReadableSize(data_compressed_bytes) as size
    FROM system.data_skipping_indices
    WHERE ${database ? `database = '${database}'` : '1=1'}
      ${table ? `AND table = '${table}'` : ''}
    ORDER BY database, table, name
  `,

  projections: (database?: string) => `
    SELECT
      database,
      table,
      name,
      partition,
      rows,
      formatReadableSize(bytes_on_disk) as bytes_on_disk,
      formatReadableSize(data_compressed_bytes) as compressed_bytes,
      formatReadableSize(data_uncompressed_bytes) as uncompressed_bytes,
      marks
    FROM system.projection_parts
    WHERE active = 1
      AND ${database ? `database = '${database}'` : '1=1'}
    ORDER BY database, table, name
  `,

  // Dictionaries
  dictionaries: () => `
    SELECT
      database,
      name,
      status,
      origin,
      type,
      key,
      element_count,
      formatReadableSize(bytes_allocated) as bytes_allocated,
      hit_rate,
      query_count,
      last_exception
    FROM system.dictionaries
    ORDER BY database, name
  `,

  // Databases Summary
  databasesSummary: () => `
    SELECT
      d.name as database,
      d.engine,
      count(distinct t.name) as table_count,
      sum(t.total_rows) as total_rows,
      formatReadableSize(sum(t.total_bytes)) as total_bytes
    FROM system.databases d
    LEFT JOIN system.tables t ON d.name = t.database
    WHERE d.name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
    GROUP BY d.name, d.engine
    ORDER BY d.name
  `,

  // Text Log
  textLog: (hours: number = 1, level?: string, limit: number = 100) => `
    SELECT
      event_time,
      level,
      logger_name,
      message,
      thread_name
    FROM system.text_log
    WHERE event_time >= now() - INTERVAL ${hours} HOUR
      ${level ? `AND level = '${level}'` : ''}
    ORDER BY event_time DESC
    LIMIT ${limit}
  `,

  // Settings
  settings: (search?: string) => `
    SELECT
      name,
      value,
      changed,
      description,
      type
    FROM system.settings
    WHERE ${search ? `name ILIKE '%${search}%' OR description ILIKE '%${search}%'` : '1=1'}
    ORDER BY name
    LIMIT 200
  `,

  // Schema Analysis - Nullable columns with no nulls
  nullableNoNulls: (database?: string, table?: string) => `
    SELECT
      c.database,
      c.table,
      c.name as column,
      c.type,
      formatReadableQuantity(sum(p.rows)) as total_rows
    FROM system.columns c
    JOIN system.parts p ON c.database = p.database AND c.table = p.table
    WHERE c.type LIKE 'Nullable(%)'
      AND p.active = 1
      ${database ? `AND c.database = '${database}'` : "AND c.database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"}
      ${table ? `AND c.table = '${table}'` : ''}
    GROUP BY c.database, c.table, c.name, c.type
    ORDER BY c.database, c.table, c.name
  `,

  // Schema Analysis - Oversized integer columns
  oversizedColumns: (database?: string) => `
    SELECT
      database,
      table,
      name as column,
      type,
      formatReadableQuantity(sum(rows)) as total_rows
    FROM system.columns c
    JOIN system.parts p USING (database, table)
    WHERE p.active = 1
      AND (
        type IN ('Int64', 'UInt64', 'Int128', 'UInt128', 'Int256', 'UInt256')
        OR type LIKE 'Nullable(Int64)%'
        OR type LIKE 'Nullable(UInt64)%'
      )
      ${database ? `AND database = '${database}'` : "AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"}
    GROUP BY database, table, name, type
    ORDER BY database, table, name
  `,

  // Column statistics for a specific table
  columnStats: (database: string, table: string) => `
    SELECT
      name,
      type,
      compression_codec,
      formatReadableSize(data_compressed_bytes) as compressed_size,
      formatReadableSize(data_uncompressed_bytes) as uncompressed_size,
      if(data_uncompressed_bytes > 0, round(data_compressed_bytes / data_uncompressed_bytes * 100, 2), 0) as compression_ratio
    FROM system.columns
    WHERE database = '${database}' AND table = '${table}'
    ORDER BY data_compressed_bytes DESC
  `,
};
