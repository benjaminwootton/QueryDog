import express from 'express';
import cors from 'cors';
import { createClient } from '@clickhouse/client';
import dotenv from 'dotenv';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json());

// Serve static files from the dist folder in production
app.use(express.static(path.join(__dirname, '../dist')));

const protocol = process.env.CLICKHOUSE_SECURE === '1' ? 'https' : 'http';
const client = createClient({
  url: `${protocol}://${process.env.CLICKHOUSE_HOST}:${process.env.CLICKHOUSE_PORT_HTTP}`,
  username: process.env.CLICKHOUSE_USER,
  password: process.env.CLICKHOUSE_PASSWORD,
  database: process.env.CLICKHOUSE_DATABASE,
});

// Connection info endpoint
app.get('/api/connection-info', (req, res) => {
  res.json({
    host: process.env.CLICKHOUSE_HOST,
    port: process.env.CLICKHOUSE_PORT_HTTP,
    secure: process.env.CLICKHOUSE_SECURE === '1',
    user: process.env.CLICKHOUSE_USER,
  });
});

// Define which fields are arrays for proper filtering
const ARRAY_FIELDS = [
  'databases', 'tables', 'columns', 'partitions', 'projections', 'views',
  'used_functions', 'used_aggregate_functions', 'used_aggregate_function_combinators',
  'used_database_engines', 'used_data_type_families', 'used_dictionaries',
  'used_formats', 'used_storages', 'used_table_functions',
  'used_executable_user_defined_functions', 'used_sql_user_defined_functions',
  'used_row_policies', 'used_privileges', 'missing_privileges', 'thread_ids'
];

// Build filter condition handling both scalar and array fields
function buildFilterCondition(field, values, params, paramIndex) {
  const paramName = `filter_${paramIndex}`;

  if (ARRAY_FIELDS.includes(field)) {
    // For array fields, use hasAny to check if array contains any of the values
    params[paramName] = values;
    return `hasAny(${field}, {${paramName}:Array(String)})`;
  } else {
    // For scalar fields, use IN
    params[paramName] = values;
    return `toString(${field}) IN {${paramName}:Array(String)}`;
  }
}

// Build range filter conditions
function buildRangeFilterConditions(rangeFilters, whereConditions, params) {
  if (!rangeFilters) return;
  const parsed = JSON.parse(rangeFilters);
  for (const [field, range] of Object.entries(parsed)) {
    if (!(/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(field))) continue; // Safety check
    if (range.min !== undefined) {
      const paramName = `${field}_min`;
      whereConditions.push(`${field} >= {${paramName}:UInt64}`);
      params[paramName] = range.min;
    }
    if (range.max !== undefined) {
      const paramName = `${field}_max`;
      whereConditions.push(`${field} <= {${paramName}:UInt64}`);
      params[paramName] = range.max;
    }
  }
}

// Get effective end time - if start and end are the same, extend end to include the full minute
function getEffectiveEndTime(start, end) {
  if (!start || !end || start !== end) return end;
  // Parse the date and add 59 seconds to include the full minute
  const endDate = new Date(end);
  endDate.setSeconds(59);
  return endDate.toISOString().replace('T', ' ').slice(0, 19);
}

// Get query log entries
app.get('/api/query-log', async (req, res) => {
  try {
    const { start, end, search, limit = 1000, offset = 0, sortField = 'event_time', sortOrder = 'DESC', filters, rangeFilters } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }
    if (search) {
      whereConditions.push('(query ILIKE {search:String} OR query_id ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    // Parse and apply field filters with array support
    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          whereConditions.push(buildFilterCondition(field, values, params, paramIndex++));
        }
      }
    }

    // Apply range filters
    buildRangeFilterConditions(rangeFilters, whereConditions, params);

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    // Allow sorting by most columns (alphanumeric only for safety)
    const safeSortField = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(sortField) ? sortField : 'event_time';
    const safeSortOrder = sortOrder === 'ASC' ? 'ASC' : 'DESC';

    const query = `
      SELECT *
      FROM system.query_log
      ${whereClause}
      ORDER BY ${safeSortField} ${safeSortOrder}
      LIMIT {limit:UInt32}
      OFFSET {offset:UInt32}
    `;

    params.limit = parseInt(limit);
    params.offset = parseInt(offset);

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching query log:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get column metadata from system.columns (must be before :field routes)
app.get('/api/query-log/columns', async (req, res) => {
  try {
    const query = `
      SELECT
        name,
        type,
        comment
      FROM system.columns
      WHERE database = 'system' AND table = 'query_log'
      ORDER BY position
    `;

    const result = await client.query({
      query,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching column metadata:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get time series data for chart
app.get('/api/query-log/timeseries', async (req, res) => {
  try {
    const { start, end, bucket = 'minute', search, filters, rangeFilters } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }
    if (search) {
      whereConditions.push('(query ILIKE {search:String} OR query_id ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          whereConditions.push(buildFilterCondition(field, values, params, paramIndex++));
        }
      }
    }

    // Apply range filters
    buildRangeFilterConditions(rangeFilters, whereConditions, params);

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    let truncFunc;
    switch (bucket) {
      case 'second':
        truncFunc = 'toStartOfSecond(event_time_microseconds)';
        break;
      case 'hour':
        truncFunc = 'toStartOfHour(event_time)';
        break;
      default:
        truncFunc = 'toStartOfMinute(event_time)';
    }

    const query = `
      SELECT
        ${truncFunc} as time,
        count() as count,
        avg(query_duration_ms) as avg_duration,
        max(query_duration_ms) as max_duration,
        min(query_duration_ms) as min_duration,
        sum(query_duration_ms) as sum_duration,
        avg(memory_usage) as avg_memory,
        max(memory_usage) as max_memory,
        min(memory_usage) as min_memory,
        sum(memory_usage) as sum_memory,
        avg(read_rows) as avg_read_rows,
        max(read_rows) as max_read_rows,
        min(read_rows) as min_read_rows,
        sum(read_rows) as sum_read_rows,
        avg(written_rows) as avg_written_rows,
        max(written_rows) as max_written_rows,
        min(written_rows) as min_written_rows,
        sum(written_rows) as sum_written_rows,
        avg(result_rows) as avg_result_rows,
        max(result_rows) as max_result_rows,
        min(result_rows) as min_result_rows,
        sum(result_rows) as sum_result_rows
      FROM system.query_log
      ${whereClause}
      GROUP BY time
      ORDER BY time ASC
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching time series:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get stacked time series data for chart (grouped by query_kind)
app.get('/api/query-log/timeseries-stacked', async (req, res) => {
  try {
    const { start, end, bucket = 'minute', search, filters, rangeFilters } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }
    if (search) {
      whereConditions.push('(query ILIKE {search:String} OR query_id ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          whereConditions.push(buildFilterCondition(field, values, params, paramIndex++));
        }
      }
    }

    // Apply range filters
    buildRangeFilterConditions(rangeFilters, whereConditions, params);

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    let truncFunc;
    switch (bucket) {
      case 'second':
        truncFunc = 'toStartOfSecond(event_time_microseconds)';
        break;
      case 'hour':
        truncFunc = 'toStartOfHour(event_time)';
        break;
      default:
        truncFunc = 'toStartOfMinute(event_time)';
    }

    const query = `
      SELECT
        ${truncFunc} as time,
        countIf(query_kind = 'Select') as Select,
        countIf(query_kind = 'Insert') as Insert,
        countIf(query_kind = 'Delete') as Delete,
        countIf(query_kind NOT IN ('Select', 'Insert', 'Delete')) as Other
      FROM system.query_log
      ${whereClause}
      GROUP BY time
      ORDER BY time ASC
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching stacked time series:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get profile events from query_log
app.get('/api/query-log/profile-events', async (req, res) => {
  try {
    const { start, end, limit = 1000, filters, eventColumns, search } = req.query;

    let whereConditions = [];
    const params = { limit: parseInt(limit) };

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }
    if (search) {
      whereConditions.push('(query ILIKE {search:String} OR query_id ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    // Apply field filters
    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    // Parse the event columns to extract from ProfileEvents map
    const eventColumnsList = eventColumns ? eventColumns.split(',') : [];
    const eventSelects = eventColumnsList.map(col => {
      // Sanitize column name (alphanumeric and underscore only)
      const safeName = col.replace(/[^a-zA-Z0-9_]/g, '');
      return `ProfileEvents['${safeName}'] as ${safeName}`;
    }).join(',\n        ');

    const query = `
      SELECT
        event_time,
        query_id,
        ${eventSelects}
      FROM system.query_log
      ${whereClause}
      ORDER BY event_time DESC
      LIMIT {limit:UInt32}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching profile events:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get histogram data for a specific field
app.get('/api/query-log/histogram/:field', async (req, res) => {
  try {
    const { field } = req.params;
    const { start, end, limit = 20, search, filters } = req.query;

    const scalarFields = [
      'client_name', 'user', 'type', 'query_kind', 'current_database',
      'exception_code', 'is_initial_query', 'client_hostname'
    ];

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }
    if (search) {
      whereConditions.push('(query ILIKE {search:String} OR query_id ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [f, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          whereConditions.push(buildFilterCondition(f, values, params, paramIndex++));
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';
    params.limit = parseInt(limit);

    let query;
    if (ARRAY_FIELDS.includes(field)) {
      query = `
        SELECT
          arrayJoin(${field}) as name,
          count() as count
        FROM system.query_log
        ${whereClause}
        GROUP BY name
        HAVING name != ''
        ORDER BY count DESC
        LIMIT {limit:UInt32}
      `;
    } else if (scalarFields.includes(field)) {
      query = `
        SELECT
          toString(${field}) as name,
          count() as count
        FROM system.query_log
        ${whereClause}
        GROUP BY name
        ORDER BY count DESC
        LIMIT {limit:UInt32}
      `;
    } else {
      return res.status(400).json({ error: 'Invalid field' });
    }

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching histogram:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get distinct values for a field (for filters) - supports both scalar and array fields
app.get('/api/query-log/distinct/:field', async (req, res) => {
  try {
    const { field } = req.params;
    const { start, end, limit = 100 } = req.query;

    const scalarFields = [
      'client_name', 'user', 'type', 'query_kind', 'current_database',
      'exception_code', 'is_initial_query', 'client_hostname'
    ];

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';
    params.limit = parseInt(limit);

    let query;
    if (ARRAY_FIELDS.includes(field)) {
      query = `
        SELECT DISTINCT arrayJoin(${field}) as value
        FROM system.query_log
        ${whereClause}
        ORDER BY value
        LIMIT {limit:UInt32}
      `;
    } else if (scalarFields.includes(field)) {
      query = `
        SELECT DISTINCT toString(${field}) as value
        FROM system.query_log
        ${whereClause}
        ORDER BY value
        LIMIT {limit:UInt32}
      `;
    } else {
      return res.status(400).json({ error: 'Invalid field' });
    }

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data.map(row => row.value).filter(v => v !== ''));
  } catch (error) {
    console.error('Error fetching distinct values:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== SYSTEM PARTS ENDPOINTS ====================

// Get system.parts data
app.get('/api/parts', async (req, res) => {
  try {
    const { limit = 2500, offset = 0, sortField = 'modification_time', sortOrder = 'DESC', filters, search } = req.query;

    let whereConditions = [];
    const params = { limit: parseInt(limit), offset: parseInt(offset) };

    // Apply search filter (searches table, database, partition_id, name)
    if (search) {
      whereConditions.push('(table ILIKE {search:String} OR database ILIKE {search:String} OR partition_id ILIKE {search:String} OR name ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    // Apply field filters
    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const safeSortField = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(sortField) ? sortField : 'modification_time';
    const safeSortOrder = sortOrder === 'ASC' ? 'ASC' : 'DESC';

    const query = `
      SELECT *
      FROM system.parts
      ${whereClause}
      ORDER BY ${safeSortField} ${safeSortOrder}
      LIMIT {limit:UInt32} OFFSET {offset:UInt32}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching parts:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.parts columns
app.get('/api/parts/columns', async (req, res) => {
  try {
    const query = `
      SELECT name, type, comment
      FROM system.columns
      WHERE database = 'system' AND table = 'parts'
      ORDER BY position
    `;

    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching parts columns:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.parts count
app.get('/api/parts/count', async (req, res) => {
  try {
    const { filters, search } = req.query;

    let whereConditions = [];
    const params = {};

    // Apply search filter
    if (search) {
      whereConditions.push('(table ILIKE {search:String} OR database ILIKE {search:String} OR partition_id ILIKE {search:String} OR name ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    // Apply field filters
    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const query = `
      SELECT count() as count
      FROM system.parts
      ${whereClause}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json({ count: data[0]?.count || 0 });
  } catch (error) {
    console.error('Error fetching parts count:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get grouped parts by table
app.get('/api/parts/grouped', async (req, res) => {
  try {
    const { filters, search } = req.query;

    let whereConditions = [];
    const params = {};

    // Apply search filter
    if (search) {
      whereConditions.push('(table ILIKE {search:String} OR database ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    // Apply field filters
    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const query = `
      SELECT
        database,
        table,
        count(DISTINCT partition_id) as partition_count,
        count() as part_count,
        sum(rows) as total_rows,
        sum(bytes_on_disk) as total_bytes,
        sum(data_compressed_bytes) as compressed_bytes,
        sum(data_uncompressed_bytes) as uncompressed_bytes,
        round((sum(data_uncompressed_bytes) - sum(data_compressed_bytes)) / nullIf(sum(data_uncompressed_bytes), 0) * 100, 1) as savings_pct,
        max(modification_time) as last_modification_time
      FROM system.parts
      ${whereClause}
      GROUP BY database, table
      ORDER BY total_bytes DESC
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching grouped parts:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get column compression details for a table
app.get('/api/table-compression/:database/:table', async (req, res) => {
  try {
    const { database, table } = req.params;

    const query = `
      SELECT
        name,
        type,
        sum(data_compressed_bytes) AS compressed_bytes,
        sum(data_uncompressed_bytes) AS uncompressed_bytes,
        round((sum(data_uncompressed_bytes) - sum(data_compressed_bytes)) / nullIf(sum(data_uncompressed_bytes), 0) * 100, 1) AS savings_pct
      FROM system.columns
      WHERE database = {database:String} AND table = {table:String}
      GROUP BY name, type
      ORDER BY compressed_bytes DESC
    `;

    const result = await client.query({
      query,
      query_params: { database, table },
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching table compression:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get aggregated partitions data (grouped by partition)
app.get('/api/partitions-summary', async (req, res) => {
  try {
    const { limit = 2500, offset = 0, sortField = 'modification_time', sortOrder = 'DESC', filters, search } = req.query;

    let whereConditions = ['active = 1'];
    const params = { limit: parseInt(limit), offset: parseInt(offset) };

    // Apply search filter
    if (search) {
      whereConditions.push('(table ILIKE {search:String} OR database ILIKE {search:String} OR partition_id ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    // Apply field filters
    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    // Map sort field to aggregated field if needed
    const sortFieldMap = {
      'rows': 'total_rows',
      'bytes_on_disk': 'total_bytes',
      'modification_time': 'latest_modification'
    };
    const mappedSortField = sortFieldMap[sortField] || sortField;
    const safeSortField = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(mappedSortField) ? mappedSortField : 'latest_modification';
    const safeSortOrder = sortOrder === 'ASC' ? 'ASC' : 'DESC';

    const query = `
      SELECT
        database,
        table,
        partition_id,
        partition,
        count() as parts_count,
        sum(rows) as total_rows,
        sum(bytes_on_disk) as total_bytes,
        sum(data_compressed_bytes) as total_compressed,
        sum(data_uncompressed_bytes) as total_uncompressed,
        round((sum(data_uncompressed_bytes) - sum(data_compressed_bytes)) / nullIf(sum(data_uncompressed_bytes), 0) * 100, 1) AS savings_pct,
        max(modification_time) as latest_modification,
        min(min_block_number) as min_block,
        max(max_block_number) as max_block
      FROM system.parts
      ${whereClause}
      GROUP BY database, table, partition_id, partition
      ORDER BY ${safeSortField} ${safeSortOrder}
      LIMIT {limit:UInt32} OFFSET {offset:UInt32}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching partitions summary:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get aggregated partitions count
app.get('/api/partitions-summary/count', async (req, res) => {
  try {
    const { filters, search } = req.query;

    let whereConditions = ['active = 1'];
    const params = {};

    if (search) {
      whereConditions.push('(table ILIKE {search:String} OR database ILIKE {search:String} OR partition_id ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const query = `
      SELECT count() as count FROM (
        SELECT database, table, partition_id
        FROM system.parts
        ${whereClause}
        GROUP BY database, table, partition_id
      )
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json({ count: data[0]?.count || 0 });
  } catch (error) {
    console.error('Error fetching partitions summary count:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get partitions summary columns (virtual columns for the aggregated view)
app.get('/api/partitions-summary/columns', async (req, res) => {
  try {
    // Return virtual column definitions for the aggregated view
    const columns = [
      { name: 'database', type: 'String', comment: 'Database name' },
      { name: 'table', type: 'String', comment: 'Table name' },
      { name: 'partition_id', type: 'String', comment: 'Partition ID' },
      { name: 'partition', type: 'String', comment: 'Partition value' },
      { name: 'parts_count', type: 'UInt64', comment: 'Number of parts in partition' },
      { name: 'total_rows', type: 'UInt64', comment: 'Total rows in partition' },
      { name: 'total_bytes', type: 'UInt64', comment: 'Total bytes on disk' },
      { name: 'total_compressed', type: 'UInt64', comment: 'Total compressed bytes' },
      { name: 'total_uncompressed', type: 'UInt64', comment: 'Total uncompressed bytes' },
      { name: 'savings_pct', type: 'Float64', comment: 'Compression savings percentage' },
      { name: 'latest_modification', type: 'DateTime', comment: 'Latest modification time' },
      { name: 'min_block', type: 'UInt64', comment: 'Minimum block number' },
      { name: 'max_block', type: 'UInt64', comment: 'Maximum block number' },
    ];
    res.json(columns);
  } catch (error) {
    console.error('Error fetching partitions summary columns:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.partitions data
app.get('/api/partitions', async (req, res) => {
  try {
    const { limit = 2500, offset = 0, sortField = 'modification_time', sortOrder = 'DESC', filters, search } = req.query;

    let whereConditions = [];
    const params = { limit: parseInt(limit), offset: parseInt(offset) };

    // Apply search filter (searches table, database, partition_id, name)
    if (search) {
      whereConditions.push('(table ILIKE {search:String} OR database ILIKE {search:String} OR partition_id ILIKE {search:String} OR name ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    // Apply field filters
    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const safeSortField = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(sortField) ? sortField : 'modification_time';
    const safeSortOrder = sortOrder === 'ASC' ? 'ASC' : 'DESC';

    const query = `
      SELECT *
      FROM system.parts
      ${whereClause}
      ORDER BY ${safeSortField} ${safeSortOrder}
      LIMIT {limit:UInt32} OFFSET {offset:UInt32}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching partitions:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.partitions count
app.get('/api/partitions/count', async (req, res) => {
  try {
    const { filters, search } = req.query;

    let whereConditions = [];
    const params = {};

    // Apply search filter
    if (search) {
      whereConditions.push('(table ILIKE {search:String} OR database ILIKE {search:String} OR partition_id ILIKE {search:String} OR name ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    // Apply field filters
    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const query = `
      SELECT count() as count
      FROM system.parts
      ${whereClause}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json({ count: data[0]?.count || 0 });
  } catch (error) {
    console.error('Error fetching partitions count:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.partitions columns
app.get('/api/partitions/columns', async (req, res) => {
  try {
    const query = `
      SELECT name, type, comment
      FROM system.columns
      WHERE database = 'system' AND table = 'parts'
      ORDER BY position
    `;

    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching partitions columns:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get parts for a specific partition
app.get('/api/partition-parts/:database/:table/:partitionId', async (req, res) => {
  try {
    const { database, table, partitionId } = req.params;
    const { activeOnly = '1' } = req.query;

    const query = `
      SELECT
        name,
        rows,
        bytes_on_disk,
        data_compressed_bytes,
        data_uncompressed_bytes,
        marks,
        modification_time,
        min_block_number,
        max_block_number,
        level,
        primary_key_bytes_in_memory,
        active
      FROM system.parts
      WHERE database = {database:String}
        AND table = {table:String}
        AND partition_id = {partitionId:String}
        ${activeOnly === '1' ? 'AND active = 1' : ''}
      ORDER BY modification_time DESC
    `;

    const result = await client.query({
      query,
      query_params: { database, table, partitionId },
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching partition parts:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get partition details for a specific table
app.get('/api/table-partitions/:database/:table', async (req, res) => {
  try {
    const { database, table } = req.params;
    const { activeOnly = '1' } = req.query;

    const params = { database, table };
    const activeFilter = activeOnly === '1' ? 'AND active = 1' : '';

    const query = `
      SELECT
        partition_id,
        count() as parts_count,
        sum(rows) as total_rows,
        sum(bytes_on_disk) as total_bytes,
        min(min_block_number) as min_block,
        max(max_block_number) as max_block,
        min(modification_time) as oldest_part,
        max(modification_time) as newest_part
      FROM system.parts
      WHERE database = {database:String} AND table = {table:String} ${activeFilter}
      GROUP BY partition_id
      ORDER BY partition_id
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching table partitions:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get distinct values for system.parts field (for filters)
app.get('/api/parts/distinct/:field', async (req, res) => {
  try {
    const { field } = req.params;
    const { limit = 100 } = req.query;

    const allowedFields = ['database', 'table', 'partition_id', 'part_type', 'active', 'disk_name'];

    if (!allowedFields.includes(field)) {
      return res.status(400).json({ error: 'Invalid field' });
    }

    const params = { limit: parseInt(limit) };

    const query = `
      SELECT DISTINCT toString(${field}) as value
      FROM system.parts
      ORDER BY value
      LIMIT {limit:UInt32}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data.map(row => row.value).filter(v => v !== ''));
  } catch (error) {
    console.error('Error fetching parts distinct values:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get histogram data for system.parts field
app.get('/api/parts/histogram/:field', async (req, res) => {
  try {
    const { field } = req.params;
    const { limit = 20, filters } = req.query;

    const allowedFields = ['database', 'table', 'partition_id', 'part_type', 'disk_name', 'active'];
    if (!allowedFields.includes(field)) {
      return res.status(400).json({ error: 'Invalid field for histogram' });
    }

    let whereConditions = [];
    const params = { limit: parseInt(limit) };

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [f, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex}`;
          whereConditions.push(`toString(${f}) IN {${paramName}:Array(String)}`);
          params[paramName] = values;
          paramIndex++;
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const query = `
      SELECT
        toString(${field}) as name,
        count() as count
      FROM system.parts
      ${whereClause}
      GROUP BY name
      ORDER BY count DESC
      LIMIT {limit:UInt32}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching parts histogram:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== ACTIVITY ENDPOINTS ====================

// Get system.processes
app.get('/api/processes', async (req, res) => {
  try {
    const { filters } = req.query;
    let whereConditions = [];
    const params = {};

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';
    const query = `SELECT * FROM system.processes ${whereClause} ORDER BY elapsed DESC`;
    const result = await client.query({ query, query_params: params, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching processes:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.processes columns
app.get('/api/processes/columns', async (req, res) => {
  try {
    const query = `
      SELECT name, type, comment
      FROM system.columns
      WHERE database = 'system' AND table = 'processes'
      ORDER BY position
    `;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching processes columns:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get distinct values for processes
app.get('/api/processes/distinct/:field', async (req, res) => {
  try {
    const { field } = req.params;
    const safeField = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(field) ? field : 'user';
    const query = `SELECT DISTINCT toString(${safeField}) as value FROM system.processes WHERE ${safeField} != '' ORDER BY value LIMIT 100`;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data.map(row => row.value).filter(v => v !== ''));
  } catch (error) {
    console.error('Error fetching processes distinct:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.merges
app.get('/api/merges', async (req, res) => {
  try {
    const { filters } = req.query;
    let whereConditions = [];
    const params = {};

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';
    const query = `SELECT * FROM system.merges ${whereClause} ORDER BY progress DESC`;
    const result = await client.query({ query, query_params: params, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching merges:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.merges columns
app.get('/api/merges/columns', async (req, res) => {
  try {
    const query = `
      SELECT name, type, comment
      FROM system.columns
      WHERE database = 'system' AND table = 'merges'
      ORDER BY position
    `;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching merges columns:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get distinct values for merges
app.get('/api/merges/distinct/:field', async (req, res) => {
  try {
    const { field } = req.params;
    const safeField = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(field) ? field : 'database';
    const query = `SELECT DISTINCT toString(${safeField}) as value FROM system.merges WHERE ${safeField} != '' ORDER BY value LIMIT 100`;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data.map(row => row.value).filter(v => v !== ''));
  } catch (error) {
    console.error('Error fetching merges distinct:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.mutations
app.get('/api/mutations', async (req, res) => {
  try {
    const { filters } = req.query;
    let whereConditions = [];
    const params = {};

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';
    const query = `SELECT * FROM system.mutations ${whereClause} ORDER BY create_time DESC`;
    const result = await client.query({ query, query_params: params, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching mutations:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.mutations columns
app.get('/api/mutations/columns', async (req, res) => {
  try {
    const query = `
      SELECT name, type, comment
      FROM system.columns
      WHERE database = 'system' AND table = 'mutations'
      ORDER BY position
    `;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching mutations columns:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get distinct values for mutations
app.get('/api/mutations/distinct/:field', async (req, res) => {
  try {
    const { field } = req.params;
    const safeField = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(field) ? field : 'database';
    const query = `SELECT DISTINCT toString(${safeField}) as value FROM system.mutations WHERE ${safeField} != '' ORDER BY value LIMIT 100`;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data.map(row => row.value).filter(v => v !== ''));
  } catch (error) {
    console.error('Error fetching mutations distinct:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== VIEW REFRESHES ENDPOINTS ====================

// Get system.view_refreshes
app.get('/api/view-refreshes', async (req, res) => {
  try {
    const { filters } = req.query;
    let whereConditions = [];
    let params = {};

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (Array.isArray(values) && values.length > 0) {
          const safeField = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(field) ? field : null;
          if (safeField) {
            whereConditions.push(`toString(${safeField}) IN ({${safeField}_values:Array(String)})`);
            params[`${safeField}_values`] = values;
          }
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';
    const query = `SELECT * FROM system.view_refreshes ${whereClause} ORDER BY next_refresh_time ASC`;
    const result = await client.query({ query, query_params: params, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching view_refreshes:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.view_refreshes columns
app.get('/api/view-refreshes/columns', async (req, res) => {
  try {
    const query = `
      SELECT name, type, comment
      FROM system.columns
      WHERE database = 'system' AND table = 'view_refreshes'
      ORDER BY position
    `;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching view_refreshes columns:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get distinct values for view_refreshes
app.get('/api/view-refreshes/distinct/:field', async (req, res) => {
  try {
    const { field } = req.params;
    const safeField = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(field) ? field : 'database';
    const query = `SELECT DISTINCT toString(${safeField}) as value FROM system.view_refreshes WHERE ${safeField} != '' ORDER BY value LIMIT 100`;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data.map(row => row.value).filter(v => v !== ''));
  } catch (error) {
    console.error('Error fetching view_refreshes distinct:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== DATABASE BROWSER ENDPOINTS ====================

// Get all databases
app.get('/api/browser/databases', async (req, res) => {
  try {
    // Filter out lowercase information_schema (duplicate of INFORMATION_SCHEMA)
    const query = `SELECT name, engine, data_path, metadata_path, uuid FROM system.databases WHERE name != 'information_schema' ORDER BY name`;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching databases:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get tables for a database
app.get('/api/browser/tables/:database', async (req, res) => {
  try {
    const { database } = req.params;
    const query = `
      SELECT name, engine, total_rows, total_bytes, metadata_modification_time
      FROM system.tables
      WHERE database = {database:String}
      ORDER BY name
    `;
    const result = await client.query({
      query,
      query_params: { database },
      format: 'JSONEachRow'
    });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching tables:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get partitions for a table (aggregated from system.parts)
app.get('/api/browser/partitions/:database/:table', async (req, res) => {
  try {
    const { database, table } = req.params;
    const query = `
      SELECT
        partition_id,
        partition,
        count() as part_count,
        sum(rows) as total_rows,
        sum(bytes_on_disk) as total_bytes,
        min(min_time) as min_time,
        max(max_time) as max_time
      FROM system.parts
      WHERE database = {database:String} AND table = {table:String} AND active = 1
      GROUP BY partition_id, partition
      ORDER BY partition_id
    `;
    const result = await client.query({
      query,
      query_params: { database, table },
      format: 'JSONEachRow'
    });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching partitions:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get columns for a table
app.get('/api/browser/columns/:database/:table', async (req, res) => {
  try {
    const { database, table } = req.params;
    const query = `
      SELECT
        name,
        type,
        default_kind,
        default_expression,
        comment,
        is_in_partition_key,
        is_in_sorting_key,
        is_in_primary_key,
        compression_codec
      FROM system.columns
      WHERE database = {database:String} AND table = {table:String}
      ORDER BY position
    `;
    const result = await client.query({
      query,
      query_params: { database, table },
      format: 'JSONEachRow'
    });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching columns:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get parts for a partition
app.get('/api/browser/parts/:database/:table/:partition', async (req, res) => {
  try {
    const { database, table, partition } = req.params;
    const query = `
      SELECT
        name,
        partition_id,
        rows,
        bytes_on_disk,
        data_compressed_bytes,
        data_uncompressed_bytes,
        marks,
        modification_time,
        min_time,
        max_time,
        level,
        primary_key_bytes_in_memory
      FROM system.parts
      WHERE database = {database:String} AND table = {table:String} AND partition_id = {partition:String} AND active = 1
      ORDER BY name
    `;
    const result = await client.query({
      query,
      query_params: { database, table, partition },
      format: 'JSONEachRow'
    });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching parts:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== PROJECTIONS ENDPOINTS ====================

// Get all projections (system-wide)
app.get('/api/projections', async (req, res) => {
  try {
    const { filters, search } = req.query;

    let whereConditions = [];
    const params = {};

    if (filters) {
      const parsed = JSON.parse(filters);
      Object.entries(parsed).forEach(([field, values], idx) => {
        if (!values || !Array.isArray(values) || values.length === 0) return;
        const condition = buildFilterCondition(field, values, params, idx);
        whereConditions.push(condition);
      });
    }

    if (search) {
      params.search = `%${search}%`;
      whereConditions.push(`(database ILIKE {search:String} OR table ILIKE {search:String} OR name ILIKE {search:String})`);
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const query = `
      SELECT
        database,
        table,
        name,
        type,
        sorting_key,
        query
      FROM system.projections
      ${whereClause}
      ORDER BY database, table, name
    `;
    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow'
    });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching projections:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get projection parts (for a specific projection)
app.get('/api/projection-parts/:database/:table/:projection', async (req, res) => {
  try {
    const { database, table, projection } = req.params;
    const query = `
      SELECT
        name,
        part_name,
        partition_id,
        rows,
        bytes_on_disk,
        data_compressed_bytes,
        data_uncompressed_bytes,
        marks,
        modification_time,
        parent_part_name,
        is_broken
      FROM system.projection_parts
      WHERE database = {database:String} AND table = {table:String} AND name = {projection:String} AND active = 1
      ORDER BY part_name
    `;
    const result = await client.query({
      query,
      query_params: { database, table, projection },
      format: 'JSONEachRow'
    });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching projection parts:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get projections for a table (browser endpoint)
app.get('/api/browser/projections/:database/:table', async (req, res) => {
  try {
    const { database, table } = req.params;
    const query = `
      SELECT
        name,
        type,
        sorting_key,
        query,
        toString(storage_policy) as storage_policy,
        toString(partition_key) as partition_key,
        toString(primary_key) as primary_key
      FROM system.projections
      WHERE database = {database:String} AND table = {table:String}
      ORDER BY name
    `;
    const result = await client.query({
      query,
      query_params: { database, table },
      format: 'JSONEachRow'
    });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching projections:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get projection parts for a projection
app.get('/api/browser/projection-parts/:database/:table/:projection', async (req, res) => {
  try {
    const { database, table, projection } = req.params;
    const query = `
      SELECT
        name,
        part_name,
        partition_id,
        rows,
        bytes_on_disk,
        data_compressed_bytes,
        data_uncompressed_bytes,
        marks,
        modification_time,
        parent_part_name,
        is_broken
      FROM system.projection_parts
      WHERE database = {database:String} AND table = {table:String} AND name = {projection:String} AND active = 1
      ORDER BY part_name
    `;
    const result = await client.query({
      query,
      query_params: { database, table, projection },
      format: 'JSONEachRow'
    });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching projection parts:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== DATA SKIPPING INDEXES ENDPOINTS ====================

// Get all data skipping indexes (system-wide)
app.get('/api/indexes', async (req, res) => {
  try {
    const { filters, search } = req.query;

    let whereConditions = [];
    const params = {};

    // Parse and apply filters
    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          whereConditions.push(buildFilterCondition(field, values, params, paramIndex++));
        }
      }
    }

    // Apply search
    if (search) {
      whereConditions.push('(database ILIKE {search:String} OR table ILIKE {search:String} OR name ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const query = `
      SELECT
        database,
        table,
        name,
        type,
        type_full,
        expr,
        granularity,
        data_compressed_bytes,
        data_uncompressed_bytes,
        marks
      FROM system.data_skipping_indices
      ${whereClause}
      ORDER BY database, table, name
    `;
    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow'
    });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching data skipping indexes:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get data skipping indexes for a table (browser endpoint)
app.get('/api/browser/indexes/:database/:table', async (req, res) => {
  try {
    const { database, table } = req.params;
    const query = `
      SELECT
        name,
        type,
        type_full,
        expr,
        granularity,
        data_compressed_bytes,
        data_uncompressed_bytes,
        marks
      FROM system.data_skipping_indices
      WHERE database = {database:String} AND table = {table:String}
      ORDER BY name
    `;
    const result = await client.query({
      query,
      query_params: { database, table },
      format: 'JSONEachRow'
    });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching data skipping indexes:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== METRICS ENDPOINTS ====================

// Get system.metrics
app.get('/api/metrics', async (req, res) => {
  try {
    const query = `SELECT * FROM system.metrics ORDER BY metric`;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching metrics:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.asynchronous_metrics
app.get('/api/async-metrics', async (req, res) => {
  try {
    const query = `SELECT * FROM system.asynchronous_metrics ORDER BY metric`;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching async metrics:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.events
app.get('/api/events', async (req, res) => {
  try {
    const query = `SELECT event, value, description FROM system.events ORDER BY event`;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching events:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== INSTANCE ENDPOINTS ====================

// Get system.users
app.get('/api/users', async (req, res) => {
  try {
    const query = `SELECT * FROM system.users ORDER BY name`;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching users:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.users columns
app.get('/api/users/columns', async (req, res) => {
  try {
    const query = `
      SELECT name, type, comment
      FROM system.columns
      WHERE database = 'system' AND table = 'users'
      ORDER BY position
    `;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching users columns:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.settings
app.get('/api/settings', async (req, res) => {
  try {
    const query = `SELECT * FROM system.settings ORDER BY name`;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching settings:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.settings columns
app.get('/api/settings/columns', async (req, res) => {
  try {
    const query = `
      SELECT name, type, comment
      FROM system.columns
      WHERE database = 'system' AND table = 'settings'
      ORDER BY position
    `;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching settings columns:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== EXPLAIN PLAN ENDPOINT ====================

// Run EXPLAIN on a query
app.post('/api/explain', async (req, res) => {
  try {
    const { query: userQuery } = req.body;

    if (!userQuery) {
      return res.status(400).json({ error: 'Query is required' });
    }

    // Run EXPLAIN on the query
    const explainQuery = `EXPLAIN ${userQuery}`;

    const result = await client.query({
      query: explainQuery,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error running explain:', error);
    res.status(500).json({ error: error.message });
  }
});

// Run different EXPLAIN types
app.post('/api/explain/:type', async (req, res) => {
  try {
    const { type } = req.params;
    const { query: userQuery } = req.body;

    if (!userQuery) {
      return res.status(400).json({ error: 'Query is required' });
    }

    // Supported explain types
    const explainTypes = {
      'plan': 'EXPLAIN',
      'indexes': 'EXPLAIN indexes = 1',
      'actions': 'EXPLAIN actions = 1',
      'pipeline': 'EXPLAIN PIPELINE',
      'ast': 'EXPLAIN AST',
      'syntax': 'EXPLAIN SYNTAX',
      'estimate': 'EXPLAIN ESTIMATE',
    };

    const explainPrefix = explainTypes[type];
    if (!explainPrefix) {
      return res.status(400).json({ error: `Invalid explain type: ${type}` });
    }

    const explainQuery = `${explainPrefix} ${userQuery}`;

    // AST and SYNTAX return plain text, not structured data
    // Use TabSeparated format and return as array of lines
    if (type === 'ast' || type === 'syntax') {
      const result = await client.query({
        query: explainQuery,
        format: 'TabSeparatedRaw',
      });
      const text = await result.text();
      // Return as array of objects with 'explain' key for consistency
      const lines = text.split('\n').filter(line => line.trim());
      res.json(lines.map(line => ({ explain: line })));
    } else {
      const result = await client.query({
        query: explainQuery,
        format: 'JSONEachRow',
      });
      const data = await result.json();
      res.json(data);
    }
  } catch (error) {
    console.error(`Error running explain ${req.params.type}:`, error);
    res.status(500).json({ error: error.message });
  }
});

// Execute a query and return results
app.post('/api/query', async (req, res) => {
  try {
    const { query: userQuery, limit = 1000 } = req.body;

    if (!userQuery) {
      return res.status(400).json({ error: 'Query is required' });
    }

    // Safety: don't allow dangerous operations
    const upperQuery = userQuery.trim().toUpperCase();
    const dangerousKeywords = ['DROP', 'TRUNCATE', 'DELETE', 'ALTER', 'DETACH', 'ATTACH', 'RENAME', 'KILL'];
    const isDangerous = dangerousKeywords.some(kw => upperQuery.startsWith(kw));

    if (isDangerous) {
      return res.status(403).json({ error: 'Dangerous operations are not allowed through this interface' });
    }

    // Check if query already has a FORMAT clause
    const hasFormat = upperQuery.includes('FORMAT');

    // Add LIMIT only for SELECT queries that don't already have one
    // Don't add LIMIT to EXPLAIN, SHOW, DESCRIBE, or queries with FORMAT or subqueries at the end
    let finalQuery = userQuery;
    const shouldAddLimit = upperQuery.startsWith('SELECT')
      && !upperQuery.includes('LIMIT')
      && !hasFormat
      && !upperQuery.endsWith(')'); // Avoid adding LIMIT after closing parenthesis (subqueries)

    if (shouldAddLimit) {
      finalQuery = `${userQuery} LIMIT ${limit}`;
    }

    const startTime = Date.now();

    // If query already has FORMAT, don't specify format in client options
    // Strip the FORMAT clause and let the query handle it, or use text mode
    let data;
    if (hasFormat) {
      // Query has its own FORMAT - execute as-is and parse the result
      // Remove the FORMAT clause to let us control the output
      const formatMatch = userQuery.match(/\s+FORMAT\s+\w+\s*$/i);
      if (formatMatch) {
        // Remove FORMAT clause and add our own
        finalQuery = userQuery.replace(/\s+FORMAT\s+\w+\s*$/i, '');
        if (shouldAddLimit) {
          finalQuery = `${finalQuery} LIMIT ${limit}`;
        }
      }
      const result = await client.query({
        query: finalQuery,
        format: 'JSONEachRow',
      });
      data = await result.json();
    } else {
      const result = await client.query({
        query: finalQuery,
        format: 'JSONEachRow',
      });
      data = await result.json();
    }

    const duration = Date.now() - startTime;

    res.json({
      data,
      rowCount: data.length,
      duration,
    });
  } catch (error) {
    console.error('Error executing query:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== PART LOG ENDPOINTS ====================

// Get part_log column metadata (must be before parameterized routes)
app.get('/api/part-log/columns', async (req, res) => {
  try {
    const query = `
      SELECT
        name,
        type,
        comment
      FROM system.columns
      WHERE database = 'system' AND table = 'part_log'
      ORDER BY position
    `;

    const result = await client.query({
      query,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching part_log column metadata:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get part_log entries
app.get('/api/part-log', async (req, res) => {
  try {
    const { start, end, limit = 2500, offset = 0, sortField = 'event_time', sortOrder = 'DESC', filters } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }

    // Apply field filters
    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    // Allow sorting by most columns (alphanumeric only for safety)
    const safeSortField = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(sortField) ? sortField : 'event_time';
    const safeSortOrder = sortOrder === 'ASC' ? 'ASC' : 'DESC';

    const query = `
      SELECT *
      FROM system.part_log
      ${whereClause}
      ORDER BY ${safeSortField} ${safeSortOrder}
      LIMIT {limit:UInt32}
      OFFSET {offset:UInt32}
    `;

    params.limit = parseInt(limit);
    params.offset = parseInt(offset);

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching part log:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get part_log count
app.get('/api/part-log/count', async (req, res) => {
  try {
    const { start, end, filters } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }

    // Apply field filters
    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const query = `
      SELECT count() as total
      FROM system.part_log
      ${whereClause}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json({ total: data[0]?.total || 0 });
  } catch (error) {
    console.error('Error fetching part_log count:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get part_log time series for chart
app.get('/api/part-log/timeseries', async (req, res) => {
  try {
    const { start, end, bucket = 'minute', filters } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    let truncFunc;
    switch (bucket) {
      case 'second':
        // event_time is DateTime (second precision), so just use it directly
        truncFunc = 'event_time';
        break;
      case 'hour':
        truncFunc = 'toStartOfHour(event_time)';
        break;
      default:
        truncFunc = 'toStartOfMinute(event_time)';
    }

    const query = `
      SELECT
        ${truncFunc} as time,
        count() as count,
        sumIf(rows, event_type = 'NewPart') as new_rows,
        sumIf(rows, event_type = 'MergeParts') as merged_rows,
        avg(duration_ms) as avg_duration,
        min(duration_ms) as min_duration,
        max(duration_ms) as max_duration,
        sum(duration_ms) as sum_duration
      FROM system.part_log
      ${whereClause}
      GROUP BY time
      ORDER BY time ASC
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching part_log time series:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get stacked time series for part_log by event_type
app.get('/api/part-log/timeseries-stacked', async (req, res) => {
  try {
    const { start, end, bucket = 'minute', filters } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    let truncFunc;
    switch (bucket) {
      case 'second':
        truncFunc = 'event_time';
        break;
      case 'hour':
        truncFunc = 'toStartOfHour(event_time)';
        break;
      default:
        truncFunc = 'toStartOfMinute(event_time)';
    }

    const query = `
      SELECT
        ${truncFunc} as time,
        countIf(event_type = 'NewPart') as NewPart,
        countIf(event_type = 'MergeParts') as MergeParts,
        countIf(event_type = 'DownloadPart') as DownloadPart,
        countIf(event_type = 'RemovePart') as RemovePart,
        countIf(event_type = 'MutatePart') as MutatePart,
        countIf(event_type NOT IN ('NewPart', 'MergeParts', 'DownloadPart', 'RemovePart', 'MutatePart')) as Other
      FROM system.part_log
      ${whereClause}
      GROUP BY time
      ORDER BY time ASC
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching part_log stacked time series:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get histogram data for part_log field
app.get('/api/part-log/histogram/:field', async (req, res) => {
  try {
    const { field } = req.params;
    const { start, end, limit = 20, filters } = req.query;

    const allowedFields = ['table', 'event_type', 'merge_reason', 'database', 'merge_algorithm'];
    if (!allowedFields.includes(field)) {
      return res.status(400).json({ error: 'Invalid field for histogram' });
    }

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [f, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex}`;
          whereConditions.push(`${f} IN {${paramName}:Array(String)}`);
          params[paramName] = values;
          paramIndex++;
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';
    params.limit = parseInt(limit);

    const query = `
      SELECT
        toString(${field}) as name,
        count() as count
      FROM system.part_log
      ${whereClause}
      GROUP BY name
      ORDER BY count DESC
      LIMIT {limit:UInt32}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching part_log histogram:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get distinct values for part_log field (for filters)
app.get('/api/part-log/distinct/:field', async (req, res) => {
  try {
    const { field } = req.params;
    const { start, end, limit = 100 } = req.query;

    const allowedFields = ['event_type', 'database', 'table', 'part_name', 'partition_id', 'merge_reason', 'merge_algorithm'];

    if (!allowedFields.includes(field)) {
      return res.status(400).json({ error: 'Invalid field' });
    }

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';
    params.limit = parseInt(limit);

    const query = `
      SELECT DISTINCT toString(${field}) as value
      FROM system.part_log
      ${whereClause}
      ORDER BY value
      LIMIT {limit:UInt32}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data.map(row => row.value).filter(v => v !== ''));
  } catch (error) {
    console.error('Error fetching part_log distinct values:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== TEXT LOG ENDPOINTS ====================

// Get text_log column metadata
app.get('/api/text-log/columns', async (req, res) => {
  try {
    const query = `
      SELECT name, type
      FROM system.columns
      WHERE database = 'system' AND table = 'text_log'
      ORDER BY position
    `;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching text_log columns:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get text_log entries
app.get('/api/text-log', async (req, res) => {
  try {
    const { start, end, search, limit = 1000, offset = 0, sortField = 'event_time', sortOrder = 'DESC', filters } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }
    if (search) {
      whereConditions.push('(message ILIKE {search:String} OR logger_name ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    // Validate sort field
    const allowedSortFields = ['event_time', 'level', 'logger_name', 'message', 'thread_name', 'thread_id', 'query_id'];
    const safeSortField = allowedSortFields.includes(sortField) ? sortField : 'event_time';
    const safeSortOrder = sortOrder === 'ASC' ? 'ASC' : 'DESC';

    params.limit = parseInt(limit);
    params.offset = parseInt(offset);

    const query = `
      SELECT *
      FROM system.text_log
      ${whereClause}
      ORDER BY ${safeSortField} ${safeSortOrder}
      LIMIT {limit:UInt32} OFFSET {offset:UInt32}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching text_log:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get text_log count
app.get('/api/text-log/count', async (req, res) => {
  try {
    const { start, end, search, filters } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }
    if (search) {
      whereConditions.push('(message ILIKE {search:String} OR logger_name ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const query = `
      SELECT count() as total
      FROM system.text_log
      ${whereClause}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json({ total: data[0]?.total || 0 });
  } catch (error) {
    console.error('Error fetching text_log count:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get text_log time series for chart
app.get('/api/text-log/timeseries', async (req, res) => {
  try {
    const { start, end, bucket = 'minute', filters } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          const paramName = `filter_${paramIndex++}`;
          params[paramName] = values;
          whereConditions.push(`toString(${field}) IN {${paramName}:Array(String)}`);
        }
      }
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    let truncFunc;
    switch (bucket) {
      case 'second':
        truncFunc = 'toStartOfSecond(event_time)';
        break;
      case 'hour':
        truncFunc = 'toStartOfHour(event_time)';
        break;
      default:
        truncFunc = 'toStartOfMinute(event_time)';
    }

    const query = `
      SELECT
        ${truncFunc} as time,
        count() as count,
        countIf(level = 'Error' OR level = 'Fatal') as errors,
        countIf(level = 'Warning') as warnings
      FROM system.text_log
      ${whereClause}
      GROUP BY time
      ORDER BY time ASC
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching text_log time series:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get distinct values for text_log field (for filters)
app.get('/api/text-log/distinct/:field', async (req, res) => {
  try {
    const { field } = req.params;
    const { start, end, limit = 100 } = req.query;

    const allowedFields = ['level', 'logger_name', 'thread_name', 'query_id'];

    if (!allowedFields.includes(field)) {
      return res.status(400).json({ error: 'Invalid field' });
    }

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';
    params.limit = parseInt(limit);

    const query = `
      SELECT DISTINCT toString(${field}) as value
      FROM system.text_log
      ${whereClause}
      ORDER BY value
      LIMIT {limit:UInt32}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data.map(row => row.value).filter(v => v !== ''));
  } catch (error) {
    console.error('Error fetching text_log distinct values:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== QUERY LOG ENDPOINTS ====================

// Get grouped query log (aggregated by query or normalized_query_hash)
app.get('/api/query-log/grouped', async (req, res) => {
  try {
    const { start, end, search, limit = 1000, sortField = 'count', sortOrder = 'DESC', filters, rangeFilters, normalize } = req.query;
    const useNormalized = normalize === 'true';

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }
    if (search) {
      whereConditions.push('(query ILIKE {search:String} OR query_id ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    // Parse and apply field filters
    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          whereConditions.push(buildFilterCondition(field, values, params, paramIndex++));
        }
      }
    }

    // Apply range filters
    buildRangeFilterConditions(rangeFilters, whereConditions, params);

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    // Define valid sort fields for grouped view
    const validSortFields = ['count', 'total_duration', 'avg_duration', 'max_duration', 'min_duration',
      'total_memory', 'avg_memory', 'max_memory', 'total_read_rows', 'avg_read_rows',
      'total_read_bytes', 'total_result_rows', 'avg_result_rows', 'first_seen', 'last_seen'];
    const safeSortField = validSortFields.includes(sortField) ? sortField : 'count';
    const safeSortOrder = sortOrder === 'ASC' ? 'ASC' : 'DESC';

    const groupByField = useNormalized ? 'normalized_query_hash' : 'query';
    const query = `
      SELECT
        any(query) as example_query,
        ${useNormalized ? 'normalized_query_hash,' : ''}
        any(user) as user,
        any(current_database) as current_database,
        count() as count,
        sum(query_duration_ms) as total_duration,
        avg(query_duration_ms) as avg_duration,
        max(query_duration_ms) as max_duration,
        min(query_duration_ms) as min_duration,
        sum(memory_usage) as total_memory,
        avg(memory_usage) as avg_memory,
        max(memory_usage) as max_memory,
        sum(read_rows) as total_read_rows,
        avg(read_rows) as avg_read_rows,
        sum(read_bytes) as total_read_bytes,
        sum(written_rows) as total_written_rows,
        avg(written_rows) as avg_written_rows,
        sum(result_rows) as total_result_rows,
        avg(result_rows) as avg_result_rows,
        min(event_time) as first_seen,
        max(event_time) as last_seen
      FROM system.query_log
      ${whereClause}
      GROUP BY ${groupByField}
      ORDER BY ${safeSortField} ${safeSortOrder}
      LIMIT {limit:UInt32}
    `;

    params.limit = parseInt(limit);

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching grouped query log:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get total count
app.get('/api/query-log/count', async (req, res) => {
  try {
    const { start, end, search, filters, rangeFilters } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = getEffectiveEndTime(start, end);
    }
    if (search) {
      whereConditions.push('(query ILIKE {search:String} OR query_id ILIKE {search:String})');
      params.search = `%${search}%`;
    }

    if (filters) {
      const parsedFilters = JSON.parse(filters);
      let paramIndex = 0;
      for (const [field, values] of Object.entries(parsedFilters)) {
        if (values && values.length > 0) {
          whereConditions.push(buildFilterCondition(field, values, params, paramIndex++));
        }
      }
    }

    // Apply range filters
    buildRangeFilterConditions(rangeFilters, whereConditions, params);

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const query = `
      SELECT count() as total
      FROM system.query_log
      ${whereClause}
    `;

    const result = await client.query({
      query,
      query_params: params,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    res.json({ total: data[0]?.total || 0 });
  } catch (error) {
    console.error('Error fetching count:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== MY QUERIES ENDPOINTS ====================

// In-memory store for query run statistics
const queryRunStats = new Map();

// Check if queries folder exists
app.get('/api/my-queries/exists', (req, res) => {
  const queriesPath = path.join(process.cwd(), 'queries');
  const exists = fs.existsSync(queriesPath) && fs.statSync(queriesPath).isDirectory();
  res.json({ exists, path: queriesPath });
});

// Get all queries from the queries folder
app.get('/api/my-queries', (req, res) => {
  try {
    const queriesPath = path.join(process.cwd(), 'queries');

    if (!fs.existsSync(queriesPath)) {
      return res.json([]);
    }

    const files = fs.readdirSync(queriesPath).filter(f => f.endsWith('.sql'));
    const queries = files.map(filename => {
      const filePath = path.join(queriesPath, filename);
      const content = fs.readFileSync(filePath, 'utf-8').trim();
      const stats = queryRunStats.get(filename) || { runs: [], lastRun: null, lastDuration: null, lastRowCount: null };

      // Calculate statistics
      const runTimes = stats.runs || [];
      const avgRunTime = runTimes.length > 0
        ? runTimes.reduce((a, b) => a + b, 0) / runTimes.length
        : null;
      const slowestRunTime = runTimes.length > 0
        ? Math.max(...runTimes)
        : null;
      const fastestRunTime = runTimes.length > 0
        ? Math.min(...runTimes)
        : null;

      return {
        filename,
        query: content,
        lastRunTime: stats.lastRun,
        lastDuration: stats.lastDuration,
        lastRowCount: stats.lastRowCount,
        avgRunTime,
        slowestRunTime,
        fastestRunTime,
        runCount: runTimes.length,
      };
    });

    res.json(queries);
  } catch (error) {
    console.error('Error reading queries folder:', error);
    res.status(500).json({ error: error.message });
  }
});

// Run a query from my-queries and track timing
app.post('/api/my-queries/run', async (req, res) => {
  try {
    const { filename, query } = req.body;

    if (!query) {
      return res.status(400).json({ error: 'Query is required' });
    }

    // Safety check - only allow SELECT queries
    const upperQuery = query.trim().toUpperCase();
    if (!upperQuery.startsWith('SELECT')) {
      return res.status(403).json({ error: 'Only SELECT queries are allowed' });
    }

    const startTime = Date.now();

    const result = await client.query({
      query,
      format: 'JSONEachRow',
    });

    const data = await result.json();
    const duration = Date.now() - startTime;

    // Update run statistics
    let updatedStats = null;
    if (filename) {
      const stats = queryRunStats.get(filename) || { runs: [], lastRun: null, lastDuration: null, lastRowCount: null };
      stats.runs.push(duration);
      // Keep only last 100 runs
      if (stats.runs.length > 100) {
        stats.runs = stats.runs.slice(-100);
      }
      stats.lastRun = new Date().toISOString();
      stats.lastDuration = duration;
      stats.lastRowCount = data.length;
      queryRunStats.set(filename, stats);

      // Calculate statistics for response
      const runTimes = stats.runs || [];
      updatedStats = {
        lastRunTime: stats.lastRun,
        lastDuration: stats.lastDuration,
        lastRowCount: stats.lastRowCount,
        avgRunTime: runTimes.length > 0
          ? runTimes.reduce((a, b) => a + b, 0) / runTimes.length
          : null,
        slowestRunTime: runTimes.length > 0
          ? Math.max(...runTimes)
          : null,
        fastestRunTime: runTimes.length > 0
          ? Math.min(...runTimes)
          : null,
        runCount: runTimes.length,
      };
    }

    res.json({
      data,
      rowCount: data.length,
      duration,
      stats: updatedStats,
    });
  } catch (error) {
    console.error('Error running my-query:', error);
    res.status(500).json({ error: error.message });
  }
});

// Reset all run statistics (must be before parameterized route)
app.delete('/api/my-queries/stats', (req, res) => {
  queryRunStats.clear();
  res.json({ success: true });
});

// Clear run statistics for a specific query
app.delete('/api/my-queries/stats/:filename', (req, res) => {
  const { filename } = req.params;
  queryRunStats.delete(filename);
  res.json({ success: true });
});

// Serve the React app for any other routes (Express v5 compatible)
app.use((req, res, next) => {
  if (req.method === 'GET' && !req.path.startsWith('/api')) {
    res.sendFile(path.join(__dirname, '../dist/index.html'));
  } else {
    next();
  }
});

const PORT = process.env.PORT || 8001;
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`QueryDog running on http://0.0.0.0:${PORT}`);
});

// Graceful shutdown handler
function shutdown(signal) {
  console.log(`\n${signal} received, shutting down gracefully...`);
  server.close(() => {
    console.log('HTTP server closed');
    client.close().then(() => {
      console.log('ClickHouse connection closed');
      process.exit(0);
    }).catch(() => {
      process.exit(0);
    });
  });

  // Force exit after 10 seconds if graceful shutdown fails
  setTimeout(() => {
    console.error('Forcing shutdown after timeout');
    process.exit(1);
  }, 10000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
