import express from 'express';
import cors from 'cors';
import { createClient } from '@clickhouse/client';
import dotenv from 'dotenv';
import path from 'path';
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

// Get query log entries
app.get('/api/query-log', async (req, res) => {
  try {
    const { start, end, search, limit = 1000, offset = 0, sortField = 'event_time', sortOrder = 'DESC', filters, rangeFilters } = req.query;

    let whereConditions = ['type != 1']; // Exclude QueryStart events, only show QueryFinish
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = end;
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

    let whereConditions = ['type != 1'];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = end;
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
        avg(memory_usage) as avg_memory,
        max(memory_usage) as max_memory
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

// Get histogram data for a specific field
app.get('/api/query-log/histogram/:field', async (req, res) => {
  try {
    const { field } = req.params;
    const { start, end, limit = 20, search, filters } = req.query;

    const scalarFields = [
      'client_name', 'user', 'type', 'query_kind', 'current_database',
      'exception_code', 'is_initial_query', 'client_hostname'
    ];

    let whereConditions = ['type != 1'];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = end;
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

    let whereConditions = ['type != 1'];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = end;
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
    const { limit = 1000, sortField = 'modification_time', sortOrder = 'DESC' } = req.query;

    const safeSortField = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(sortField) ? sortField : 'modification_time';
    const safeSortOrder = sortOrder === 'ASC' ? 'ASC' : 'DESC';

    const query = `
      SELECT *
      FROM system.parts
      ORDER BY ${safeSortField} ${safeSortOrder}
      LIMIT {limit:UInt32}
    `;

    const result = await client.query({
      query,
      query_params: { limit: parseInt(limit) },
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

// ==================== ACTIVITY ENDPOINTS ====================

// Get system.processes
app.get('/api/processes', async (req, res) => {
  try {
    const query = `SELECT * FROM system.processes ORDER BY elapsed DESC`;
    const result = await client.query({ query, format: 'JSONEachRow' });
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

// Get system.merges
app.get('/api/merges', async (req, res) => {
  try {
    const query = `SELECT * FROM system.merges ORDER BY progress DESC`;
    const result = await client.query({ query, format: 'JSONEachRow' });
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

// Get system.mutations
app.get('/api/mutations', async (req, res) => {
  try {
    const query = `SELECT * FROM system.mutations ORDER BY create_time DESC`;
    const result = await client.query({ query, format: 'JSONEachRow' });
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
    const { start, end, limit = 1000, offset = 0, sortField = 'event_time', sortOrder = 'DESC' } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = end;
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
    const { start, end } = req.query;

    let whereConditions = [];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = end;
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

// ==================== QUERY LOG ENDPOINTS ====================

// Get total count
app.get('/api/query-log/count', async (req, res) => {
  try {
    const { start, end, search, filters, rangeFilters } = req.query;

    let whereConditions = ['type != 1'];
    const params = {};

    if (start) {
      whereConditions.push('event_time >= {start:DateTime}');
      params.start = start;
    }
    if (end) {
      whereConditions.push('event_time <= {end:DateTime}');
      params.end = end;
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

// Serve the React app for any other routes (Express v5 compatible)
app.use((req, res, next) => {
  if (req.method === 'GET' && !req.path.startsWith('/api')) {
    res.sendFile(path.join(__dirname, '../dist/index.html'));
  } else {
    next();
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`QueryDog running on http://localhost:${PORT}`);
});
