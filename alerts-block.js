app.get('/api/alerts/exists', (req, res) => {
  const alertsPath = getAlertsPath();
  const exists = fs.existsSync(alertsPath) && fs.statSync(alertsPath).isDirectory();
  res.json({ exists, path: alertsPath });
});

app.get('/api/alerts', (req, res) => {
  try {
    const alertsPath = getAlertsPath();

    if (!fs.existsSync(alertsPath)) {
      return res.json({ alerts: [], path: alertsPath });
    }

    const files = fs.readdirSync(alertsPath).filter(f => f.endsWith('.sql'));
    const aggregates = getAggregatesByFilename('alert');
    const alerts = files.map(filename => {
      const filePath = path.join(alertsPath, filename);
      const content = fs.readFileSync(filePath, 'utf-8').trim();
      const agg = aggregates.get(filename) || {
        avgRunTime: null, fastestRunTime: null, slowestRunTime: null,
        lastRunTime: null, lastDuration: null, lastRowCount: null, runCount: 0,
      };

      return {
        filename,
        query: content,
        description: readDescription(alertsPath, filename),
        lastRunTime: agg.lastRunTime,
        lastDuration: agg.lastDuration,
        lastRowCount: agg.lastRowCount,
        avgRunTime: agg.avgRunTime,
        slowestRunTime: agg.slowestRunTime,
        fastestRunTime: agg.fastestRunTime,
        runCount: agg.runCount,
      };
    });

    res.json({ alerts, path: alertsPath });
  } catch (error) {
    console.error('Error reading alerts folder:', error);
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/alerts/run', async (req, res) => {
  try {
    const { filename, query } = req.body;

    if (!query) {
      return res.status(400).json({ error: 'Query is required' });
    }

    // Strip leading SQL line and block comments before checking the first keyword
    const stripped = query
      .replace(/\/\*[\s\S]*?\*\//g, '')
      .split('\n')
      .map(line => line.replace(/^\s*--.*$/, ''))
      .join('\n')
      .trim();
    const upperQuery = stripped.toUpperCase();
    if (!upperQuery.startsWith('SELECT') && !upperQuery.startsWith('WITH')) {
      return res.status(403).json({ error: 'Only SELECT and WITH (CTE) queries are allowed for alerts' });
    }

    const startTime = Date.now();
    const result = await client.query({
      query,
      format: 'JSON',
      clickhouse_settings: { max_execution_time: 0 },
      request_timeout: 3600000,
    });

    const jsonResponse = await result.json();
    const data = jsonResponse.data || [];
    const rowCount = jsonResponse.rows || data.length;
    const stats = jsonResponse.statistics || {};
    const duration = stats.elapsed ? Math.round(stats.elapsed * 1000) : Date.now() - startTime;
    const readRows = stats.rows_read || null;
    const readBytes = stats.bytes_read || null;
    const queryId = result.query_id;

    let updatedStats = null;
    if (filename) {
      recordRun('alert', filename, {
        queryId,
        runTime: new Date().toISOString(),
        duration,
        rowCount,
        readRows,
        readBytes,
      });
      updatedStats = getAggregatesForFilename('alert', filename);
    }

    // Fire sinks when the alert is firing (i.e. the query returned rows).
    // Fire-and-forget — don't delay the HTTP response on webhook latency.
    if (data.length > 0 && filename) {
      const description = readDescription(getAlertsPath(), filename);
      const env = currentEnv();
      console.log(`[alert] firing: ${filename} (${rowCount} rows) embeddedEnv=${env?._embeddedId || 'n/a'}`);
      if (process.env.SLACK_WEBHOOK_URL) {
        void sendSlackAlert({ filename, description, data, rowCount });
      }
      if (alertWebhookConfig(env)) {
        void sendAlertWebhook({ filename, description, data, rowCount, env });
      }
    }

    res.json({ data, rowCount, duration, queryId, stats: updatedStats });
  } catch (error) {
    console.error('Error running alert:', error);
    res.status(500).json({ error: error.message });
  }
});

app.delete('/api/alerts/stats', (req, res) => {
  clearRunStats('alert');
  res.json({ success: true });
});

app.delete('/api/alerts/stats/:filename', (req, res) => {
  clearRunStats('alert', req.params.filename);
  res.json({ success: true });
});

app.get('/api/alerts/run-log/:filename', async (req, res) => {
  try {
    const { filename } = req.params;
    const entries = getRunLog('alert', filename, 50);
    if (entries.length === 0) return res.json({ runLog: [] });

    const queryIds = entries.map(r => r.queryId).filter(Boolean);
    let queryLogData = {};
    if (queryIds.length > 0) {
      try {
        const queryLogTable = getSystemTable('query_log');
        const queryLogQuery = `
          SELECT query_id, type, query_duration_ms, read_rows, read_bytes,
                 result_rows, result_bytes, memory_usage, ProfileEvents
          FROM ${queryLogTable}
          WHERE query_id IN (${queryIds.map(id => `'${id}'`).join(', ')})
            AND type = 'QueryFinish'
        `;
        const result = await client.query({ query: queryLogQuery, format: 'JSONEachRow' });
        const data = await result.json();
        data.forEach(row => { queryLogData[row.query_id] = row; });
      } catch (err) {
        console.error('Error fetching query_log data for alert run log:', err);
      }
    }

    const runLog = entries.map(entry => ({
      queryId: entry.queryId,
      runTime: entry.runTime,
      duration: entry.duration,
      rowCount: entry.rowCount,
      readRows: entry.readRows,
      readBytes: entry.readBytes,
      queryLog: queryLogData[entry.queryId] || null,
    }));

    res.json({ runLog });
  } catch (error) {
    console.error('Error fetching alert run log:', error);
    res.status(500).json({ error: error.message });
  }
});

app.put('/api/alerts/update', (req, res) => {
  try {
    const { filename, query, description } = req.body;
    if (!filename || !query) {
      return res.status(400).json({ error: 'Filename and query are required' });
    }
    if (!filename.endsWith('.sql') || filename.includes('/') || filename.includes('\\')) {
      return res.status(400).json({ error: 'Invalid filename' });
    }
    const alertsPath = getAlertsPath();
    const filePath = path.join(alertsPath, filename);
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: 'Alert file not found' });
    }
    fs.writeFileSync(filePath, query.trim() + '\n', 'utf-8');
    if (description !== undefined) {
      writeDescription(alertsPath, filename, description);
    }
    res.json({ success: true, filename });
  } catch (error) {
    console.error('Error updating alert:', error);
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/alerts/clone', (req, res) => {
  try {
    const { sourceFilename, newFilename, query } = req.body;
    if (!sourceFilename || !newFilename || !query) {
      return res.status(400).json({ error: 'Source filename, new filename, and query are required' });
    }
    if (!newFilename.endsWith('.sql') || newFilename.includes('/') || newFilename.includes('\\')) {
      return res.status(400).json({ error: 'Invalid filename' });
    }
    const alertsPath = getAlertsPath();
    const newFilePath = path.join(alertsPath, newFilename);

    if (fs.existsSync(newFilePath)) {
      let counter = 1;
      let uniqueFilename = newFilename;
      let uniqueFilePath = newFilePath;
      while (fs.existsSync(uniqueFilePath)) {
        const baseName = newFilename.replace(/\.sql$/, '');
        uniqueFilename = `${baseName}_${counter}.sql`;
        uniqueFilePath = path.join(alertsPath, uniqueFilename);
        counter++;
      }
      fs.writeFileSync(uniqueFilePath, query.trim() + '\n', 'utf-8');
      return res.json({ success: true, filename: uniqueFilename });
    }

    fs.writeFileSync(newFilePath, query.trim() + '\n', 'utf-8');
    res.json({ success: true, filename: newFilename });
  } catch (error) {
    console.error('Error cloning alert:', error);
    res.status(500).json({ error: error.message });
  }
});

// ==================== CLUSTER ENDPOINTS ====================

// Get system.replication_queue (detailed view)
app.get('/api/cluster/replication-queue', asyncHandler(async (req, res) => {
  const query = `
    SELECT
      database, table, replica_name, is_leader, is_readonly,
      future_parts, parts_to_check, queue_size, last_queue_update_exception
    FROM system.replicas
    ORDER BY database, table, replica_name
  `;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get system.replication_queue columns (using replicas table for this view)
app.get('/api/cluster/replication-queue/columns', asyncHandler(async (req, res) => {
  const columns = [
    { name: 'database', type: 'String', comment: 'Database name' },
    { name: 'table', type: 'String', comment: 'Table name' },
    { name: 'replica_name', type: 'String', comment: 'Replica name in ZooKeeper' },
    { name: 'is_leader', type: 'UInt8', comment: 'Whether this replica is the leader' },
    { name: 'is_readonly', type: 'UInt8', comment: 'Whether this replica is in read-only mode' },
    { name: 'future_parts', type: 'UInt32', comment: 'Number of data parts that will appear after INSERTs' },
    { name: 'parts_to_check', type: 'UInt32', comment: 'Number of data parts in the queue for verification' },
    { name: 'queue_size', type: 'UInt32', comment: 'Size of the queue for operations waiting to be performed' },
    { name: 'last_queue_update_exception', type: 'String', comment: 'Last exception during queue update' },
  ];
  res.json(columns);
}));

// Get grouped replication queue errors
app.get('/api/cluster/replication-queue/grouped', asyncHandler(async (req, res) => {
  const query = `
    SELECT table, last_exception, postpone_reason, count() as count
    FROM system.replication_queue
    GROUP BY table, last_exception, postpone_reason
    ORDER BY count DESC
  `;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get system.replicas
app.get('/api/cluster/replicas', asyncHandler(async (req, res) => {
  const query = `SELECT * FROM system.replicas ORDER BY database, table, replica_name`;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get system.replicas columns
app.get('/api/cluster/replicas/columns', columnsHandler('replicas'));

// Get system.clusters
app.get('/api/cluster/clusters', asyncHandler(async (req, res) => {
  const query = `SELECT * FROM system.clusters ORDER BY cluster, shard_num, replica_num`;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get system.clusters columns
app.get('/api/cluster/clusters/columns', columnsHandler('clusters'));

// Get clustered tables overview - uses cluster() function for cluster-wide view
app.get('/api/cluster/clustered-tables', async (req, res) => {
  try {
    const clusterName = currentCluster();
    if (!clusterName) {
      return res.status(400).json({
        error: 'No cluster configured. Add "cluster" to your environment config.',
        hint: 'Add cluster: "your_cluster_name" to querydog.yml'
      });
    }

    const query = `
      SELECT
        hostName() as host,
        database,
        table,
        engine,
        count() as part_count,
        sum(rows) as total_rows,
        sum(bytes_on_disk) as total_bytes,
        sum(data_compressed_bytes) as compressed_bytes,
        sum(data_uncompressed_bytes) as uncompressed_bytes,
        round((sum(data_uncompressed_bytes) - sum(data_compressed_bytes)) / nullIf(sum(data_uncompressed_bytes), 0) * 100, 1) as compression_ratio,
        max(modification_time) as last_modified
      FROM cluster('${clusterName}', system.parts)
      WHERE active = 1
        AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
      GROUP BY host, database, table, engine
      ORDER BY total_bytes DESC
    `;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching clustered tables:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get clustered tables summary - aggregated across all nodes
app.get('/api/cluster/clustered-tables-summary', async (req, res) => {
  try {
    const clusterName = currentCluster();
    if (!clusterName) {
      return res.status(400).json({
        error: 'No cluster configured. Add "cluster" to your environment config.',
        hint: 'Add cluster: "your_cluster_name" to querydog.yml'
      });
    }

    const query = `
      SELECT
        database,
        table,
        engine,
        count(DISTINCT hostName()) as node_count,
        sum(rows) as total_rows,
        sum(bytes_on_disk) as total_bytes,
        sum(data_compressed_bytes) as compressed_bytes,
        sum(data_uncompressed_bytes) as uncompressed_bytes,
        round((sum(data_uncompressed_bytes) - sum(data_compressed_bytes)) / nullIf(sum(data_uncompressed_bytes), 0) * 100, 1) as compression_ratio,
        max(modification_time) as last_modified
      FROM cluster('${clusterName}', system.parts)
      WHERE active = 1
        AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
      GROUP BY database, table, engine
      ORDER BY total_bytes DESC
    `;
    const result = await client.query({ query, format: 'JSONEachRow' });
    const data = await result.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching clustered tables summary:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get system.replicated_fetches
app.get('/api/cluster/fetches', asyncHandler(async (req, res) => {
  const query = `SELECT * FROM system.replicated_fetches ORDER BY database, table`;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get system.replicated_fetches columns
app.get('/api/cluster/fetches/columns', columnsHandler('replicated_fetches'));

// Get system.distributed_ddl_queue
app.get('/api/cluster/distributed-ddl', asyncHandler(async (req, res) => {
  const query = `SELECT * FROM system.distributed_ddl_queue ORDER BY entry_version DESC LIMIT 1000`;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get system.distributed_ddl_queue columns
app.get('/api/cluster/distributed-ddl/columns', columnsHandler('distributed_ddl_queue'));

// Get system.zookeeper (root path)
app.get('/api/cluster/zookeeper', asyncHandler(async (req, res) => {
  const query = `SELECT * FROM system.zookeeper WHERE path = '/' LIMIT 100`;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get system.zookeeper columns
app.get('/api/cluster/zookeeper/columns', columnsHandler('zookeeper'));

// Get system.zookeeper_connection
app.get('/api/cluster/zookeeper-connection', asyncHandler(async (req, res) => {
  const query = `SELECT * FROM system.zookeeper_connection LIMIT 1000`;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get system.zookeeper_connection columns
app.get('/api/cluster/zookeeper-connection/columns', columnsHandler('zookeeper_connection'));

// Get system.zookeeper_connection_log
app.get('/api/cluster/zookeeper-connection-log', asyncHandler(async (req, res) => {
  const query = `SELECT * FROM system.zookeeper_connection_log ORDER BY event_time DESC LIMIT 1000`;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get system.zookeeper_connection_log columns
app.get('/api/cluster/zookeeper-connection-log/columns', columnsHandler('zookeeper_connection_log'));

// Get system.zookeeper_log
app.get('/api/cluster/zookeeper-log', asyncHandler(async (req, res) => {
  const query = `SELECT * FROM system.zookeeper_log ORDER BY event_time DESC LIMIT 1000`;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get system.zookeeper_log columns
app.get('/api/cluster/zookeeper-log/columns', columnsHandler('zookeeper_log'));

// Get system.distribution_queue
app.get('/api/cluster/distribution-queue', asyncHandler(async (req, res) => {
  const query = `SELECT * FROM system.distribution_queue ORDER BY data_path LIMIT 1000`;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get system.distribution_queue columns
app.get('/api/cluster/distribution-queue/columns', columnsHandler('distribution_queue'));

// ==================== METRIC LOG API (Dashboard) ====================

// Default top 20 metrics for the dashboard
const DEFAULT_DASHBOARD_METRICS = [
  'CurrentMetric_Query',
  'CurrentMetric_MemoryTracking',
  'CurrentMetric_TCPConnection',
  'CurrentMetric_HTTPConnection',
  'CurrentMetric_Merge',
  'CurrentMetric_BackgroundMergesAndMutationsPoolTask',
  'CurrentMetric_Read',
  'CurrentMetric_Write',
  'CurrentMetric_MarkCacheBytes',
  'CurrentMetric_UncompressedCacheBytes',
  'ProfileEvent_Query',
  'ProfileEvent_InsertedRows',
  'ProfileEvent_SelectedRows',
  'ProfileEvent_ReadCompressedBytes',
  'ProfileEvent_InsertedBytes',
  'ProfileEvent_MergedRows',
  'ProfileEvent_MarkCacheHits',
  'ProfileEvent_MarkCacheMisses',
  'ProfileEvent_QueryMemoryLimitExceeded',
  'ProfileEvent_FailedQuery',
];

// Get available metric columns from metric_log
app.get('/api/metric-log/columns', asyncHandler(async (req, res) => {
  const query = `
    SELECT name, type
    FROM system.columns
    WHERE database = 'system'
      AND table = 'metric_log'
      AND (name LIKE 'CurrentMetric_%' OR name LIKE 'ProfileEvent_%')
    ORDER BY name
  `;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get time series data for selected metrics
app.get('/api/metric-log/timeseries', asyncHandler(async (req, res) => {
  const { start, end, bucket = 'minute', metrics } = req.query;

  // Parse metrics or use defaults
  const metricList = metrics
    ? (typeof metrics === 'string' ? metrics.split(',') : metrics)
    : DEFAULT_DASHBOARD_METRICS;

  // Validate metrics to prevent SQL injection
  const safeMetrics = metricList.filter(m => /^(CurrentMetric_|ProfileEvent_)[A-Za-z0-9_]+$/.test(m));

  if (safeMetrics.length === 0) {
    return res.json([]);
  }

  // Filter against actual columns in metric_log to avoid UNKNOWN_IDENTIFIER errors
  let validMetrics;
  try {
    const colResult = await client.query({
      query: `SELECT name FROM system.columns WHERE database = 'system' AND table = 'metric_log' AND name IN (${safeMetrics.map(m => `'${m}'`).join(',')})`,
      format: 'JSONEachRow',
    });
    const existingCols = new Set((await colResult.json()).map(r => r.name));
    validMetrics = safeMetrics.filter(m => existingCols.has(m));
  } catch (colErr) {
    console.error('Error checking metric_log columns, falling back to all metrics:', colErr.message);
    validMetrics = safeMetrics;
  }

  if (validMetrics.length === 0) {
    return res.json([]);
  }

  // Determine truncation function based on bucket
  // Use toDateTime to avoid toStartOfSecond incompatibility with DateTime columns
  let truncFunc;
  switch (bucket) {
    case 'second':
      truncFunc = 'toDateTime(event_time)';
      break;
    case 'hour':
      truncFunc = 'toStartOfHour(event_time)';
      break;
    default:
      truncFunc = 'toStartOfMinute(event_time)';
  }

  // Build dynamic SELECT: sum for ProfileEvent, max for CurrentMetric
  const selectClauses = validMetrics.map(m => {
    if (m.startsWith('ProfileEvent_')) {
      return `sum(${m}) as ${m}`;
    } else {
      return `max(${m}) as ${m}`;
    }
  }).join(',\n        ');

  const query = `
    SELECT
      ${truncFunc} as time,
      ${selectClauses}
    FROM system.metric_log
    WHERE event_time >= '${start}' AND event_time <= '${end}'
    GROUP BY time
    ORDER BY time
  `;

  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get default dashboard metrics list
app.get('/api/metric-log/defaults', async (req, res) => {
  res.json(DEFAULT_DASHBOARD_METRICS);
});

// ==================== ASYNC METRIC LOG API (Dashboard) ====================

// Default async metrics from asynchronous_metric_log for the dashboard
const DEFAULT_ASYNC_METRIC_LOG_METRICS = [
  'OSMemoryTotal',
  'OSMemoryAvailable',
  'OSMemoryCached',
  'OSMemoryBuffers',
  'jemalloc.resident',
  'jemalloc.allocated',
  'ReplicasMaxQueueSize',
  'ReplicasSumQueueSize',
  'UncompressedCacheBytes',
  'MarkCacheBytes',
];

// Get available async metric names from asynchronous_metric_log
app.get('/api/async-metric-log/columns', asyncHandler(async (req, res) => {
  const query = `
    SELECT DISTINCT metric as name
    FROM system.asynchronous_metric_log
    ORDER BY metric
    LIMIT 500
  `;
  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get time series data for selected async metrics
app.get('/api/async-metric-log/timeseries', asyncHandler(async (req, res) => {
  const { start, end, bucket = 'minute', metrics } = req.query;

  // Parse metrics or use defaults
  const metricList = metrics
    ? (typeof metrics === 'string' ? metrics.split(',') : metrics)
    : DEFAULT_ASYNC_METRIC_LOG_METRICS;

  // Validate metrics - async metrics can have various characters including dots
  const validMetrics = metricList.filter(m => /^[A-Za-z0-9_.]+$/.test(m));

  if (validMetrics.length === 0) {
    return res.json([]);
  }

  // Determine truncation function based on bucket
  // Use toDateTime to avoid toStartOfSecond incompatibility with DateTime columns
  let truncFunc;
  switch (bucket) {
    case 'second':
      truncFunc = 'toDateTime(event_time)';
      break;
    case 'hour':
      truncFunc = 'toStartOfHour(event_time)';
      break;
    default:
      truncFunc = 'toStartOfMinute(event_time)';
  }

  // Build query to pivot async metrics into columns
  const selectClauses = validMetrics.map(m =>
    `maxIf(value, metric = '${m}') as \`${m}\``
  ).join(',\n        ');

  const whereMetrics = validMetrics.map(m => `'${m}'`).join(', ');

  const query = `
    SELECT
      ${truncFunc} as time,
      ${selectClauses}
    FROM system.asynchronous_metric_log
    WHERE event_time >= '${start}' AND event_time <= '${end}'
      AND metric IN (${whereMetrics})
    GROUP BY time
    ORDER BY time
  `;

  const result = await client.query({ query, format: 'JSONEachRow' });
  const data = await result.json();
  res.json(data);
}));

// Get default async dashboard metrics list
app.get('/api/async-metric-log/defaults', async (req, res) => {
  res.json(DEFAULT_ASYNC_METRIC_LOG_METRICS);
});

// Serve the React app for any other routes (Express v5 compatible)
app.use((req, res, next) => {
  if (req.method === 'GET' && !req.path.startsWith('/api')) {
    res.sendFile(path.join(__dirname, '../dist/index.html'));
  } else {
    next();
  }
});

function isRunningInDocker() {
  if (fs.existsSync('/.dockerenv')) return true;
  try {
    const cgroup = fs.readFileSync('/proc/1/cgroup', 'utf-8');
    return /docker|containerd|kubepods/i.test(cgroup);
  } catch {
    return false;
  }
}

const DEFAULT_PORT = isRunningInDocker() ? 3001 : 3002;
const PORT = process.env.PORT || DEFAULT_PORT;
let server;

// Startup function - validates connection before starting server

// Express error middleware — last-resort handler for routes wrapped in
// asyncHandler that throw. Logs context, returns JSON 500.
app.use((err, req, res, _next) => {
  console.error(`Error in ${req.method} ${req.originalUrl}:`, err);
  if (res.headersSent) return;
  res.status(500).json({ error: err.message });
});

async function startup() {
  // Display banner
  console.log('\n' + figlet.textSync('QueryDog', { font: 'Standard' }) + '\n');

  // Start HTTP server FIRST (non-blocking)
  server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`Server listening on port ${PORT}\n`);
  });

  if (embeddedModeEnabled()) {
    console.log(`Embedded mode: environments will be loaded per-session from ${process.env.QUERYDOG_API_BOOTSTRAP_URL}\n`);
    return;
  }

  const env = environments[activeEnvIndex];
  if (!env) {
    console.log('No environments configured. Add a querydog.yml or set QUERYDOG_API_BOOTSTRAP_URL.\n');
    return;
  }
  const protocol = env.secure ? 'https' : 'http';

  if (environments.length > 1) {
    console.log(`Available environments (${environments.length}):`);
    environments.forEach((e, i) => {
      const marker = i === activeEnvIndex ? ' (active)' : '';
      console.log(`  ${i + 1}. ${e.name} - ${e.host}:${e.port}${marker}`);
    });
    console.log('');
  }

  console.log(`Testing connection to "${env.name}" (${protocol}://${env.host}:${env.port})...`);
  pingWithTimeout().then(() => {
    console.log('ClickHouse connection successful!');
  }).catch((error) => {
    console.error('\n╔══════════════════════════════════════════════════════════════╗');
    console.error('║  WARNING: Failed to connect to ClickHouse                    ║');
    console.error('╚══════════════════════════════════════════════════════════════╝\n');
    console.error(`Environment: ${env.name}`);
    console.error(`Host: ${env.host}:${env.port}`);
    console.error(`User: ${env.user}`);
    console.error(`Database: ${env.database}`);
    console.error(`Secure: ${env.secure ? 'Yes' : 'No'}\n`);
    console.error('Error details:', error.message);
    console.error('\nPlease check your querydog.yml configuration and ensure ClickHouse is reachable.');
    console.error('You can switch environments from the UI.\n');
  });
}

// Graceful shutdown handler
function shutdown(signal) {
  console.log(`\n${signal} received, shutting down gracefully...`);
  if (server) {
    server.close(async () => {
      console.log('HTTP server closed');
      try {
        if (globalClient) await globalClient.close();
        for (const sess of sessions.values()) {
          await sess.client?.close?.().catch(() => {});
        }
      } catch {}
      console.log('ClickHouse connections closed');
      process.exit(0);
    });
  } else {
    process.exit(0);
  }

  // Force exit after 10 seconds if graceful shutdown fails
  setTimeout(() => {
    console.error('Forcing shutdown after timeout');
    process.exit(1);
  }, 10000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Start the application
startup();
