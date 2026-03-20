import { executeQuery, SystemQueries } from '../utils/clickhouse';
import { formatOutput, OutputFormat, printHeader, printOutput } from '../utils/formatters';

// Clusters
export async function listClusters(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Clusters', envName);
  }

  const data = await executeQuery(SystemQueries.clusters());
  const output = formatOutput(data, format);
  printOutput(output);
}

// Replicas
export async function listReplicas(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Replicas', envName);
  }

  const data = await executeQuery(SystemQueries.replicas());
  const output = formatOutput(data, format);
  printOutput(output);
}

// Replication Queue
export async function listReplicationQueue(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Replication Queue', envName);
  }

  const data = await executeQuery(SystemQueries.replicationQueue());
  const output = formatOutput(data, format);
  printOutput(output);
}

// Zookeeper
export async function listZookeeper(
  format: OutputFormat,
  envName: string,
  path: string = '/'
): Promise<void> {
  if (format === 'table') {
    printHeader(`Zookeeper (${path})`, envName);
  }

  const data = await executeQuery(SystemQueries.zookeeper(path));
  const output = formatOutput(data, format);
  printOutput(output);
}

// Merges
export async function listMerges(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Active Merges', envName);
  }

  const data = await executeQuery(SystemQueries.merges());
  const output = formatOutput(data, format);

  if (format === 'table') {
    console.log(output);
    console.log(`\nTotal: ${data.length} active merges\n\n`);
  } else {
    printOutput(output);
  }
}

// Background Jobs
export async function listBackgroundJobs(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Background Jobs', envName);
  }

  try {
    const data = await executeQuery(SystemQueries.backgroundJobs());
    const output = formatOutput(data, format);
    printOutput(output);
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    if (message.includes('UNKNOWN_TABLE')) {
      console.log('system.backups table not available on this server\n\n');
    } else {
      throw err;
    }
  }
}

// Async Inserts
export async function listAsyncInserts(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Async Inserts', envName);
  }

  try {
    const data = await executeQuery(SystemQueries.asyncInserts());
    const output = formatOutput(data, format);
    printOutput(output);
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    if (message.includes('UNKNOWN_TABLE')) {
      console.log('system.asynchronous_inserts table not available on this server\n\n');
    } else {
      throw err;
    }
  }
}

// Query Cache
export async function listQueryCache(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Query Cache', envName);
  }

  try {
    const data = await executeQuery(SystemQueries.queryCache());
    const output = formatOutput(data, format);
    printOutput(output);
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    if (message.includes('UNKNOWN_TABLE')) {
      console.log('system.query_cache table not available on this server\n\n');
    } else {
      throw err;
    }
  }
}

// View Refreshes
export async function listViewRefreshes(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('View Refreshes', envName);
  }

  try {
    const data = await executeQuery(SystemQueries.viewRefreshes());
    const output = formatOutput(data, format);
    printOutput(output);
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    if (message.includes('UNKNOWN_TABLE')) {
      console.log('system.view_refreshes table not available on this server\n\n');
    } else {
      throw err;
    }
  }
}
