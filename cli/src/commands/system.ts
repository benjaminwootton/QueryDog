import { executeQuery, SystemQueries } from '../utils/clickhouse';
import { formatOutput, OutputFormat, printHeader } from '../utils/formatters';

// Disks
export async function listDisks(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Disks', envName);
  }

  const data = await executeQuery(SystemQueries.disks());
  const output = formatOutput(data, format);
  console.log(output);
}

// Storage Policies
export async function listStoragePolicies(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Storage Policies', envName);
  }

  const data = await executeQuery(SystemQueries.storagePolicies());
  const output = formatOutput(data, format);
  console.log(output);
}

// Users
export async function listUsers(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Users', envName);
  }

  const data = await executeQuery(SystemQueries.users());
  const output = formatOutput(data, format);
  console.log(output);
}

// Roles
export async function listRoles(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Roles', envName);
  }

  const data = await executeQuery(SystemQueries.roles());
  const output = formatOutput(data, format);
  console.log(output);
}

// Grants
export async function listGrants(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Grants', envName);
  }

  const data = await executeQuery(SystemQueries.grants());
  const output = formatOutput(data, format);
  console.log(output);
}

// Quotas
export async function listQuotas(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Quotas', envName);
  }

  const data = await executeQuery(SystemQueries.quotas());
  const output = formatOutput(data, format);
  console.log(output);
}

// Metrics
export async function listMetrics(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Metrics', envName);
  }

  const data = await executeQuery(SystemQueries.metrics());
  const output = formatOutput(data, format);
  console.log(output);
}

// Async Metrics
export async function listAsyncMetrics(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Asynchronous Metrics', envName);
  }

  const data = await executeQuery(SystemQueries.asyncMetrics());
  const output = formatOutput(data, format);
  console.log(output);
}

// Events
export async function listEvents(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Events', envName);
  }

  const data = await executeQuery(SystemQueries.events());
  const output = formatOutput(data, format);
  console.log(output);
}

// Errors
export async function listErrors(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Errors', envName);
  }

  const data = await executeQuery(SystemQueries.errors());
  const output = formatOutput(data, format);
  console.log(output);
}

// Warnings
export async function listWarnings(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Warnings', envName);
  }

  const data = await executeQuery(SystemQueries.warnings());
  const output = formatOutput(data, format);
  console.log(output);
}

// Settings
export async function listSettings(
  format: OutputFormat,
  envName: string,
  search?: string
): Promise<void> {
  if (format === 'table') {
    printHeader(search ? `Settings matching "${search}"` : 'Settings', envName);
  }

  const data = await executeQuery(SystemQueries.settings(search));
  const output = formatOutput(data, format);
  console.log(output);
}

// Dictionaries
export async function listDictionaries(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Dictionaries', envName);
  }

  const data = await executeQuery(SystemQueries.dictionaries());
  const output = formatOutput(data, format);
  console.log(output);
}

// Databases Summary
export async function listDatabasesSummary(
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Databases Summary', envName);
  }

  const data = await executeQuery(SystemQueries.databasesSummary());
  const output = formatOutput(data, format);
  console.log(output);
}

// Text Log
export async function listTextLog(
  format: OutputFormat,
  envName: string,
  hours: number = 1,
  level?: string,
  limit: number = 100
): Promise<void> {
  if (format === 'table') {
    printHeader(`Text Log (last ${hours}h)`, envName);
  }

  const data = await executeQuery(SystemQueries.textLog(hours, level, limit));
  const output = formatOutput(data, format);
  console.log(output);
}

// Data Skipping Indexes
export async function listIndexes(
  format: OutputFormat,
  envName: string,
  database?: string,
  table?: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Data Skipping Indexes', envName);
  }

  const data = await executeQuery(SystemQueries.dataSkippingIndexes(database, table));
  const output = formatOutput(data, format);
  console.log(output);
}

// Projections
export async function listProjections(
  format: OutputFormat,
  envName: string,
  database?: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Projections', envName);
  }

  const data = await executeQuery(SystemQueries.projections(database));
  const output = formatOutput(data, format);
  console.log(output);
}

// Schema Analysis - Nullable columns (candidates for optimization)
export async function listNullableColumns(
  format: OutputFormat,
  envName: string,
  database?: string,
  table?: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Nullable Columns (check for unnecessary nullability)', envName);
  }

  const data = await executeQuery(SystemQueries.nullableNoNulls(database, table));
  const output = formatOutput(data, format);
  console.log(output);

  if (format === 'table' && data.length > 0) {
    console.log(`\nFound ${data.length} Nullable columns. Use queries to check if NULLs actually exist.`);
  }
}

// Schema Analysis - Oversized columns
export async function listOversizedColumns(
  format: OutputFormat,
  envName: string,
  database?: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Large Integer Types (potential optimization)', envName);
  }

  const data = await executeQuery(SystemQueries.oversizedColumns(database));
  const output = formatOutput(data, format);
  console.log(output);

  if (format === 'table' && data.length > 0) {
    console.log(`\nFound ${data.length} columns with Int64/UInt64 or larger. Check if smaller types would suffice.`);
  }
}

// Column statistics for a table
export async function listColumnStats(
  format: OutputFormat,
  envName: string,
  database: string,
  table: string
): Promise<void> {
  if (format === 'table') {
    printHeader(`Column Stats: ${database}.${table}`, envName);
  }

  const data = await executeQuery(SystemQueries.columnStats(database, table));
  const output = formatOutput(data, format);
  console.log(output);
}
