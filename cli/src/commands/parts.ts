import { executeQuery, SystemQueries } from '../utils/clickhouse';
import { formatOutput, OutputFormat, printHeader } from '../utils/formatters';

export async function listParts(
  database: string | undefined,
  format: OutputFormat,
  envName: string,
  table?: string
): Promise<void> {
  if (format === 'table') {
    printHeader(table ? `Parts for ${table}` : 'Parts', envName);
  }

  const query = SystemQueries.parts(database, table);
  const data = await executeQuery(query);
  const output = formatOutput(data, format);
  console.log(output);

  if (format === 'table' && data.length > 0) {
    console.log(`\nTotal: ${data.length} parts`);
  }
}

export async function listPartitionsSummary(
  database: string | undefined,
  format: OutputFormat,
  envName: string
): Promise<void> {
  if (format === 'table') {
    printHeader('Partitions Summary', envName);
  }

  const query = SystemQueries.partitionsSummary(database);
  const data = await executeQuery(query);
  const output = formatOutput(data, format);
  console.log(output);

  if (format === 'table' && data.length > 0) {
    console.log(`\nTotal: ${data.length} partitions`);
  }
}
