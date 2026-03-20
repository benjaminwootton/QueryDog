import { executeQuery, SystemQueries } from '../utils/clickhouse';
import { loadCLIConfig } from '../utils/config';
import { formatOutput, OutputFormat, printHeader } from '../utils/formatters';

interface PartitionInfo {
  table: string;
  partition: string;
  name: string;
  rows: number;
  bytes_on_disk: string;
  modification_time: string;
  active: boolean;
}

export async function listPartitions(
  database: string,
  format: OutputFormat,
  envName: string,
  table?: string
): Promise<void> {
  const config = loadCLIConfig();

  if (format === 'table') {
    printHeader(table ? `Partitions for ${table}` : 'Partitions', envName);
  }

  const query = SystemQueries.partitions(database, table);
  const data = await executeQuery<PartitionInfo>(query);

  const columns = config.partitions.columns;
  const output = formatOutput(data, format, columns, config);
  console.log(output);

  if (format === 'table' && data.length > 0) {
    console.log(`\nTotal: ${data.length} partitions`);
  }
}
