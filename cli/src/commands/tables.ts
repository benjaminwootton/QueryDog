import { executeQuery, SystemQueries } from '../utils/clickhouse';
import { loadCLIConfig } from '../utils/config';
import { formatOutput, OutputFormat, printHeader, printOutput } from '../utils/formatters';

interface TableInfo {
  name: string;
  engine: string;
  total_rows: number;
  total_bytes: string;
  partition_key: string;
  sorting_key: string;
  primary_key: string;
  create_table_query: string;
}

export async function listTables(
  database: string,
  format: OutputFormat,
  envName: string
): Promise<void> {
  const config = loadCLIConfig();

  if (format === 'table') {
    printHeader('Tables', envName);
  }

  const query = SystemQueries.tables(database);
  const data = await executeQuery<TableInfo>(query);

  const columns = config.tables.columns;
  const output = formatOutput(data, format, columns, config);

  if (format === 'table' && data.length > 0) {
    console.log(output);
    console.log(`\nTotal: ${data.length} tables\n\n`);
  } else {
    printOutput(output);
  }
}
