import { executeQuery, SystemQueries } from '../utils/clickhouse';
import { loadCLIConfig } from '../utils/config';
import { formatOutput, OutputFormat, printHeader } from '../utils/formatters';

interface ProcessInfo {
  query_id: string;
  user: string;
  elapsed: number;
  read_rows: string;
  memory_usage: string;
  query: string;
}

export async function listProcesses(
  format: OutputFormat,
  envName: string
): Promise<void> {
  const config = loadCLIConfig();

  if (format === 'table') {
    printHeader('Running Processes', envName);
  }

  const query = SystemQueries.processes();
  const data = await executeQuery<ProcessInfo>(query);

  const columns = config.processes.columns;
  const output = formatOutput(data, format, columns, config);
  console.log(output);

  if (format === 'table') {
    console.log(`\nTotal: ${data.length} running processes`);
  }
}
