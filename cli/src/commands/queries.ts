import { executeQuery, SystemQueries } from '../utils/clickhouse';
import { loadCLIConfig } from '../utils/config';
import { formatOutput, OutputFormat, printHeader, printOutput } from '../utils/formatters';

export type QueryMode = 'all' | 'slowest' | 'fastest' | 'highestmemory' | 'rowsread' | 'frequent' | 'bytable' | 'errors';

export async function listQueries(
  mode: QueryMode,
  format: OutputFormat,
  envName: string,
  hours: number = 24,
  limit: number = 50
): Promise<void> {
  const config = loadCLIConfig();

  let title: string;
  let query: string;

  switch (mode) {
    case 'slowest':
      title = `Slowest Queries (last ${hours}h)`;
      query = SystemQueries.queriesSlowest(hours, limit);
      break;
    case 'fastest':
      title = `Fastest Queries (last ${hours}h)`;
      query = SystemQueries.queriesFastest(hours, limit);
      break;
    case 'highestmemory':
      title = `Highest Memory Queries (last ${hours}h)`;
      query = SystemQueries.queriesHighestMemory(hours, limit);
      break;
    case 'rowsread':
      title = `Most Rows Read (last ${hours}h)`;
      query = SystemQueries.queriesRowsRead(hours, limit);
      break;
    case 'frequent':
      title = `Most Frequent Queries (last ${hours}h)`;
      query = SystemQueries.queriesGrouped(hours, limit);
      break;
    case 'bytable':
      title = `Queries By Table (last ${hours}h)`;
      query = SystemQueries.queriesByTable(hours, limit);
      break;
    case 'errors':
      title = `Query Errors (last ${hours}h)`;
      query = SystemQueries.queriesErrors(hours, limit);
      break;
    case 'all':
    default:
      title = `Recent Queries (last ${hours}h)`;
      query = SystemQueries.queriesAll(hours, limit);
      break;
  }

  if (format === 'table') {
    printHeader(title, envName);
  }

  const data = await executeQuery(query);
  const output = formatOutput(data, format, undefined, config);

  if (format === 'table' && data.length > 0) {
    console.log(output);
    console.log(`\nTotal: ${data.length} queries\n\n`);
  } else {
    printOutput(output);
  }
}
