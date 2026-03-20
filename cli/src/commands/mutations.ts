import { executeQuery, SystemQueries } from '../utils/clickhouse';
import { loadCLIConfig } from '../utils/config';
import { formatOutput, OutputFormat, printHeader, printOutput } from '../utils/formatters';

interface MutationInfo {
  table: string;
  mutation_id: string;
  command: string;
  create_time: string;
  is_done: boolean;
  parts_to_do: number;
  latest_fail_reason: string;
}

export async function listMutations(
  database: string,
  format: OutputFormat,
  envName: string
): Promise<void> {
  const config = loadCLIConfig();

  if (format === 'table') {
    printHeader('Mutations', envName);
  }

  const query = SystemQueries.mutations(database);
  const data = await executeQuery<MutationInfo>(query);

  const columns = config.mutations.columns;
  const output = formatOutput(data, format, columns, config);

  if (format === 'table') {
    console.log(output);
    const pending = data.filter(m => !m.is_done).length;
    const completed = data.filter(m => m.is_done).length;
    console.log(`\nTotal: ${data.length} mutations (${pending} pending, ${completed} completed)\n\n`);
  } else {
    printOutput(output);
  }
}
