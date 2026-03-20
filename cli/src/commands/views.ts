import { executeQuery, SystemQueries } from '../utils/clickhouse';
import { loadCLIConfig } from '../utils/config';
import { formatOutput, OutputFormat, printHeader, printOutput } from '../utils/formatters';

interface ViewInfo {
  name: string;
  is_materialized: boolean;
  create_table_query: string;
}

interface MaterializedViewInfo {
  name: string;
  engine: string;
  as_select: string;
  total_rows: number;
  total_bytes: string;
}

export async function listViews(
  database: string,
  format: OutputFormat,
  envName: string
): Promise<void> {
  const config = loadCLIConfig();

  if (format === 'table') {
    printHeader('Views', envName);
  }

  const query = SystemQueries.views(database);
  const data = await executeQuery<ViewInfo>(query);

  const columns = config.views.columns;
  const output = formatOutput(data, format, columns, config);

  if (format === 'table' && data.length > 0) {
    console.log(output);
    console.log(`\nTotal: ${data.length} views\n\n`);
  } else {
    printOutput(output);
  }
}

export async function listMaterializedViews(
  database: string,
  format: OutputFormat,
  envName: string
): Promise<void> {
  const config = loadCLIConfig();

  if (format === 'table') {
    printHeader('Materialized Views', envName);
  }

  const query = SystemQueries.materializedViews(database);
  const data = await executeQuery<MaterializedViewInfo>(query);

  const columns = config.materialized_views.columns;
  const output = formatOutput(data, format, columns, config);

  if (format === 'table' && data.length > 0) {
    console.log(output);
    console.log(`\nTotal: ${data.length} materialized views\n\n`);
  } else {
    printOutput(output);
  }
}
