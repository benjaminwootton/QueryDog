import Table from 'cli-table3';
import chalk from 'chalk';
import { CLIConfig } from './config';

export type OutputFormat = 'table' | 'json' | 'csv';

export interface FormatOptions {
  wide?: boolean;
}

// Global format options set by CLI
let globalFormatOptions: FormatOptions = {};

export function setFormatOptions(options: FormatOptions): void {
  globalFormatOptions = options;
}

export function getFormatOptions(): FormatOptions {
  return globalFormatOptions;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function formatOutput(
  data: any[],
  format: OutputFormat,
  columns?: string[],
  config?: CLIConfig,
  options?: FormatOptions
): string {
  if (data.length === 0) {
    return chalk.yellow('No data found.');
  }

  // Use provided options or fall back to global
  const opts = options || globalFormatOptions;

  switch (format) {
    case 'json':
      return formatAsJson(data);
    case 'csv':
      return formatAsCsv(data, columns);
    case 'table':
    default:
      return formatAsTable(data, columns, config, opts);
  }
}

function formatAsJson<T>(data: T[]): string {
  return JSON.stringify(data, null, 2);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function formatAsCsv(
  data: any[],
  columns?: string[]
): string {
  if (data.length === 0) return '';

  const keys = columns || Object.keys(data[0]);
  const header = keys.join(',');
  const rows = data.map((row) =>
    keys
      .map((key) => {
        const value = row[key];
        const strValue = value === null || value === undefined ? '' : String(value);
        // Escape quotes and wrap in quotes if contains comma
        if (strValue.includes(',') || strValue.includes('"') || strValue.includes('\n')) {
          return `"${strValue.replace(/"/g, '""')}"`;
        }
        return strValue;
      })
      .join(',')
  );

  return [header, ...rows].join('\n');
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function formatAsTable(
  data: any[],
  columns?: string[],
  config?: CLIConfig,
  options?: FormatOptions
): string {
  if (data.length === 0) return '';

  const keys = columns || Object.keys(data[0]);
  const wide = options?.wide || false;

  // Calculate max column width based on terminal width
  const terminalWidth = process.stdout.columns || 120;
  // Account for table borders: ~3 chars per column (| + spaces)
  const availableWidth = terminalWidth - (keys.length * 3) - 1;
  const maxColWidth = wide ? Infinity : Math.max(15, Math.floor(availableWidth / keys.length));

  const table = new Table({
    head: keys.map((k) => chalk.cyan.bold(k)),
    style: {
      head: [],
      border: ['gray'],
    },
    wordWrap: false,
  });

  data.forEach((row) => {
    const values = keys.map((key) => {
      const value = row[key];
      if (value === null || value === undefined) {
        return chalk.gray('null');
      }

      let strValue = String(value);

      // Truncate long values unless wide mode
      if (!wide && strValue.length > maxColWidth) {
        strValue = strValue.substring(0, maxColWidth - 1) + '…';
      }

      // Color code certain values
      if (key === 'is_done' || key === 'active' || key === 'success') {
        return value ? chalk.green.bold('✓ true') : chalk.red.bold('✗ false');
      }
      if (key === 'type' && strValue === 'ExceptionWhileProcessing') {
        return chalk.red.bold(strValue);
      }
      if (key === 'type' && strValue === 'QueryFinish') {
        return chalk.green(strValue);
      }
      // Database and table names in magenta
      if (key === 'database' || key === 'db') {
        return chalk.magenta(strValue);
      }
      if (key === 'table' || key === 'name' || key === 'table_name') {
        return chalk.cyan(strValue);
      }
      // Size/memory values
      if (key.includes('size') || key.includes('bytes') || key.includes('memory') || key.includes('rows')) {
        return chalk.green(strValue);
      }
      // Time/duration values
      if (key.includes('time') || key.includes('duration') || key.includes('elapsed')) {
        return chalk.yellow(strValue);
      }
      // Status fields
      if (key === 'status' || key === 'state') {
        if (strValue.toLowerCase().includes('error') || strValue.toLowerCase().includes('fail')) {
          return chalk.red.bold(strValue);
        }
        if (strValue.toLowerCase().includes('success') || strValue.toLowerCase().includes('done') || strValue.toLowerCase().includes('complete')) {
          return chalk.green.bold(strValue);
        }
        if (strValue.toLowerCase().includes('running') || strValue.toLowerCase().includes('progress')) {
          return chalk.yellow(strValue);
        }
        return chalk.blue(strValue);
      }
      // User names
      if (key === 'user' || key === 'user_name' || key === 'initial_user') {
        return chalk.blue(strValue);
      }
      // Query text - keep it readable but dimmed
      if (key === 'query' || key === 'query_text') {
        return chalk.white(strValue);
      }
      // Numbers get yellow
      if (typeof value === 'number') {
        return chalk.yellow(strValue);
      }

      return chalk.white(strValue);
    });

    table.push(values);
  });

  return table.toString();
}

export function printEnvironments(environments: { name: string; host: string; port: number; database: string }[]): string {
  const table = new Table({
    head: [
      chalk.cyan.bold('#'),
      chalk.cyan.bold('Name'),
      chalk.cyan.bold('Host'),
      chalk.cyan.bold('Port'),
      chalk.cyan.bold('Database'),
    ],
    style: {
      head: [],
      border: ['gray'],
    },
  });

  environments.forEach((env, index) => {
    table.push([
      chalk.yellow.bold(String(index + 1)),
      chalk.green.bold(env.name),
      chalk.cyan(env.host),
      chalk.yellow(String(env.port)),
      chalk.magenta(env.database),
    ]);
  });

  return table.toString();
}

export function printHeader(title: string, env?: string): void {
  console.log();
  console.log(chalk.bold.cyan('╔══════════════════════════════════════════════════════════════╗'));
  console.log(chalk.bold.cyan('║') + chalk.bold.yellow(`  🐕 QueryDog CLI - ${title}`.padEnd(63)) + chalk.bold.cyan('║'));
  if (env) {
    console.log(chalk.bold.cyan('║') + chalk.magenta(`  Environment: ${env}`.padEnd(62)) + chalk.bold.cyan('║'));
  }
  console.log(chalk.bold.cyan('╚══════════════════════════════════════════════════════════════╝'));
  console.log();
}

export function printError(message: string): void {
  console.error(chalk.red.bold('Error: ') + chalk.red(message));
}

export function printSuccess(message: string): void {
  console.log(chalk.green.bold('✓ ') + chalk.green(message));
}

export function printInfo(message: string): void {
  console.log(chalk.blue.bold('ℹ ') + chalk.blue(message));
}

export function printOutput(output: string): void {
  console.log(output);
  console.log('\n');
}
