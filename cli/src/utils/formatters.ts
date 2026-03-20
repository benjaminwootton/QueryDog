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
      if (key === 'is_done' || key === 'active') {
        return value ? chalk.green('true') : chalk.red('false');
      }
      if (key === 'type' && strValue === 'ExceptionWhileProcessing') {
        return chalk.red(strValue);
      }
      if (typeof value === 'number') {
        return chalk.yellow(strValue);
      }

      return strValue;
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
      chalk.yellow(String(index + 1)),
      chalk.white(env.name),
      chalk.gray(env.host),
      chalk.gray(String(env.port)),
      chalk.blue(env.database),
    ]);
  });

  return table.toString();
}

export function printHeader(title: string, env?: string): void {
  console.log();
  console.log(chalk.bold.blue('╔══════════════════════════════════════════════════════════════╗'));
  console.log(chalk.bold.blue('║') + chalk.bold.white(`  QueryDog CLI - ${title}`.padEnd(62)) + chalk.bold.blue('║'));
  if (env) {
    console.log(chalk.bold.blue('║') + chalk.gray(`  Environment: ${env}`.padEnd(62)) + chalk.bold.blue('║'));
  }
  console.log(chalk.bold.blue('╚══════════════════════════════════════════════════════════════╝'));
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
