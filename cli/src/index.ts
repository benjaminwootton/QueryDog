#!/usr/bin/env node

import { Command } from 'commander';
import chalk from 'chalk';
import ora from 'ora';
import * as readline from 'readline';
import {
  listEnvironments,
  getEnvironment,
  getEnvironmentByIndex,
  findEnvironmentByPartialName,
  Environment,
  ConfigNotFoundError,
  configExists,
  saveQuerydogConfig,
  QuerydogConfig,
} from './utils/config';
import { createClickHouseClient, closeClient } from './utils/clickhouse';
import { printEnvironments, printError, printHeader, OutputFormat, setFormatOptions } from './utils/formatters';

// Commands
import { listTables } from './commands/tables';
import { listViews, listMaterializedViews } from './commands/views';
import { listPartitions } from './commands/partitions';
import { listMutations } from './commands/mutations';
import { listProcesses } from './commands/processes';
import { listQueries, QueryMode } from './commands/queries';
import {
  listDisks,
  listStoragePolicies,
  listUsers,
  listRoles,
  listGrants,
  listQuotas,
  listMetrics,
  listAsyncMetrics,
  listEvents,
  listErrors,
  listWarnings,
  listSettings,
  listDictionaries,
  listDatabasesSummary,
  listTextLog,
  listIndexes,
  listProjections,
  listNullableColumns,
  listOversizedColumns,
  listColumnStats,
} from './commands/system';
import {
  listClusters,
  listReplicas,
  listReplicationQueue,
  listZookeeper,
  listMerges,
  listBackgroundJobs,
  listAsyncInserts,
  listQueryCache,
  listViewRefreshes,
} from './commands/cluster';

const program = new Command();

program
  .name('querydog')
  .description('CLI tool for querying and managing ClickHouse databases')
  .version('1.0.0');

// Global options
program
  .option('-e, --env <name>', 'Environment name from querydog.yaml')
  .option('-f, --format <format>', 'Output format: table, json, csv', 'table')
  .option('--hours <hours>', 'Time range in hours for query log commands', '24')
  .option('--limit <limit>', 'Limit number of results', '50')
  .option('-d, --database <database>', 'Filter by database')
  .option('-t, --table <table>', 'Filter by table')
  .option('-w, --wide', 'Wide output - do not truncate columns to terminal width')
  .option('-i, --interactive', 'Interactive mode - stay in REPL after each command');

// Helper to prompt for input
function prompt(rl: readline.Interface, question: string, defaultValue?: string): Promise<string> {
  const displayQuestion = defaultValue
    ? `${question} ${chalk.gray(`[${defaultValue}]`)}: `
    : `${question}: `;

  return new Promise((resolve) => {
    rl.question(displayQuestion, (answer) => {
      resolve(answer.trim() || defaultValue || '');
    });
  });
}

// Helper to display config not found error
function handleConfigNotFound(error: ConfigNotFoundError): void {
  console.log(chalk.red('\n  Error: querydog.yaml configuration file not found\n'));
  console.log(chalk.gray('  Searched locations:'));
  error.searchedPaths.forEach(p => console.log(chalk.gray(`    - ${p}`)));
  console.log('');
  console.log(chalk.cyan('  To create a new configuration file, run:\n'));
  console.log(chalk.white('    querydog init\n'));
  console.log(chalk.gray('  This will interactively create a querydog.yaml file with your'));
  console.log(chalk.gray('  ClickHouse connection settings.\n'));
  console.log(chalk.gray('  Example querydog.yaml structure:'));
  console.log(chalk.gray('  ─────────────────────────────────────'));
  console.log(chalk.yellow('  environments:'));
  console.log(chalk.yellow('    - name: "Production"'));
  console.log(chalk.yellow('      host: "clickhouse.example.com"'));
  console.log(chalk.yellow('      port: 8443'));
  console.log(chalk.yellow('      user: "default"'));
  console.log(chalk.yellow('      password: ""'));
  console.log(chalk.yellow('      database: "default"'));
  console.log(chalk.yellow('      secure: true'));
  console.log('');
}

// Helper to get environment
async function getEnvOrPrompt(envName?: string): Promise<Environment | null> {
  try {
    if (!envName) {
      console.log(chalk.yellow('\nAvailable environments:'));
      const envs = listEnvironments();
      console.log(printEnvironments(envs.map((e, i) => ({
        '#': i + 1,
        name: e.name,
        host: e.host,
        port: e.port,
        database: e.database,
      }))));
      console.log(chalk.gray('\nUse --env <name|number> to select an environment\n'));
      return null;
    }

    let env: Environment | undefined;

    // Try numeric index first
    const index = parseInt(envName, 10);
    if (!isNaN(index) && String(index) === envName) {
      env = getEnvironmentByIndex(index);
      if (!env) {
        printError(`Environment #${index} not found (valid range: 1-${listEnvironments().length})`);
        return null;
      }
      return env;
    }

    // Try exact match
    env = getEnvironment(envName);

    // Try partial match
    if (!env) {
      const matches = findEnvironmentByPartialName(envName);
      if (matches.length === 1) {
        env = matches[0];
      } else if (matches.length > 1) {
        console.log(chalk.yellow(`Multiple environments match "${envName}":`));
        matches.forEach(e => console.log(`  - ${e.name}`));
        return null;
      }
    }

    if (!env) {
      printError(`Environment "${envName}" not found`);
      return null;
    }

    return env;
  } catch (error) {
    if (error instanceof ConfigNotFoundError) {
      handleConfigNotFound(error);
      process.exit(1);
    }
    throw error;
  }
}

// Wrapper for commands
async function runCommand(
  action: (format: OutputFormat, envName: string, ...args: unknown[]) => Promise<void>,
  options: { env?: string; format?: string; database?: string; table?: string; hours?: string; limit?: string; wide?: boolean },
  ...args: unknown[]
): Promise<void> {
  const env = await getEnvOrPrompt(options.env);
  if (!env) return;

  const spinner = ora({
    text: 'Connecting to ClickHouse...',
    isSilent: !process.stdout.isTTY,
  }).start();

  try {
    createClickHouseClient(env);
    spinner.succeed(`Connected to ${env.name}`);

    // Set global format options
    setFormatOptions({ wide: options.wide || false });

    const format = (options.format || 'table') as OutputFormat;
    await action(format, env.name, ...args);
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    spinner.fail(`Error: ${message}`);
  } finally {
    await closeClient();
  }
}

// ==================== ENVIRONMENT MANAGEMENT ====================

program
  .command('init')
  .description('Create a new querydog.yaml configuration file')
  .option('--force', 'Overwrite existing configuration file')
  .action(async (cmdOpts) => {
    // Check if config already exists
    if (configExists() && !cmdOpts.force) {
      console.log(chalk.yellow('\n  A querydog.yaml file already exists.'));
      console.log(chalk.gray('  Use --force to overwrite, or edit the file directly.\n'));
      console.log(chalk.cyan('  To add a new environment to an existing config, use:\n'));
      console.log(chalk.white('    querydog envs --add\n'));
      return;
    }

    console.log(chalk.cyan('\n  QueryDog Configuration Setup'));
    console.log(chalk.gray('  ════════════════════════════════════════\n'));
    console.log(chalk.gray('  Enter your ClickHouse connection details:\n'));

    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });

    try {
      const name = await prompt(rl, '  Environment name', 'Production');
      const host = await prompt(rl, '  Host');
      if (!host) {
        console.log(chalk.red('\n  Error: Host is required\n'));
        rl.close();
        return;
      }
      const portStr = await prompt(rl, '  Port', '8443');
      const port = parseInt(portStr, 10);
      if (isNaN(port)) {
        console.log(chalk.red('\n  Error: Invalid port number\n'));
        rl.close();
        return;
      }
      const user = await prompt(rl, '  User', 'default');
      const password = await prompt(rl, '  Password (will be stored in plain text)', '');
      const database = await prompt(rl, '  Database', 'default');
      const secureStr = await prompt(rl, '  Use HTTPS (true/false)', 'true');
      const secure = secureStr.toLowerCase() === 'true' || secureStr === '1' || secureStr === 'yes';

      rl.close();

      const environment: Environment = {
        name,
        host,
        port,
        user,
        password,
        database,
        secure,
      };

      const config: QuerydogConfig = {
        environments: [environment],
      };

      const savedPath = saveQuerydogConfig(config);
      console.log(chalk.green(`\n  ✓ Configuration saved to: ${savedPath}\n`));
      console.log(chalk.gray('  You can now run commands like:\n'));
      console.log(chalk.white(`    querydog tables --env "${name}"`));
      console.log(chalk.white(`    querydog queries --env "${name}"\n`));
    } catch (err) {
      rl.close();
      const message = err instanceof Error ? err.message : String(err);
      console.log(chalk.red(`\n  Error: ${message}\n`));
    }
  });

program
  .command('envs')
  .description('Manage environments (list, add, remove)')
  .option('--add', 'Add a new environment')
  .option('--remove <name>', 'Remove an environment by name or number')
  .action(async (cmdOpts) => {
    try {
      // Handle --add
      if (cmdOpts.add) {
        if (!configExists()) {
          console.log(chalk.yellow('\n  No querydog.yaml file found.'));
          console.log(chalk.cyan('  Run "querydog init" to create one first.\n'));
          return;
        }

        console.log(chalk.cyan('\n  Add New Environment'));
        console.log(chalk.gray('  ════════════════════════════════════════\n'));

        const rl = readline.createInterface({
          input: process.stdin,
          output: process.stdout,
        });

        try {
          const existingEnvs = listEnvironments();
          console.log(chalk.gray('  Existing environments:'));
          existingEnvs.forEach(e => console.log(chalk.gray(`    - ${e.name}`)));
          console.log('');

          const name = await prompt(rl, '  Environment name');
          if (!name) {
            console.log(chalk.red('\n  Error: Environment name is required\n'));
            rl.close();
            return;
          }
          if (existingEnvs.some(e => e.name.toLowerCase() === name.toLowerCase())) {
            console.log(chalk.red(`\n  Error: Environment "${name}" already exists\n`));
            rl.close();
            return;
          }

          const host = await prompt(rl, '  Host');
          if (!host) {
            console.log(chalk.red('\n  Error: Host is required\n'));
            rl.close();
            return;
          }
          const portStr = await prompt(rl, '  Port', '8443');
          const port = parseInt(portStr, 10);
          if (isNaN(port)) {
            console.log(chalk.red('\n  Error: Invalid port number\n'));
            rl.close();
            return;
          }
          const user = await prompt(rl, '  User', 'default');
          const password = await prompt(rl, '  Password (will be stored in plain text)', '');
          const database = await prompt(rl, '  Database', 'default');
          const secureStr = await prompt(rl, '  Use HTTPS (true/false)', 'true');
          const secure = secureStr.toLowerCase() === 'true' || secureStr === '1' || secureStr === 'yes';

          rl.close();

          const environment: Environment = {
            name,
            host,
            port,
            user,
            password,
            database,
            secure,
          };

          const config: QuerydogConfig = {
            environments: [...existingEnvs, environment],
          };

          const savedPath = saveQuerydogConfig(config);
          console.log(chalk.green(`\n  ✓ Environment "${name}" added to: ${savedPath}\n`));
        } catch (err) {
          rl.close();
          throw err;
        }
        return;
      }

      // Handle --remove
      if (cmdOpts.remove) {
        const existingEnvs = listEnvironments();
        const identifier = cmdOpts.remove;

        let envToRemove: Environment | undefined;
        let envIndex = -1;

        // Try numeric index first
        const index = parseInt(identifier, 10);
        if (!isNaN(index) && String(index) === identifier) {
          if (index >= 1 && index <= existingEnvs.length) {
            envIndex = index - 1;
            envToRemove = existingEnvs[envIndex];
          }
        } else {
          // Try name match (case-insensitive)
          envIndex = existingEnvs.findIndex(e => e.name.toLowerCase() === identifier.toLowerCase());
          if (envIndex !== -1) {
            envToRemove = existingEnvs[envIndex];
          }
        }

        if (!envToRemove) {
          console.log(chalk.red(`\n  Error: Environment "${identifier}" not found\n`));
          console.log(chalk.gray('  Available environments:'));
          existingEnvs.forEach((e, i) => console.log(chalk.gray(`    ${i + 1}. ${e.name}`)));
          console.log('');
          return;
        }

        const remainingEnvs = existingEnvs.filter((_, i) => i !== envIndex);
        const config: QuerydogConfig = { environments: remainingEnvs };
        const savedPath = saveQuerydogConfig(config);
        console.log(chalk.green(`\n  ✓ Environment "${envToRemove.name}" removed from: ${savedPath}\n`));
        return;
      }

      // Default: list environments
      printHeader('Environments', '');
      const envs = listEnvironments();
      console.log(printEnvironments(envs.map((e, i) => ({
        '#': i + 1,
        name: e.name,
        host: e.host,
        port: e.port,
        database: e.database,
      }))));
    } catch (error) {
      if (error instanceof ConfigNotFoundError) {
        handleConfigNotFound(error);
        process.exit(1);
      }
      throw error;
    }
  });

// ==================== QUERY LOG COMMANDS ====================

program
  .command('queries')
  .description('Query log analysis')
  .option('--mode <mode>', 'Query mode: all, slowest, highestmemory, frequent, bytable, errors', 'all')
  .action(async (cmdOpts) => {
    const opts = program.opts();
    const hours = parseInt(opts.hours || '24', 10);
    const limit = parseInt(opts.limit || '50', 10);
    const mode = (cmdOpts.mode || 'all') as QueryMode;

    await runCommand(
      (format, envName) => listQueries(mode, format, envName, hours, limit),
      opts
    );
  });

// ==================== SCHEMA COMMANDS ====================

program
  .command('tables')
  .description('List tables in the database')
  .action(async () => {
    const opts = program.opts();
    await runCommand(
      (format, envName) => listTables(opts.database, format, envName),
      opts
    );
  });

program
  .command('views')
  .description('List views in the database')
  .action(async () => {
    const opts = program.opts();
    await runCommand(
      (format, envName) => listViews(opts.database, format, envName),
      opts
    );
  });

program
  .command('materialized-views')
  .description('List materialized views')
  .action(async () => {
    const opts = program.opts();
    await runCommand(
      (format, envName) => listMaterializedViews(opts.database, format, envName),
      opts
    );
  });

program
  .command('databases')
  .description('List databases with summary statistics')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listDatabasesSummary, opts);
  });

// ==================== STORAGE COMMANDS ====================

program
  .command('partitions')
  .description('List partitions')
  .action(async () => {
    const opts = program.opts();
    await runCommand(
      (format, envName) => listPartitions(opts.database, format, envName, opts.table),
      opts
    );
  });

program
  .command('mutations')
  .description('List mutations')
  .action(async () => {
    const opts = program.opts();
    await runCommand(
      (format, envName) => listMutations(opts.database, format, envName),
      opts
    );
  });

program
  .command('indexes')
  .description('List data skipping indexes')
  .action(async () => {
    const opts = program.opts();
    await runCommand(
      (format, envName) => listIndexes(format, envName, opts.database, opts.table),
      opts
    );
  });

program
  .command('projections')
  .description('List projections')
  .action(async () => {
    const opts = program.opts();
    await runCommand(
      (format, envName) => listProjections(format, envName, opts.database),
      opts
    );
  });

// ==================== SCHEMA ANALYSIS COMMANDS ====================

program
  .command('schema-nullables')
  .description('Find Nullable columns (optimization candidates)')
  .action(async () => {
    const opts = program.opts();
    await runCommand(
      (format, envName) => listNullableColumns(format, envName, opts.database, opts.table),
      opts
    );
  });

program
  .command('schema-oversized')
  .description('Find oversized integer columns (Int64 where Int32 may suffice)')
  .action(async () => {
    const opts = program.opts();
    await runCommand(
      (format, envName) => listOversizedColumns(format, envName, opts.database),
      opts
    );
  });

program
  .command('column-stats')
  .description('Show column statistics for a table (requires -d and -t)')
  .action(async () => {
    const opts = program.opts();
    if (!opts.database || !opts.table) {
      console.log('Error: --database (-d) and --table (-t) are required for column-stats');
      return;
    }
    await runCommand(
      (format, envName) => listColumnStats(format, envName, opts.database, opts.table),
      opts
    );
  });

program
  .command('dictionaries')
  .description('List dictionaries')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listDictionaries, opts);
  });

// ==================== ACTIVITY COMMANDS ====================

program
  .command('processes')
  .description('List running processes')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listProcesses, opts);
  });

program
  .command('merges')
  .description('List active merges')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listMerges, opts);
  });

// ==================== CLUSTER COMMANDS ====================

program
  .command('clusters')
  .description('List clusters')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listClusters, opts);
  });

program
  .command('replicas')
  .description('List replicas')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listReplicas, opts);
  });

program
  .command('replication-queue')
  .description('Show replication queue')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listReplicationQueue, opts);
  });

program
  .command('zookeeper')
  .description('Browse zookeeper')
  .option('--path <path>', 'Zookeeper path', '/')
  .action(async (cmdOpts) => {
    const opts = program.opts();
    await runCommand(
      (format, envName) => listZookeeper(format, envName, cmdOpts.path),
      opts
    );
  });

// ==================== STORAGE & METRICS ====================

program
  .command('disks')
  .description('List disks')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listDisks, opts);
  });

program
  .command('storage-policies')
  .description('List storage policies')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listStoragePolicies, opts);
  });

program
  .command('metrics')
  .description('Show system metrics')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listMetrics, opts);
  });

program
  .command('async-metrics')
  .description('Show asynchronous metrics')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listAsyncMetrics, opts);
  });

program
  .command('events')
  .description('Show system events')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listEvents, opts);
  });

program
  .command('system-errors')
  .description('Show system errors')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listErrors, opts);
  });

program
  .command('warnings')
  .description('Show system warnings')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listWarnings, opts);
  });

// ==================== USERS & SECURITY ====================

program
  .command('users')
  .description('List users')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listUsers, opts);
  });

program
  .command('roles')
  .description('List roles')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listRoles, opts);
  });

program
  .command('grants')
  .description('List grants')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listGrants, opts);
  });

program
  .command('quotas')
  .description('List quotas')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listQuotas, opts);
  });

// ==================== CONFIGURATION ====================

program
  .command('settings')
  .description('List settings')
  .option('--search <search>', 'Search settings by name or description')
  .action(async (cmdOpts) => {
    const opts = program.opts();
    await runCommand(
      (format, envName) => listSettings(format, envName, cmdOpts.search),
      opts
    );
  });

// ==================== LOGS ====================

program
  .command('text-log')
  .description('Show text log entries')
  .option('--level <level>', 'Filter by log level (Debug, Information, Warning, Error)')
  .action(async (cmdOpts) => {
    const opts = program.opts();
    const hours = parseInt(opts.hours || '1', 10);
    const limit = parseInt(opts.limit || '100', 10);

    await runCommand(
      (format, envName) => listTextLog(format, envName, hours, cmdOpts.level, limit),
      opts
    );
  });

// ==================== BACKGROUND OPERATIONS ====================

program
  .command('async-inserts')
  .description('Show async insert queue')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listAsyncInserts, opts);
  });

program
  .command('query-cache')
  .description('Show query cache entries')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listQueryCache, opts);
  });

program
  .command('view-refreshes')
  .description('Show materialized view refresh status')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listViewRefreshes, opts);
  });

program
  .command('background-jobs')
  .description('Show background jobs')
  .action(async () => {
    const opts = program.opts();
    await runCommand(listBackgroundJobs, opts);
  });

// ==================== DDL COMMAND ====================

program
  .command('ddl')
  .description('Show CREATE TABLE DDL statements')
  .option('--all', 'Show DDL for all tables')
  .action(async (cmdOpts) => {
    const opts = program.opts();

    await runCommand(
      async (format, envName) => {
        const { getClient } = await import('./utils/clickhouse');
        const client = getClient();

        let query: string;
        if (cmdOpts.all) {
          query = `
            SELECT database, name as table, create_table_query as ddl
            FROM system.tables
            WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
            ORDER BY database, name
          `;
        } else if (opts.database && opts.table) {
          query = `
            SELECT database, name as table, create_table_query as ddl
            FROM system.tables
            WHERE database = '${opts.database}' AND name = '${opts.table}'
          `;
        } else if (opts.database) {
          query = `
            SELECT database, name as table, create_table_query as ddl
            FROM system.tables
            WHERE database = '${opts.database}'
            ORDER BY name
          `;
        } else {
          console.log(chalk.yellow('Usage: querydog ddl --all'));
          console.log(chalk.yellow('       querydog ddl -d <database>'));
          console.log(chalk.yellow('       querydog ddl -d <database> -t <table>'));
          return;
        }

        const result = await client.query({ query, format: 'JSONEachRow' });
        const data = await result.json() as { database: string; table: string; ddl: string }[];

        if (data.length === 0) {
          console.log(chalk.yellow('No tables found.'));
          return;
        }

        printHeader('DDL Statements', envName);

        if (format === 'json') {
          console.log(JSON.stringify(data, null, 2));
        } else if (format === 'csv') {
          console.log('database,table,ddl');
          data.forEach(row => {
            console.log(`${row.database},${row.table},"${row.ddl.replace(/"/g, '""')}"`);
          });
        } else {
          // Pretty print DDL for table format
          data.forEach((row, index) => {
            if (index > 0) console.log('');
            console.log(chalk.cyan.bold(`-- ${row.database}.${row.table}`));
            console.log(chalk.green(row.ddl) + ';');
          });
        }
      },
      opts
    );
  });

// ==================== UI COMMAND ====================

program
  .command('ui')
  .description('Show the port the web UI is running on')
  .option('--port <port>', 'Specify the port to use', '3001')
  .action((cmdOpts) => {
    const port = cmdOpts.port || process.env.PORT || '3001';
    console.log(chalk.cyan('\n  QueryDog Web UI'));
    console.log(chalk.gray('  ================\n'));
    console.log(`  Port: ${chalk.green(port)}`);
    console.log(`  URL:  ${chalk.blue(`http://localhost:${port}`)}\n`);
    console.log(chalk.gray('  To start the UI via Docker:'));
    console.log(chalk.white('    querydog ui\n'));
    console.log(chalk.gray('  Or start the server directly:'));
    console.log(chalk.white('    npm run start\n'));
  });

// Add trailing blank lines after every command
program.hook('postAction', () => {
  console.log('\n');
});

// Interactive REPL mode
async function startInteractiveMode(): Promise<void> {
  const opts = program.opts();

  console.log(chalk.cyan.bold('\n  🐕 QueryDog Interactive Mode'));
  console.log(chalk.gray('  Type a command (without "querydog"), or "exit" to quit.\n'));

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  const promptUser = (): void => {
    rl.question(chalk.yellow.bold('querydog> '), async (input) => {
      const trimmed = input.trim();

      if (trimmed === 'exit' || trimmed === 'quit' || trimmed === 'q') {
        console.log(chalk.gray('\n  Goodbye! 🐕\n'));
        rl.close();
        process.exit(0);
      }

      if (trimmed === '') {
        promptUser();
        return;
      }

      if (trimmed === 'help') {
        program.outputHelp();
        promptUser();
        return;
      }

      try {
        // Parse the input as command arguments, preserving global options
        const args = trimmed.split(/\s+/);

        // Preserve env and format from global options
        if (opts.env && !args.includes('-e') && !args.includes('--env')) {
          args.push('-e', opts.env);
        }
        if (opts.format && !args.includes('-f') && !args.includes('--format')) {
          args.push('-f', opts.format);
        }
        if (opts.wide && !args.includes('-w') && !args.includes('--wide')) {
          args.push('-w');
        }

        // Create a fresh program instance for parsing
        await program.parseAsync(['node', 'querydog', ...args]);
      } catch (err) {
        // Errors are already handled by commands
      }

      promptUser();
    });
  };

  promptUser();
}

// Parse and run
program.parse();

// Check if interactive mode or show help if no command
const globalOpts = program.opts();
if (globalOpts.interactive) {
  startInteractiveMode();
} else if (!process.argv.slice(2).length) {
  program.outputHelp();
}
