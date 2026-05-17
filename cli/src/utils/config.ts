import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'yaml';

export interface Environment {
  name: string;
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  secure: boolean;
  skip_tls_verify?: boolean;
  tls_reject_unauthorized?: boolean;  // alias: false = skip verification
}

export interface QuerydogConfig {
  environments: Environment[];
}

interface ColumnConfig {
  columns: string[];
  order_by?: string;
  limit?: number;
}

export interface CLIConfig {
  tables: ColumnConfig;
  views: ColumnConfig;
  materialized_views: ColumnConfig;
  partitions: ColumnConfig;
  mutations: ColumnConfig;
  processes: ColumnConfig;
  queries: {
    all: ColumnConfig;
    slowest: ColumnConfig;
    highestmemory: ColumnConfig;
  };
  output: {
    max_column_width: number;
    truncate_queries: boolean;
    query_max_length: number;
  };
}

function findProjectRoot(): string {
  // Start from current directory and walk up to find querydog.yml
  let dir = process.cwd();
  while (dir !== path.dirname(dir)) {
    const candidate = path.join(dir, 'querydog.yml');
    if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
      return dir;
    }
    dir = path.dirname(dir);
  }
  // Fall back to parent of cli directory
  return path.resolve(__dirname, '../../..');
}

export class ConfigNotFoundError extends Error {
  constructor(public searchedPaths: string[]) {
    super('Configuration file not found');
    this.name = 'ConfigNotFoundError';
  }
}

function getConfigSearchPaths(): string[] {
  return [
    path.join(findProjectRoot(), 'querydog.yml'),
    path.resolve(__dirname, '../../../querydog.yml'),  // When running from dist
    path.resolve(__dirname, '../../querydog.yml'),     // When running with ts-node
    path.resolve(process.cwd(), '../querydog.yml'),    // Parent of cwd
    path.resolve(process.cwd(), 'querydog.yml'),       // Current dir
  ];
}

function loadQuerydogConfig(): QuerydogConfig {
  const locations = getConfigSearchPaths();

  for (const configPath of locations) {
    if (fs.existsSync(configPath)) {
      const stat = fs.statSync(configPath);
      if (stat.isDirectory()) {
        // Skip directories (e.g. Docker creates a directory when the source file doesn't exist)
        continue;
      }
      const content = fs.readFileSync(configPath, 'utf8');
      const parsed = yaml.parse(content);
      // Handle empty file, null, or missing environments key
      if (!parsed || typeof parsed !== 'object') {
        return { environments: [] };
      }
      return {
        environments: Array.isArray(parsed.environments) ? parsed.environments : [],
      };
    }
  }

  throw new ConfigNotFoundError(locations);
}

export function saveQuerydogConfig(config: QuerydogConfig, targetPath?: string): string {
  const savePath = targetPath || path.resolve(process.cwd(), 'querydog.yml');
  const content = yaml.stringify(config, { indent: 2 });
  fs.writeFileSync(savePath, content, 'utf8');
  return savePath;
}

export function configExists(): boolean {
  const locations = getConfigSearchPaths();
  return locations.some(p => fs.existsSync(p) && fs.statSync(p).isFile());
}

export function loadCLIConfig(): CLIConfig {
  // Try multiple locations for CLI config
  const locations = [
    path.resolve(__dirname, '../config/querydog-cli.yaml'),   // When running from dist/utils
    path.resolve(__dirname, '../../config/querydog-cli.yaml'), // Fallback
    path.resolve(process.cwd(), 'config/querydog-cli.yaml'),   // From cwd
  ];

  for (const configPath of locations) {
    if (fs.existsSync(configPath)) {
      const content = fs.readFileSync(configPath, 'utf8');
      return yaml.parse(content) as CLIConfig;
    }
  }

  // Return default config if not found
  return {
    tables: { columns: ['name', 'engine', 'total_rows', 'total_bytes'] },
    views: { columns: ['database', 'name', 'engine'] },
    materialized_views: { columns: ['database', 'name', 'engine', 'total_rows'] },
    partitions: { columns: ['database', 'table', 'partition', 'rows', 'bytes_on_disk'] },
    mutations: { columns: ['table', 'mutation_id', 'command', 'is_done'] },
    processes: { columns: ['query_id', 'user', 'elapsed', 'memory_usage', 'query'] },
    queries: {
      all: { columns: ['event_time', 'query_duration_ms', 'memory_usage', 'query'] },
      slowest: { columns: ['event_time', 'query_duration_ms', 'read_rows', 'memory_usage'] },
      highestmemory: { columns: ['event_time', 'memory_usage', 'peak_memory_usage', 'query_duration_ms'] },
    },
    output: {
      max_column_width: 80,
      truncate_queries: true,
      query_max_length: 100,
    },
  };
}

export function getEnvironment(name: string): Environment | undefined {
  const config = loadQuerydogConfig();
  return config.environments.find(
    (env) => env.name.toLowerCase() === name.toLowerCase()
  );
}

export function listEnvironments(): Environment[] {
  const config = loadQuerydogConfig();
  return config.environments || [];
}

export function findEnvironmentByPartialName(partial: string): Environment[] {
  const config = loadQuerydogConfig();
  const lowerPartial = partial.toLowerCase();
  return config.environments.filter(
    (env) => env.name.toLowerCase().includes(lowerPartial)
  );
}

export function getEnvironmentByIndex(index: number): Environment | undefined {
  const config = loadQuerydogConfig();
  // 1-based index for user convenience
  if (index < 1 || index > config.environments.length) {
    return undefined;
  }
  return config.environments[index - 1];
}
