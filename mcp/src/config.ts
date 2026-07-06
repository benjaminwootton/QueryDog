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
  tls_reject_unauthorized?: boolean;
  http_headers?: Record<string, string>;
}

interface QuerydogConfig {
  environments: Environment[];
}

function findProjectRoot(): string {
  let dir = process.cwd();
  while (dir !== path.dirname(dir)) {
    if (fs.existsSync(path.join(dir, 'querydog.yml'))) {
      return dir;
    }
    dir = path.dirname(dir);
  }
  return process.cwd();
}

function loadConfig(): QuerydogConfig {
  const locations = [
    path.join(findProjectRoot(), 'querydog.yml'),
    path.resolve(process.cwd(), 'querydog.yml'),
    path.resolve(process.cwd(), '../querydog.yml'),
  ];

  for (const configPath of locations) {
    if (fs.existsSync(configPath)) {
      const content = fs.readFileSync(configPath, 'utf8');
      return yaml.parse(content) as QuerydogConfig;
    }
  }

  throw new Error(`querydog.yml not found. Searched: ${locations.join(', ')}`);
}

export function listEnvironments(): Environment[] {
  return loadConfig().environments;
}

export function getEnvironment(nameOrIndex: string): Environment | undefined {
  const config = loadConfig();

  // Try numeric index (1-based)
  const index = parseInt(nameOrIndex, 10);
  if (!isNaN(index) && String(index) === nameOrIndex) {
    if (index >= 1 && index <= config.environments.length) {
      return config.environments[index - 1];
    }
    return undefined;
  }

  // Try exact name match
  const exact = config.environments.find(
    e => e.name.toLowerCase() === nameOrIndex.toLowerCase()
  );
  if (exact) return exact;

  // Try partial match
  const partial = config.environments.filter(
    e => e.name.toLowerCase().includes(nameOrIndex.toLowerCase())
  );
  if (partial.length === 1) return partial[0];

  return undefined;
}
