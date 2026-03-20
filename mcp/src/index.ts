#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  ListResourcesRequestSchema,
  ReadResourceRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';

import { listEnvironments, getEnvironment, Environment } from './config';
import { connect, query, close, Queries } from './clickhouse';

const server = new Server(
  {
    name: 'querydog-mcp',
    version: '1.0.0',
  },
  {
    capabilities: {
      tools: {},
      resources: {},
    },
  }
);

// List available tools
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: 'list_environments',
        description: 'List available ClickHouse environments from querydog.yaml',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
      {
        name: 'list_tables',
        description: 'List tables in a ClickHouse database',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            database: { type: 'string', description: 'Database name (optional)' },
          },
          required: ['env'],
        },
      },
      {
        name: 'queries_slow',
        description: 'Get slowest queries from query log',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            hours: { type: 'number', description: 'Hours to look back (default: 24)' },
            limit: { type: 'number', description: 'Max results (default: 50)' },
          },
          required: ['env'],
        },
      },
      {
        name: 'queries_fastest',
        description: 'Get fastest queries from query log (baseline performance)',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            hours: { type: 'number', description: 'Hours to look back (default: 24)' },
            limit: { type: 'number', description: 'Max results (default: 50)' },
          },
          required: ['env'],
        },
      },
      {
        name: 'queries_rows_read',
        description: 'Get queries ordered by rows read (find full table scans)',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            hours: { type: 'number', description: 'Hours to look back (default: 24)' },
            limit: { type: 'number', description: 'Max results (default: 50)' },
          },
          required: ['env'],
        },
      },
      {
        name: 'queries_memory',
        description: 'Get highest memory usage queries',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            hours: { type: 'number', description: 'Hours to look back (default: 24)' },
            limit: { type: 'number', description: 'Max results (default: 50)' },
          },
          required: ['env'],
        },
      },
      {
        name: 'queries_frequent',
        description: 'Get most frequent query patterns (grouped)',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            hours: { type: 'number', description: 'Hours to look back (default: 24)' },
            limit: { type: 'number', description: 'Max results (default: 50)' },
          },
          required: ['env'],
        },
      },
      {
        name: 'queries_by_table',
        description: 'Get query statistics grouped by table',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            hours: { type: 'number', description: 'Hours to look back (default: 24)' },
            limit: { type: 'number', description: 'Max results (default: 50)' },
          },
          required: ['env'],
        },
      },
      {
        name: 'queries_errors',
        description: 'Get query errors from query log',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            hours: { type: 'number', description: 'Hours to look back (default: 24)' },
            limit: { type: 'number', description: 'Max results (default: 50)' },
          },
          required: ['env'],
        },
      },
      {
        name: 'processes',
        description: 'Get currently running processes/queries',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
          },
          required: ['env'],
        },
      },
      {
        name: 'merges',
        description: 'Get active merge operations',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
          },
          required: ['env'],
        },
      },
      {
        name: 'mutations',
        description: 'Get mutation operations',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            database: { type: 'string', description: 'Database name (optional)' },
          },
          required: ['env'],
        },
      },
      {
        name: 'disks',
        description: 'Get disk usage information',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
          },
          required: ['env'],
        },
      },
      {
        name: 'partitions',
        description: 'Get partition information',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            database: { type: 'string', description: 'Database name (optional)' },
            table: { type: 'string', description: 'Table name (optional)' },
          },
          required: ['env'],
        },
      },
      {
        name: 'schema_nullables',
        description: 'Find Nullable columns (optimization candidates)',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            database: { type: 'string', description: 'Database name (optional)' },
          },
          required: ['env'],
        },
      },
      {
        name: 'schema_oversized',
        description: 'Find oversized integer columns (Int64 where Int32 may suffice)',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            database: { type: 'string', description: 'Database name (optional)' },
          },
          required: ['env'],
        },
      },
      {
        name: 'system_errors',
        description: 'Get system errors',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
          },
          required: ['env'],
        },
      },
      {
        name: 'warnings',
        description: 'Get system warnings',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
          },
          required: ['env'],
        },
      },
      {
        name: 'replicas',
        description: 'Get replica status (for replicated tables)',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
          },
          required: ['env'],
        },
      },
      {
        name: 'run_query',
        description: 'Run a custom SQL query',
        inputSchema: {
          type: 'object',
          properties: {
            env: { type: 'string', description: 'Environment name or number' },
            sql: { type: 'string', description: 'SQL query to execute' },
          },
          required: ['env', 'sql'],
        },
      },
    ],
  };
});

// List resources (environments)
server.setRequestHandler(ListResourcesRequestSchema, async () => {
  const envs = listEnvironments();
  return {
    resources: envs.map((env, i) => ({
      uri: `querydog://env/${i + 1}`,
      name: env.name,
      description: `${env.host}:${env.port} (${env.database})`,
      mimeType: 'application/json',
    })),
  };
});

// Read resource
server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const uri = request.params.uri;
  const match = uri.match(/querydog:\/\/env\/(\d+)/);
  if (match) {
    const envs = listEnvironments();
    const index = parseInt(match[1], 10) - 1;
    if (index >= 0 && index < envs.length) {
      const env = envs[index];
      return {
        contents: [
          {
            uri,
            mimeType: 'application/json',
            text: JSON.stringify({
              name: env.name,
              host: env.host,
              port: env.port,
              database: env.database,
              secure: env.secure,
            }, null, 2),
          },
        ],
      };
    }
  }
  throw new Error(`Resource not found: ${uri}`);
});

// Helper to run a query with environment
async function runWithEnv<T>(
  envName: string,
  fn: () => Promise<T>
): Promise<T> {
  const env = getEnvironment(envName);
  if (!env) {
    throw new Error(`Environment "${envName}" not found`);
  }
  connect(env);
  try {
    return await fn();
  } finally {
    await close();
  }
}

// Handle tool calls
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    switch (name) {
      case 'list_environments': {
        const envs = listEnvironments();
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(
                envs.map((e, i) => ({
                  number: i + 1,
                  name: e.name,
                  host: e.host,
                  port: e.port,
                  database: e.database,
                })),
                null,
                2
              ),
            },
          ],
        };
      }

      case 'list_tables': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.tables(args?.database as string | undefined))
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'queries_slow': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.queriesSlow(
            (args?.hours as number) || 24,
            (args?.limit as number) || 50
          ))
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'queries_fastest': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.queriesFastest(
            (args?.hours as number) || 24,
            (args?.limit as number) || 50
          ))
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'queries_rows_read': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.queriesRowsRead(
            (args?.hours as number) || 24,
            (args?.limit as number) || 50
          ))
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'queries_memory': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.queriesMemory(
            (args?.hours as number) || 24,
            (args?.limit as number) || 50
          ))
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'queries_frequent': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.queriesFrequent(
            (args?.hours as number) || 24,
            (args?.limit as number) || 50
          ))
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'queries_by_table': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.queriesByTable(
            (args?.hours as number) || 24,
            (args?.limit as number) || 50
          ))
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'queries_errors': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.queriesErrors(
            (args?.hours as number) || 24,
            (args?.limit as number) || 50
          ))
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'processes': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.processes())
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'merges': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.merges())
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'mutations': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.mutations(args?.database as string | undefined))
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'disks': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.disks())
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'partitions': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.partitions(
            args?.database as string | undefined,
            args?.table as string | undefined
          ))
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'schema_nullables': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.schemaNullables(args?.database as string | undefined))
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'schema_oversized': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.schemaOversized(args?.database as string | undefined))
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'system_errors': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.systemErrors())
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'warnings': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.warnings())
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'replicas': {
        const data = await runWithEnv(args?.env as string, () =>
          query(Queries.replicas())
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      case 'run_query': {
        if (!args?.sql) {
          throw new Error('SQL query is required');
        }
        const data = await runWithEnv(args?.env as string, () =>
          query(args.sql as string)
        );
        return { content: [{ type: 'text', text: JSON.stringify(data, null, 2) }] };
      }

      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      content: [{ type: 'text', text: `Error: ${message}` }],
      isError: true,
    };
  }
});

// Start server
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error('QueryDog MCP server running on stdio');
}

main().catch(console.error);
