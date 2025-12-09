# Query Dog

A powerful tool for ClickHouse performance optimisation.  Visualise query performance and background activity, identify bottlenecks and optimise cluster performance with a single container.

## Prerequisites

- A ClickHouse database with query logs
- Node.js 22+ if running from source

### Running From Docker

Run the container from Docker Hub or GitHub Container Registry:

```bash
# From GitHub Container Registry
docker run -d \
  -p 3001:3001 \
  -e CLICKHOUSE_HOST=your-clickhouse-host \
  -e CLICKHOUSE_USER=your-username \
  -e CLICKHOUSE_PASSWORD=your-password \
  -e CLICKHOUSE_DATABASE=your-database \
  -e CLICKHOUSE_SECURE=1 \
  -e CLICKHOUSE_PORT=9440 \
  -e CLICKHOUSE_PORT_HTTP=8443 \
  ghcr.io/benjaminwootton/querydog:latest
```

3. Access the application at http://localhost:3001

## Running From Source

Create a `.env` file in the root directory and populate the following variables:

```env
CLICKHOUSE_HOST=your-clickhouse-host
CLICKHOUSE_USER=your-username
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=your-database
CLICKHOUSE_SECURE=1
CLICKHOUSE_PORT=9440
CLICKHOUSE_PORT_HTTP=8443
```

2. Build the application:
   ```bash
   npm run build
   ```

3. Start the production server:
   ```bash
   npm start
   ```

   
