# Query Dog

A powerful tool for ClickHouse performance optimisation.  Visualise query performance and background activity, identify bottlenecks and optimise cluster performance with a single container.

<img width="2054" height="1101" alt="Screenshot 2025-12-09 at 11 31 53 pm" src="https://github.com/user-attachments/assets/e1b52d42-028a-4c46-8631-7fdcaba44747" />
<img width="2055" height="1101" alt="Screenshot 2025-12-09 at 11 32 10 pm" src="https://github.com/user-attachments/assets/81150e55-b47b-4feb-89cd-02bee733ad4b" />

## Prerequisites

- A ClickHouse database with query logs
- Node.js 22+ if running from source

### Running From Docker

Run the container fr
om Docker Hub or GitHub Container Registry:

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

   
