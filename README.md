QueryDog is a powerful tool for ClickHouse performance optimisation.  Visualise query performance and background activity, identify bottlenecks and optimise cluster performance with a single container.

By [Benjamin Wootton](https://benjaminwootton.com).

<img width="2054" height="1101" alt="Screenshot 2025-12-09 at 11 31 53 pm" src="https://github.com/user-attachments/assets/e1b52d42-028a-4c46-8631-7fdcaba44747" />
<img width="2055" height="1101" alt="Screenshot 2025-12-09 at 11 32 10 pm" src="https://github.com/user-attachments/assets/81150e55-b47b-4feb-89cd-02bee733ad4b" />

## Prerequisites

- A ClickHouse database with query logs
- Node.js 22+ if running from source
- Docker if running via containers

## Running

Clone the repository:

```bash
git clone https://github.com/benjaminwootton/querydog
cd querydog
```

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Populate .env with your connection details:

```env
CLICKHOUSE_HOST=your-clickhouse-host
CLICKHOUSE_USER=your-username
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=your-database
CLICKHOUSE_SECURE=1
CLICKHOUSE_PORT_HTTP=8443
```

### Run With Docker Compose (Preferred)

```bash
docker compose up --build
```

### Run From Source

```bash
npm install
npm run dev:all
```
Access at http://localhost:3001

### Next Steps

Please visit https://benjaminwootton.com for more details on the project and my ClickHouse consulting services.

