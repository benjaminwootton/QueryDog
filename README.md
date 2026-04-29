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

Copy `querydog.yml.example` to `querydog.yml`:

```bash
cp querydog.yml.example querydog.yml
```

Edit `querydog.yml` with your connection details. You can configure multiple environments and switch between them from the UI:

```yaml
environments:
  - name: "Production"
    host: "your-clickhouse-host"
    port: 8443
    user: "your-username"
    password: "your-password"
    database: "default"
    secure: true

  - name: "Local"
    host: "localhost"
    port: 8123
    user: "default"
    password: ""
    database: "default"
```

### Run With Docker Compose (Preferred)

This builds the image locally from the `Dockerfile` and starts the web UI:

```bash
docker compose up --build
```

Subsequent starts (no source changes) can just use:

```bash
docker compose up
```

Access the UI at http://localhost:3001.

### Running The CLI Through Docker

The same image also ships the QueryDog CLI. Anything other than `server` passed to the container falls through to the CLI, so you can run one-off CLI commands against the built image with `docker compose run`:

```bash
docker compose run --rm querydog help
docker compose run --rm querydog tables
docker compose run --rm querydog query "SELECT 1"
```

`--rm` cleans up the container after the command exits, and your `querydog.yml` and `./queries` mounts are preserved automatically.

For convenience, add a shell alias so you can call the CLI like a local binary:

```bash
alias qd='docker compose run --rm querydog'
# then:
qd help
qd tables
qd query "SELECT 1"
```

### Run From Source

```bash
npm install
npm run dev:all
```
Access at http://localhost:3001

### Next Steps

Please visit https://benjaminwootton.com for more details on the project and my ClickHouse consulting services.

