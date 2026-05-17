#!/bin/sh
#
# QueryDog Docker Entrypoint
# Routes to either the CLI or the server based on arguments
#

set -e

# Check if querydog.yml was mounted correctly (Docker creates a directory if the source file doesn't exist)
if [ -d "/app/querydog.yml" ]; then
    echo "ERROR: querydog.yml not found." >&2
    echo "" >&2
    echo "Docker created a directory instead of mounting a file because querydog.yml" >&2
    echo "does not exist on the host. Please create a querydog.yml config file first:" >&2
    echo "" >&2
    echo "  querydog init" >&2
    echo "" >&2
    echo "Or specify a config file path:" >&2
    echo "" >&2
    echo "  QUERYDOG_CONFIG=/path/to/querydog.yml docker compose up" >&2
    echo "" >&2
    exit 1
fi

if [ ! -f "/app/querydog.yml" ]; then
    echo "ERROR: querydog.yml not found at /app/querydog.yml" >&2
    echo "" >&2
    echo "Please create a querydog.yml config file and ensure it is mounted into the container." >&2
    echo "Run 'querydog init' to create one, or see the documentation for details." >&2
    echo "" >&2
    exit 1
fi

# If first argument is "server", run the web server
if [ "$1" = "server" ]; then
    shift
    exec node server/index.js "$@"
fi

# Otherwise, run the CLI
exec node cli/dist/index.js "$@"
