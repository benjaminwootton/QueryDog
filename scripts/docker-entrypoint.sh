#!/bin/sh
#
# QueryDog Docker Entrypoint
# Routes to either the CLI or the server based on arguments
#

set -e

# If first argument is "server", run the web server
if [ "$1" = "server" ]; then
    shift
    exec node server/index.js "$@"
fi

# Otherwise, run the CLI
exec node cli/dist/index.js "$@"
